// MmapBackend.State.cs
//
// High-performance, memory-mapped backend for IBackend<TKey,TValue> with BLUE/GREEN publishing.
//
// Key rules:
// - Reads are always served from an immutable, memory-mapped “active” snapshot.
// - All mutations go only to an in-memory “staging” trie.
// - Mutations DO NOT affect reads until you call Swap().
// - Swap() builds a brand-new file (temp + replace), then atomically switches “active” to the new mapping.
// - If the file path does not exist, a new empty mmap file is created.
//
// File format (v1):
// [MmapHeader][MmapNode array][Value blob region]
//
// Value storage:
// - Node stores (ValueOffset, ValueLength).
// - ValueOffset is relative to header.ValueRegionOffset and points to a 4-byte length prefix.
// - Blob entry layout: [int32 length][byte[length] payload]
// - ValueLength is the payload length (excluding the 4-byte prefix).
//
// Build requirements: <AllowUnsafeBlocks>true</AllowUnsafeBlocks>
//
// Concurrency model:
// - Reads are lock-free w.r.t swap using ref-counted snapshot leases so Swap can safely retire old mappings.
// - Staging mutations are protected by a private lock and never touch the active mapping.
//
// Safety model:
// - Bounds checks remain enabled by default; define MMAP_UNSAFE_FAST only for trusted files.

using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading;

namespace BenchmarkTreeBackends.Backends.MMAP
{
    public abstract unsafe partial class MmapBackend<TKey, TValue> where TValue : class
    {
        // -----------------------------
        // State: immutable mmap snapshot (ref-counted)
        // -----------------------------

        private sealed class State
        {
            private readonly MemoryMappedFile _mmf;
            private readonly MemoryMappedViewAccessor _accessor;
            private readonly byte* _base;
            private readonly long _fileSize;
            private readonly bool _pointerAcquired;

            public readonly MmapHeader Header;
            public readonly long RootPos;

            private int _refCount;

            private State(MemoryMappedFile mmf, MemoryMappedViewAccessor accessor, byte* @base, bool pointerAcquired, long fileSize, in MmapHeader header)
            {
                _mmf = mmf;
                _accessor = accessor;
                _base = @base;
                _pointerAcquired = pointerAcquired;
                _fileSize = fileSize;

                Header = header;
                RootPos = header.NodeRegionOffset; // root is node index 0 => offset NodeRegionOffset

                _refCount = 1; // publisher ref
            }

            public static State OpenReadOnly(string filePath)
            {
                var fi = new FileInfo(filePath);
                if (!fi.Exists) throw new FileNotFoundException("MMAP file not found.", filePath);

                long fileSize = fi.Length;
                if (fileSize < sizeof(MmapHeader) + sizeof(MmapNode))
                    throw new InvalidDataException("MMAP file too small.");

                var mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
                var accessor = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);

                byte* ptr = null;
                bool acquired = false;

                try
                {
                    accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                    acquired = true;

                    ref readonly var hdr = ref Unsafe.AsRef<MmapHeader>(ptr);

                    if (hdr.Magic != DomainTreeMmapFormat.Magic) throw new InvalidDataException("Invalid magic.");
                    if (hdr.Version != DomainTreeMmapFormat.Version) throw new InvalidDataException("Unsupported version.");
                    if (hdr.Endianness != DomainTreeMmapFormat.LittleEndian) throw new InvalidDataException("Endian mismatch.");

                    if (hdr.NodeRegionOffset < sizeof(MmapHeader)) throw new InvalidDataException("Invalid NodeRegionOffset.");
                    if (hdr.NodeCount < 1) throw new InvalidDataException("Invalid NodeCount.");

                    long nodeBytes = checked(hdr.NodeCount * (long)sizeof(MmapNode));
                    long valueRegionMin = checked(hdr.NodeRegionOffset + nodeBytes);

                    if (hdr.ValueRegionOffset < valueRegionMin) throw new InvalidDataException("Invalid ValueRegionOffset.");
                    if (hdr.ValueRegionOffset > fileSize) throw new InvalidDataException("ValueRegionOffset out of range.");

                    // Root bounds check (node 0)
                    if (!IsOffsetValidStatic(fileSize, hdr.NodeRegionOffset, sizeof(MmapNode)))
                        throw new InvalidDataException("Root node out of range.");

                    return new State(mmf, accessor, ptr, acquired, fileSize, hdr);
                }
                catch
                {
                    try
                    {
                        if (acquired) accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                    }
                    catch { /* best-effort */ }

                    accessor.Dispose();
                    mmf.Dispose();
                    throw;
                }
            }

            public void AddRef() => Interlocked.Increment(ref _refCount);

            public void Release()
            {
                if (Interlocked.Decrement(ref _refCount) == 0)
                    DisposeNow();
            }

            public void RetireAndTryDispose()
            {
                Release(); // drop publisher ref; actual dispose occurs when last reader releases
            }

            private void DisposeNow()
            {
                try
                {
                    if (_pointerAcquired)
                        _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                }
                catch { /* best-effort */ }

                _accessor.Dispose();
                _mmf.Dispose();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ref readonly MmapNode GetNodeAt(long offset)
            {
#if !MMAP_UNSAFE_FAST
                if (!IsOffsetValid(offset, sizeof(MmapNode)))
                    throw new InvalidDataException("Node offset out of range.");
#endif
                return ref Unsafe.AsRef<MmapNode>(_base + offset);
            }

            public bool TryFindNode(MmapBackend<TKey, TValue> owner, TKey key, out long nodePos, bool requireValue)
            {
                nodePos = 0;

                byte[]? bKey;
                try { bKey = owner.ConvertToByteKey(key, throwException: false); }
                catch { return false; }

                if (bKey is null)
                    return false;

                long currentPos = RootPos;

                // Empty key => root
                if (bKey.Length == 0)
                {
                    ref readonly var root = ref GetNodeAt(currentPos);
                    if (requireValue && root.ValueOffset == 0) return false;
                    nodePos = currentPos;
                    return true;
                }

                for (int i = 0; i < bKey.Length; i++)
                {
                    ref readonly var node = ref GetNodeAt(currentPos);
                    if (node.ChildCount == 0 || node.FirstChildPos == 0)
                        return false;

                    uint labelId = DomainTreeMmapFormat.ToLabelId(bKey[i]);
                    long childPos = FindChild(node.FirstChildPos, node.ChildCount, labelId);
                    if (childPos == 0)
                        return false;

                    currentPos = childPos;
                }

                ref readonly var finalNode = ref GetNodeAt(currentPos);
                if (requireValue && finalNode.ValueOffset == 0)
                    return false;

                nodePos = currentPos;
                return true;
            }

            public bool TryReadValueBytes(in MmapNode node, out ReadOnlySpan<byte> payload)
            {
                if (node.ValueOffset == 0)
                {
                    payload = default;
                    return false;
                }

                long abs = checked(Header.ValueRegionOffset + node.ValueOffset);

#if !MMAP_UNSAFE_FAST
                // Need at least 4 bytes for length prefix
                if (!IsOffsetValid(abs, 4))
                    throw new InvalidDataException("Value prefix out of range.");
#endif

                byte* p = _base + abs;
                int len = *(int*)p;

                if (len < 0)
                    throw new InvalidDataException("Negative value length.");

                if (node.ValueLength != len)
                    throw new InvalidDataException("ValueLength mismatch.");

#if !MMAP_UNSAFE_FAST
                if (!IsOffsetValid(abs + 4L, len))
                    throw new InvalidDataException("Value payload out of range.");
#endif

                payload = new ReadOnlySpan<byte>(p + 4, len);
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private long FindChild(long firstChildPos, uint childCount, uint labelId)
            {
#if !MMAP_UNSAFE_FAST
                long bytes = checked((long)childCount * sizeof(MmapNode));
                if (!IsOffsetValid(firstChildPos, bytes))
                    throw new InvalidDataException("Children region out of range.");
#endif
                long lo = 0;
                long hi = (long)childCount - 1;

                while (lo <= hi)
                {
                    long mid = lo + ((hi - lo) >> 1);
                    long pos = firstChildPos + mid * sizeof(MmapNode);

                    ref readonly var n = ref GetNodeAt(pos);
                    uint midId = n.LabelId;

                    if (midId == labelId) return pos;
                    if (midId < labelId) lo = mid + 1;
                    else hi = mid - 1;
                }

                return 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool IsOffsetValid(long offset, long length)
                => IsOffsetValidStatic(_fileSize, offset, length);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsOffsetValidStatic(long fileSize, long offset, long length)
            {
                if (length < 0) return false;
                if ((ulong)offset >= (ulong)fileSize) return false;
                long end = offset + length;
                return end >= offset && end <= fileSize;
            }
        }
    }
}