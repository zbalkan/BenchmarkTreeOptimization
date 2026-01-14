using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BenchmarkTreeBackends.Backends.MMAP
{
    internal unsafe sealed class MmapFile : IDisposable
    {
        private const uint Magic = 0x50414D4Du; // "MMAP" (pick any constant; must match your writer if you validate)
        private const uint Version = 1;

        private readonly string _path;

        private MemoryMappedFile? _mmf;
        private MemoryMappedViewAccessor? _view;
        private SafeBuffer? _buffer;

        public byte* BasePtr { get; private set; }
        public MmapHeader* Header { get; private set; }

        public long CapacityBytes { get; private set; }

        public MmapFile(string path, uint nodeCapacity = 2_000_000, long valueCapacityBytes = 256L * 1024 * 1024)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path must be non-empty.", nameof(path));
            if (nodeCapacity < 2)
                nodeCapacity = 2; // sentinel + root
            if (valueCapacityBytes < 1024)
                valueCapacityBytes = 1024;

            _path = path;

            long headerSize = AlignUp(sizeof(MmapHeader), 64);
            long nodeRegionOffset = headerSize;

            long nodeSize = sizeof(MmapNode);
            long nodeRegionBytes = checked((long)nodeCapacity * nodeSize);

            long valueRegionOffset = AlignUp(nodeRegionOffset + nodeRegionBytes, 64);
            long capacity = checked(valueRegionOffset + valueCapacityBytes);

            EnsureFileExistsWithLength(_path, capacity);

            Map(capacity);
            InitOrValidate(headerSize, nodeRegionOffset, nodeCapacity, valueRegionOffset, valueCapacityBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref MmapNode GetNode(uint index)
        {
            // Bounds: 0..NodeCount-1 valid, but also must not exceed capacity allocation.
            uint nodeCount = Header->NodeCount;
            if (index >= nodeCount)
                throw new IndexOutOfRangeException($"Node index {index} out of range (NodeCount={nodeCount}).");

            // Also ensure index is within allocated capacity implied by ValueRegionOffset.
            // MaxNodes = (ValueRegionOffset - NodeRegionOffset) / sizeof(MmapNode)
            long maxNodes = (Header->ValueRegionOffset - Header->NodeRegionOffset) / sizeof(MmapNode);
            if (index >= (uint)maxNodes)
                throw new IndexOutOfRangeException($"Node index {index} exceeds mapped node capacity (MaxNodes={maxNodes}).");

            return ref ((MmapNode*)(BasePtr + Header->NodeRegionOffset))[index];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> GetValue(long valueOffset, int valueLength)
        {
            if (valueOffset < 0 || valueLength < 0)
                throw new ArgumentOutOfRangeException();

            // Layout is: [int32 len][payload], ValueOffset points to the start of len.
            // Payload begins at +4.
            long start = checked(Header->ValueRegionOffset + valueOffset);
            long payloadStart = checked(start + 4);
            long payloadEnd = checked(payloadStart + valueLength);

            if (payloadEnd > CapacityBytes)
                throw new EndOfStreamException("Value payload exceeds mapped capacity.");

            // Optional: sanity-check stored length matches ValueLength (cheap-ish, but can be disabled later)
            int storedLen = *(int*)(BasePtr + start);
            if (storedLen != valueLength)
                throw new InvalidDataException($"Corrupt value length prefix: stored={storedLen}, expected={valueLength}.");

            return new ReadOnlySpan<byte>(BasePtr + payloadStart, valueLength);
        }

        public void Flush()
        {
            // Best-effort; view accessor flushes pages.
            _view?.Flush();
        }

        public void ResetToEmpty()
        {
            // Writer-side operation; caller should hold the writer lock.
            Header->WriteInProgress = 1;

            try
            {
                Header->NodeCount = 2;   // 0 sentinel, 1 root
                Header->ValueCount = 0;
                Header->ValueTail = 0;

                // Clear root
                ref var root = ref ((MmapNode*)(BasePtr + Header->NodeRegionOffset))[1];
                root.Flags = 0;
                root.ValueOffset = 0;
                root.ValueLength = 0;
                fixed (uint* p = root.Children)
                {
                    for (int i = 0; i < 256; i++)
                        p[i] = 0;
                }

                // (Optional) clear sentinel too
                ref var sentinel = ref ((MmapNode*)(BasePtr + Header->NodeRegionOffset))[0];
                sentinel.Flags = 0;
                sentinel.ValueOffset = 0;
                sentinel.ValueLength = 0;
                fixed (uint* sp = sentinel.Children)
                {
                    for (int i = 0; i < 256; i++)
                        sp[i] = 0;
                }
            }
            finally
            {
                Header->WriteInProgress = 0;
            }
        }

        private void InitOrValidate(
            long headerSize,
            long nodeRegionOffset,
            uint nodeCapacity,
            long valueRegionOffset,
            long valueCapacityBytes)
        {
            Header = (MmapHeader*)BasePtr;

            bool uninitialized = Header->Magic == 0 && Header->Version == 0;

            if (uninitialized)
            {
                // Initialize new file
                Header->Magic = Magic;
                Header->Version = Version;

                Header->WriteInProgress = 0;

                Header->NodeRegionOffset = nodeRegionOffset;
                Header->ValueRegionOffset = valueRegionOffset;

                Header->NodeCount = 2;   // sentinel + root
                Header->ValueCount = 0;
                Header->ValueTail = 0;

                // Zero sentinel + root
                var nodes = (MmapNode*)(BasePtr + Header->NodeRegionOffset);

                // sentinel [0]
                nodes[0].Flags = 0;
                nodes[0].ValueOffset = 0;
                nodes[0].ValueLength = 0;
                uint* sp = nodes[0].Children;
                for (int i = 0; i < 256; i++)
                    sp[i] = 0;

                // root [1]
                nodes[1].Flags = 0;
                nodes[1].ValueOffset = 0;
                nodes[1].ValueLength = 0;
                uint* rp = nodes[1].Children;
                for (int i = 0; i < 256; i++)
                    rp[i] = 0;

                return;
            }

            // Validate existing file
            if (Header->Magic != Magic)
                throw new InvalidDataException("Invalid MMAP file (magic mismatch).");
            if (Header->Version != Version)
                throw new InvalidDataException("Invalid MMAP file (version mismatch).");

            if (Header->NodeRegionOffset != nodeRegionOffset)
                throw new InvalidDataException("Invalid layout (NodeRegionOffset mismatch).");
            if (Header->ValueRegionOffset != valueRegionOffset)
                throw new InvalidDataException("Invalid layout (ValueRegionOffset mismatch).");

            // Basic range checks
            long maxNodes = (Header->ValueRegionOffset - Header->NodeRegionOffset) / sizeof(MmapNode);
            if (maxNodes < 2 || maxNodes != nodeCapacity)
            {
                // If you later allow “open existing with different capacities”, relax this check.
                throw new InvalidDataException("Node capacity mismatch.");
            }

            if (Header->NodeCount < 2 || Header->NodeCount > (uint)maxNodes)
                throw new InvalidDataException("Corrupt NodeCount.");

            if (Header->ValueTail < 0 || Header->ValueTail > valueCapacityBytes)
                throw new InvalidDataException("Corrupt ValueTail.");

            // Ensure root exists (defensive)
            _ = ref GetNode(1);
        }

        private void Map(long capacityBytes)
        {
            CapacityBytes = capacityBytes;

            _mmf = MemoryMappedFile.CreateFromFile(_path, FileMode.Open, mapName: null, capacity: capacityBytes,
                access: MemoryMappedFileAccess.ReadWrite);

            _view = _mmf.CreateViewAccessor(0, capacityBytes, MemoryMappedFileAccess.ReadWrite);
            _buffer = _view.SafeMemoryMappedViewHandle;

            byte* ptr = null;
            _buffer.AcquirePointer(ref ptr);
            BasePtr = ptr;
        }

        private static void EnsureFileExistsWithLength(string path, long length)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);

            // Create if missing
            if (!File.Exists(path))
            {
                using var fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
                fs.SetLength(length);
                fs.Flush(flushToDisk: true);
                return;
            }

            // If exists, enforce size (for now).
            var fi = new FileInfo(path);
            if (fi.Length != length)
            {
                // Simplest & safest: refuse.
                // If you want grow/rebuild later, implement that explicitly (your next step).
                throw new InvalidDataException($"Existing file length {fi.Length} != expected {length}.");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long AlignUp(long value, long align)
            => (value + (align - 1)) & ~(align - 1);

        public void Dispose()
        {
            // Release pointer first
            if (_buffer is not null && BasePtr != null)
            {
                _buffer.ReleasePointer();
                BasePtr = null;
            }

            _view?.Dispose();
            _mmf?.Dispose();

            _view = null;
            _mmf = null;
            _buffer = null;

            Header = null;
        }
    }
}
