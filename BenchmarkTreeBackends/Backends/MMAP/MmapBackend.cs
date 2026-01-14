// MmapBackend.cs
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

using BenchmarkTreeBackends.Codecs;
using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace BenchmarkTreeBackends.Backends.MMAP
{
    /// <summary>
    /// mmap-backed implementation of IBackend<TKey,TValue> with blue/green swaps.
    /// Reads come from an immutable, memory-mapped snapshot. All writes go to a staging trie until Swap() publishes.
    /// </summary>
    public abstract unsafe partial class MmapBackend<TKey, TValue> : IBackend<TKey, TValue>, IDisposable
        where TValue : class
    {
        protected readonly IValueCodec<TValue> _codec;
        private readonly string _filePath;

        private volatile State? _active;

        private readonly object _stagingLock = new();
        private TrieNode _stagingRoot;
        private bool _stagingLoadedFromActive;

        private volatile bool _disposed;

        protected MmapBackend(string filePath, IValueCodec<TValue> codec)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("File path must be non-empty.", nameof(filePath));

            _codec = codec ?? throw new ArgumentNullException(nameof(codec));
            _filePath = filePath;

            if (!File.Exists(_filePath))
                CreateEmptyFile(_filePath);

            _active = State.OpenReadOnly(_filePath);
            _stagingRoot = TrieNode.CreateRoot();
            _stagingLoadedFromActive = false;
        }

        // User-provided key serialization (DomainTree-compatible). Must allow empty key (root).
        public abstract byte[]? ConvertToByteKey(TKey key, bool throwException = true);

        public bool IsEmpty
        {
            get
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                using var lease = AcquireActive();

                // Root is index 1
                ref readonly var root = ref lease.State.GetNodeAtIndex(1);

                if (root.ValueOffset != 0)
                    return false;

                unsafe
                {
                    fixed (MmapNode* n = &root)
                    {
                        uint* p = n->Children;

                        for (int i = 0; i < 256; i++)
                        {
                            if (p[i] != 0)
                                return false;
                        }
                    }
                }


                return true;
            }
        }

        public TValue this[TKey? key]
        {
            get
            {
                if (key is null) throw new ArgumentNullException(nameof(key));
                if (TryGet(key, out var v)) return v;
                throw new KeyNotFoundException();
            }
            set
            {
                if (key is null) throw new ArgumentNullException(nameof(key));
                ArgumentNullException.ThrowIfNull(value);
                AddOrUpdate(key, value, (_, __) => value);
            }
        }

        // -----------------------------
        // Reads (from active snapshot)
        // -----------------------------

        public bool ContainsKey(TKey key)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            using var lease = AcquireActive();
            return lease.State.TryFindNode(this, key, out _, requireValue: false);
        }

        public bool TryGet(TKey key, out TValue value)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            using var lease = AcquireActive();

            if (!lease.State.TryFindNode(this, key, out uint nodeIndex, requireValue: true))
            {
                value = null!;
                return false;
            }

            ref readonly var node = ref lease.State.GetNodeAtIndex(nodeIndex);
            if (!lease.State.TryReadValueBytes(node, out ReadOnlySpan<byte> payload))
            {
                value = null!;
                return false;
            }

            value = _codec.Decode(payload);
            return true;
        }

        public IEnumerator<TValue> GetEnumerator()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var lease = AcquireActive(); // enumerator must hold the lease until disposed
            return new MmapEnumerator(this, lease, reverse: false);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public IEnumerable<TValue> GetReverseEnumerable()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return new ReverseEnumerable(this);
        }

        private sealed class ReverseEnumerable : IEnumerable<TValue>
        {
            private readonly MmapBackend<TKey, TValue> _owner;

            public ReverseEnumerable(MmapBackend<TKey, TValue> owner) => _owner = owner;

            public IEnumerator<TValue> GetEnumerator()
            {
                var lease = _owner.AcquireActive();
                return new MmapEnumerator(_owner, lease, reverse: true);
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        // -----------------------------
        // Mutations (staging only)
        // MUST call Swap() to publish
        // -----------------------------

        public void Add(TKey key, TValue value)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ArgumentNullException.ThrowIfNull(key);
            ArgumentNullException.ThrowIfNull(value);

            var bKey = ConvertToByteKey(key, true) ?? throw new InvalidOperationException("ConvertToByteKey returned null.");
            var bytes = _codec.Encode(value);

            lock (_stagingLock)
            {
                EnsureStagingLoadedFromActive_NoLock();
                if (!TrieInsertOrUpdate_NoLock(_stagingRoot, bKey, bytes, allowOverwrite: false))
                    throw new ArgumentException("Key already exists.", nameof(key));
            }
        }

        public bool TryAdd(TKey key, TValue? value)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (key is null) throw new ArgumentNullException(nameof(key));
            if (value is null) return false;

            byte[]? bKey;
            try { bKey = ConvertToByteKey(key, throwException: false); }
            catch { return false; }
            if (bKey is null) return false;

            var bytes = _codec.Encode(value);

            lock (_stagingLock)
            {
                EnsureStagingLoadedFromActive_NoLock();
                return TrieInsertOrUpdate_NoLock(_stagingRoot, bKey, bytes, allowOverwrite: false);
            }
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (key is null) throw new ArgumentNullException(nameof(key));
            ArgumentNullException.ThrowIfNull(valueFactory);

            var bKey = ConvertToByteKey(key, true) ?? throw new InvalidOperationException("ConvertToByteKey returned null.");

            lock (_stagingLock)
            {
                EnsureStagingLoadedFromActive_NoLock();

                if (!TrieTryGetValueBytes_NoLock(_stagingRoot, bKey, out var existing))
                    return _codec.Decode(existing);

                var created = valueFactory(key);
                var bytes = _codec.Encode(created);
                TrieInsertOrUpdate_NoLock(_stagingRoot, bKey, bytes, allowOverwrite: true);
                return created;
            }
        }

        public TValue GetOrAdd(TKey key, TValue value) => GetOrAdd(key, _ => value);

        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (key is null) throw new ArgumentNullException(nameof(key));
            ArgumentNullException.ThrowIfNull(addValueFactory);
            ArgumentNullException.ThrowIfNull(updateValueFactory);

            var bKey = ConvertToByteKey(key, true) ?? throw new InvalidOperationException("ConvertToByteKey returned null.");

            lock (_stagingLock)
            {
                EnsureStagingLoadedFromActive_NoLock();

                if (!TrieTryGetValueBytes_NoLock(_stagingRoot, bKey, out var existing))
                {
                    var existingValue = _codec.Decode(existing);
                    var updated = updateValueFactory(key, existingValue);
                    TrieInsertOrUpdate_NoLock(_stagingRoot, bKey, _codec.Encode(updated), allowOverwrite: true);
                    return updated;
                }
                else
                {
                    var added = addValueFactory(key);
                    TrieInsertOrUpdate_NoLock(_stagingRoot, bKey, _codec.Encode(added), allowOverwrite: true);
                    return added;
                }
            }
        }

        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
            => AddOrUpdate(key, _ => addValue, updateValueFactory);

        public bool TryRemove(TKey key, out TValue? value)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (key is null) throw new ArgumentNullException(nameof(key));

            byte[]? bKey;
            try { bKey = ConvertToByteKey(key, throwException: false); }
            catch { value = null; return false; }
            if (bKey is null) { value = null; return false; }

            lock (_stagingLock)
            {
                EnsureStagingLoadedFromActive_NoLock();
                if (!TrieTryRemove_NoLock(_stagingRoot, bKey, out var removedBytes) || removedBytes is null)
                {
                    value = null;
                    return false;
                }

                value = _codec.Decode(removedBytes);
                return true;
            }
        }

        public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (key is null) throw new ArgumentNullException(nameof(key));
            ArgumentNullException.ThrowIfNull(newValue);
            ArgumentNullException.ThrowIfNull(comparisonValue);

            byte[]? bKey;
            try { bKey = ConvertToByteKey(key, throwException: false); }
            catch { return false; }
            if (bKey is null) return false;

            var newBytes = _codec.Encode(newValue);
            var cmpBytes = _codec.Encode(comparisonValue);

            lock (_stagingLock)
            {
                EnsureStagingLoadedFromActive_NoLock();

                if (!TrieTryGetValueBytes_NoLock(_stagingRoot, bKey, out var existing))
                    return false;

                if (!existing.SequenceEqual(cmpBytes))
                    return false;

                TrieInsertOrUpdate_NoLock(_stagingRoot, bKey, newBytes, allowOverwrite: true);
                return true;
            }
        }

        public void Clear()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            lock (_stagingLock)
            {
                EnsureStagingLoadedFromActive_NoLock();
                _stagingRoot = TrieNode.CreateRoot();
            }
        }

        /// <summary>
        /// Publishes the current staging trie by building a new mmap file and atomically swapping the active mapping.
        /// If anything fails while building/writing/opening the new snapshot, the active snapshot remains unchanged.
        /// </summary>
        public void Swap(bool parallelWrite = false)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            TrieNode snapshot;
            lock (_stagingLock)
            {
                EnsureStagingLoadedFromActive_NoLock();
                snapshot = _stagingRoot.Clone(); // stable copy
            }

            string tmpPath = _filePath + ".tmp";

            try
            {
                BuildFileFromTrie(snapshot, tmpPath, parallelWrite);

                if (File.Exists(_filePath))
                {
                    try
                    {
                        string bak = _filePath + ".bak";
                        File.Replace(tmpPath, _filePath, bak, ignoreMetadataErrors: true);
                        TryDeleteQuiet(bak);
                    }
                    catch
                    {
                        File.Delete(_filePath);
                        File.Move(tmpPath, _filePath);
                    }
                }
                else
                {
                    File.Move(tmpPath, _filePath);
                }

                // Open new mapping first; if this fails, do not swap.
                var newState = State.OpenReadOnly(_filePath);

                // Publish new snapshot
                var old = Interlocked.Exchange(ref _active, newState);
                old?.RetireAndTryDispose();
            }
            finally
            {
                TryDeleteQuiet(tmpPath);
            }
        }

        // -----------------------------
        // Active snapshot leasing (safe swap without reader locks)
        // -----------------------------

        private ActiveLease AcquireActive()
        {
            var s = Volatile.Read(ref _active) ?? throw new ObjectDisposedException(GetType().Name);
            s.AddRef();
            return new ActiveLease(s);
        }

        private readonly struct ActiveLease : IDisposable
        {
            public readonly State State;

            public ActiveLease(State s) => State = s;

            public void Dispose() => State.Release();
        }

        // -----------------------------
        // File creation / loading / building
        // -----------------------------

        private static void CreateEmptyFile(string path)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);

            long headerSize = sizeof(MmapHeader);
            long nodeSize = sizeof(MmapNode);

            var header = new MmapHeader
            {
                Magic = DomainTreeMmapFormat.Magic,
                Version = DomainTreeMmapFormat.Version,
                Endianness = DomainTreeMmapFormat.LittleEndian,
                NodeRegionOffset = headerSize,
                NodeCount = 2, // index 0 = sentinel/null, index 1 = root
                ValueRegionOffset = headerSize + nodeSize * 2
            };

            var sentinel = new MmapNode(); // all zeros
            var root = new MmapNode { LabelId = 0, ValueOffset = 0, ValueLength = 0 };

            using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
            using var bw = new BinaryWriter(fs, Encoding.UTF8, leaveOpen: false);

            bw.WriteStruct(header);
            bw.WriteStruct(sentinel);
            bw.WriteStruct(root);

            bw.Flush();
            fs.Flush(flushToDisk: true);
        }

        private void EnsureStagingLoadedFromActive_NoLock()
        {
            if (_stagingLoadedFromActive)
                return;

            using var lease = AcquireActive();
            _stagingRoot = TrieNode.CreateRoot();

            var q = new Queue<(uint nodeIndex, TrieNode tnode)>();
            q.Enqueue((1u, _stagingRoot)); // root index = 1

            while (q.Count > 0)
            {
                var (idx, tnode) = q.Dequeue();
                ref readonly var n = ref lease.State.GetNodeAtIndex(idx);

                if (lease.State.TryReadValueBytes(n, out var payload))
                {
                    var copy = payload.ToArray();
                    tnode.ValueBytes = copy;
                }

                unsafe
                {
                    fixed (uint* p = n.Children)
                    {
                        for (int b = 0; b < 256; b++)
                        {
                            uint childIndex = p[b];
                            if (childIndex == 0)
                                continue;

                            byte label = (byte)b;
                            var childTrie = tnode.GetOrCreateChild(label);
                            q.Enqueue((childIndex, childTrie));
                        }
                    }
                }
            }

            _stagingLoadedFromActive = true;
        }

        private static void BuildFileFromTrie(TrieNode root, string outputPath, bool parallelWrite)
        {
            // Node 0 = sentinel, Node 1 = root
            var nodes = new List<MmapNode>(capacity: 1024);
            var meta = new List<TrieNode>(capacity: 1024);

            nodes.Add(new MmapNode());              // index 0: sentinel
            meta.Add(TrieNode.CreateRoot());        // unused placeholder

            nodes.Add(new MmapNode { LabelId = 0 }); // index 1: root
            meta.Add(root);

            var q = new Queue<int>();
            q.Enqueue(1);

            while (q.Count > 0)
            {
                int parentIndex = q.Dequeue();
                var tnode = meta[parentIndex];
                var parentNode = nodes[parentIndex];

                var children = tnode.GetChildrenSorted();
                if (children.ToArray().Length == 0)
                {
                    nodes[parentIndex] = parentNode;
                    continue;
                }

                unsafe
                {
                    uint* p = parentNode.Children;
                    {
                        foreach (var (label, childTrie) in children)
                        {
                            int childIndex = nodes.Count;

                            p[label] = (uint)childIndex;

                            nodes.Add(new MmapNode
                            {
                                LabelId = DomainTreeMmapFormat.ToLabelId(label),
                                ValueOffset = 0,
                                ValueLength = 0
                            });

                            meta.Add(childTrie);
                            q.Enqueue(childIndex);
                        }
                    }
                }

                nodes[parentIndex] = parentNode;
            }

            // Assign value offsets (relative to ValueRegionOffset)
            long valueCursor = 0;
            for (int i = 1; i < meta.Count; i++)
            {
                var t = meta[i];
                var n = nodes[i];

                if (t.ValueBytes is null || t.ValueBytes.Length == 0)
                {
                    n.ValueOffset = 0;
                    n.ValueLength = 0;
                }
                else
                {
                    n.ValueOffset = valueCursor;
                    n.ValueLength = t.ValueBytes.Length;
                    valueCursor += 4L + t.ValueBytes.Length;
                }

                nodes[i] = n;
            }

            long headerSize = sizeof(MmapHeader);
            long nodeSize = sizeof(MmapNode);

            long nodeRegionOffset = headerSize;
            long valueRegionOffset = nodeRegionOffset + nodes.Count * nodeSize;

            var header = new MmapHeader
            {
                Magic = DomainTreeMmapFormat.Magic,
                Version = DomainTreeMmapFormat.Version,
                Endianness = DomainTreeMmapFormat.LittleEndian,
                NodeRegionOffset = nodeRegionOffset,
                NodeCount = nodes.Count,
                ValueRegionOffset = valueRegionOffset
            };

            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(outputPath))!);

            using var fs = new FileStream(
                outputPath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 1 << 20,
                options: FileOptions.SequentialScan);

            // Write header
            {
                Span<byte> hdrBuf = stackalloc byte[sizeof(MmapHeader)];
                WriteHeaderLE(hdrBuf, header);
                fs.Write(hdrBuf);
            }

            // Write nodes
            int totalNodeBytes = checked((int)(nodes.Count * nodeSize));
            byte[] nodeBlock = new byte[totalNodeBytes];

            unsafe
            {
                fixed (byte* p = nodeBlock)
                {
                    for (int i = 0; i < nodes.Count; i++)
                        *(MmapNode*)(p + i * nodeSize) = nodes[i];
                }
            }

            if (parallelWrite && totalNodeBytes >= (8 << 20))
            {
                const int chunk = 4 << 20;
                int offset = 0;
                while (offset < totalNodeBytes)
                {
                    int len = Math.Min(chunk, totalNodeBytes - offset);
                    fs.Write(nodeBlock, offset, len);
                    offset += len;
                }
            }
            else
            {
                fs.Write(nodeBlock, 0, totalNodeBytes);
            }

            // Write value blobs
            for (int i = 0; i < meta.Count; i++)
            {
                var vb = meta[i].ValueBytes;
                if (vb is null || vb.Length == 0)
                    continue;

                Span<byte> lenBuf = stackalloc byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(lenBuf, vb.Length);
                fs.Write(lenBuf);
                fs.Write(vb, 0, vb.Length);
            }

            fs.Flush(flushToDisk: true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteHeaderLE(Span<byte> dst, in MmapHeader h)
        {
            // Explicit LE writes, matching Pack=1 and field order.
            int o = 0;
            BinaryPrimitives.WriteUInt32LittleEndian(dst.Slice(o, 4), h.Magic); o += 4;
            BinaryPrimitives.WriteUInt16LittleEndian(dst.Slice(o, 2), h.Version); o += 2;
            BinaryPrimitives.WriteUInt16LittleEndian(dst.Slice(o, 2), h.Endianness); o += 2;
            BinaryPrimitives.WriteInt64LittleEndian(dst.Slice(o, 8), h.NodeRegionOffset); o += 8;
            BinaryPrimitives.WriteInt64LittleEndian(dst.Slice(o, 8), h.NodeCount); o += 8;
            BinaryPrimitives.WriteInt64LittleEndian(dst.Slice(o, 8), h.ValueRegionOffset); o += 8;
        }

        private static void TryDeleteQuiet(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }

        // -----------------------------
        // Staging trie implementation
        // -----------------------------

        private static bool TrieInsertOrUpdate_NoLock(TrieNode root, byte[] key, byte[] valueBytes, bool allowOverwrite)
        {
            var n = root;
            for (int i = 0; i < key.Length; i++)
                n = n.GetOrCreateChild(key[i]);

            if (!allowOverwrite && n.ValueBytes is not null)
                return false;

            // staging must own the bytes; store as-is (codec already allocated).
            n.ValueBytes = valueBytes;
            return true;
        }

        private static bool TrieTryGetValueBytes_NoLock(TrieNode root, byte[] key, out ReadOnlySpan<byte> valueBytes)
        {
            var n = root;

            for (int i = 0; i < key.Length; i++)
            {
                if (!n.TryGetChild(key[i], out var child))
                {
                    valueBytes = default;
                    return false; // path missing => key not present
                }

                n = child;
            }

            // Key path exists, but value may not.
            var vb = n.ValueBytes;
            if (vb is null || vb.Length == 0)
            {
                valueBytes = default;
                return false; // present-node-without-value is "not found" for TryGet semantics
            }

            valueBytes = vb;
            return true;
        }

        private static bool TrieTryRemove_NoLock(TrieNode root, byte[] key, out byte[]? removedBytes)
        {
            // Track path for pruning
            TrieNode[] stack = new TrieNode[key.Length + 1];
            byte[] labels = new byte[key.Length];

            var n = root;
            stack[0] = root;

            for (int i = 0; i < key.Length; i++)
            {
                labels[i] = key[i];
                if (!n.TryGetChild(key[i], out var child))
                {
                    removedBytes = null;
                    return false;
                }
                n = child;
                stack[i + 1] = n;
            }

            if (n.ValueBytes is null)
            {
                removedBytes = null;
                return false;
            }

            removedBytes = n.ValueBytes;
            n.ValueBytes = null;

            // Prune empty nodes bottom-up
            for (int i = key.Length - 1; i >= 0; i--)
            {
                var cur = stack[i + 1];
                if (cur.ValueBytes is not null || cur.ChildrenCount != 0)
                    break;

                var parent = stack[i];
                parent.RemoveChild(labels[i]);
            }

            return true;
        }

        private void ThrowIfDisposed()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
        }

        #region Dispose

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    var s = Interlocked.Exchange(ref _active, null);
                    if (s is not null)
                    {
                        // Drop publisher ref; actual dispose occurs when last reader finishes.
                        s.RetireAndTryDispose();
                    }
                }
                _disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        #endregion Dispose
    }
}