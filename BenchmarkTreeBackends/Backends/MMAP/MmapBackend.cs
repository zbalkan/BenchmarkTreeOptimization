using BenchmarkTreeBackends.Codecs;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace BenchmarkTreeBackends.Backends.MMAP
{
    public abstract unsafe partial class MmapBackend<TKey, TValue> : IBackend<TKey, TValue>, IDisposable
        where TValue : class
    {
        private readonly IValueCodec<TValue> _codec;
        private readonly MmapFile _file;
        private readonly Lock _lock = new();
        private readonly MmapTrieReader _reader;
        private readonly MmapTrieWriter _writer;
        private bool _disposed;

        protected MmapBackend(string path, IValueCodec<TValue> codec)
        {
            _codec = codec;

            var tempPath = path + ".tmp";
            var backupPath = path + ".bak";

            // 1) Recover from previous crash if needed
            RecoverIfNeeded(path, tempPath, backupPath);

            // 2) Open existing file
            var oldFile = new MmapFile(path);

            // 3) Capture layout (same capacity)
            uint nodeCap =
                (uint)((oldFile.Header->ValueRegionOffset - oldFile.Header->NodeRegionOffset) / sizeof(MmapNode));

            long valueCap =
                oldFile.CapacityBytes - oldFile.Header->ValueRegionOffset;

            // 4) Create temp file
            if (File.Exists(tempPath))
                File.Delete(tempPath);

            var newFile = new MmapFile(tempPath, nodeCap, valueCap);
            var newWriter = new MmapTrieWriter(newFile);
            var oldReader = new MmapTrieReader(oldFile);

            // 5) Copy live entries
            ForEachLiveEntry(oldFile, oldReader, (key, payload) =>
            {
                newWriter.InsertOrUpdate(key, payload, overwrite: true);
            });

            // 6) Flush + close temp file
            newFile.Flush();
            newFile.Dispose();

            // 7) Close old file before replace
            oldFile.Dispose();

            // 8) Atomic replace (with backup)
            ReplaceWithBackup(path, tempPath, backupPath);

            // 9) Verify new file opens correctly
            try
            {
                _file = new MmapFile(path);
            }
            catch
            {
                // Roll back to old file if verification fails
                Rollback(path, backupPath);
                _file = new MmapFile(path);
            }

            // 10) Success → remove backup
            if (File.Exists(backupPath))
                File.Delete(backupPath);

            _reader = new MmapTrieReader(_file);
            _writer = new MmapTrieWriter(_file);
        }

        public bool IsEmpty => _file.Header->ValueCount == 0;

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
                AddOrUpdate(key, value, (_, __) => value);
            }
        }

        public void Add(TKey key, TValue value)
        {
            var b = ConvertToByteKey(key, true)!;
            var payload = _codec.Encode(value);

            lock (_lock)
            {
                if (!_writer.InsertOrUpdate(b, payload, overwrite: false))
                    throw new ArgumentException("Key exists");
            }
        }

        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            var b = ConvertToByteKey(key, true)!;

            lock (_lock)
            {
                // ATOMIC: Check + act under single lock
                if (_reader.TryFindNode(b, out var idx) && _reader.TryGetValue(idx, out var payload))
                {
                    var existing = _codec.Decode(payload);
                    var updated = updateValueFactory(key, existing);
                    _writer.InsertOrUpdate(b, _codec.Encode(updated), overwrite: true);
                    return updated;
                }

                var added = addValueFactory(key);
                _writer.InsertOrUpdate(b, _codec.Encode(added), overwrite: true);
                return added;
            }
        }

        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            return AddOrUpdate(key, _ => addValue, updateValueFactory);
        }

        public void Clear()
        {
            lock (_lock)
            {
                _file.Header->NodeCount = 2; // sentinel + root
                _file.Header->ValueCount = 0;
                _file.Header->ValueTail = 0;

                ref var root = ref _file.GetNode(1);
                root.Flags = 0;
                root.ValueOffset = 0;
                root.ValueLength = 0;

                fixed (uint* p = root.Children)
                    for (int i = 0; i < 256; i++)
                        p[i] = 0;
            }
        }

        public bool ContainsKey(TKey key)
        {
            var b = ConvertToByteKey(key, false);
            if (b is null) return false;
            return _reader.TryFindNode(b, out _);
        }

        public abstract byte[]? ConvertToByteKey(TKey key, bool throwException = true);

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public IEnumerator<TValue> GetEnumerator()
        {
            var stack = new Stack<uint>();
            stack.Push(1);

            while (stack.Count > 0)
            {
                uint idx = stack.Pop();
                ref var n = ref _file.GetNode(idx);

                if ((n.Flags & 1u) != 0 &&
                    _reader.TryGetValue(idx, out var payload))
                    yield return _codec.Decode(payload);
                unsafe
                {
                    fixed (uint* p = n.Children)
                        for (int i = 255; i >= 0; i--)
                            if (p[i] != 0) stack.Push(p[i]);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => new DfsEnumerator(this, reverse: false);

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            var b = ConvertToByteKey(key, true)!;

            lock (_lock)
            {
                if (_reader.TryFindNode(b, out var idx) && _reader.TryGetValue(idx, out var payload))
                    return _codec.Decode(payload);

                var created = valueFactory(key);
                _writer.InsertOrUpdate(b, _codec.Encode(created), overwrite: true);
                return created;
            }
        }

        public TValue GetOrAdd(TKey key, TValue value)
        {
            return GetOrAdd(key, _ => value);
        }

        public IEnumerable<TValue> GetReverseEnumerable() => new ReverseEnumerable(this);

        public bool TryAdd(TKey key, TValue? value)
        {
            if (value is null) return false;

            var b = ConvertToByteKey(key, false);
            if (b is null) return false;

            var payload = _codec.Encode(value);

            lock (_lock)
            {
                return _writer.InsertOrUpdate(b, payload, overwrite: false);
            }
        }

        public bool TryGet(TKey key, out TValue value)
        {
            var b = ConvertToByteKey(key, false);
            if (b is null) { value = null!; return false; }

            if (!_reader.TryFindNode(b, out var idx) ||
                !_reader.TryGetValue(idx, out var payload))
            {
                value = null!;
                return false;
            }

            value = _codec.Decode(payload);
            return true;
        }

        public bool TryRemove(TKey key)
        {
            var b = ConvertToByteKey(key, false);
            if (b is null) return false;

            lock (_lock)
            {
                return _writer.TryRemove(b);
            }
        }

        public bool TryRemove(TKey key, out TValue? value)
        {
            var b = ConvertToByteKey(key, false);
            if (b is null) { value = null; return false; }

            if (_reader.TryFindNode(b, out var idx) &&
                _reader.TryGetValue(idx, out var payload))
            {
                value = _codec.Decode(payload);
            }
            else
            {
                value = null;
            }

            lock (_lock)
                return _writer.TryRemove(b);
        }

        public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
        {
            var b = ConvertToByteKey(key, false);
            if (b is null) return false;

            var newPayload = _codec.Encode(newValue);
            var cmpPayload = _codec.Encode(comparisonValue);

            lock (_lock)
            {
                if (!_reader.TryFindNode(b, out var idx) ||
                    !_reader.TryGetValue(idx, out var existing))
                    return false;

                if (!existing.SequenceEqual(cmpPayload))
                    return false;

                _writer.InsertOrUpdate(b, newPayload, overwrite: true);
                return true;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _file.Dispose();
                }

                _disposed = true;
            }
        }

        private static void ForEachLiveEntry(
                    MmapFile file,
                    MmapTrieReader reader,
                    Action<byte[], ReadOnlySpan<byte>> visitor)
        {
            var path = new byte[256];
            var stack = new Stack<Frame>(64);

            stack.Push(new Frame { Node = 1, Depth = 0 });

            while (stack.Count > 0)
            {
                var f = stack.Pop();
                ref var n = ref file.GetNode(f.Node);

                if ((n.Flags & 1u) != 0 &&
                    reader.TryGetValue(f.Node, out var payload))
                {
                    var key = new byte[f.Depth];
                    Buffer.BlockCopy(path, 0, key, 0, f.Depth);
                    visitor(key, payload);
                }

                fixed (uint* p = n.Children)
                {
                    for (int i = 255; i >= 0; i--)
                    {
                        uint child = p[i];
                        if (child == 0) continue;

                        path[f.Depth] = (byte)i;

                        stack.Push(new Frame
                        {
                            Node = child,
                            Depth = f.Depth + 1
                        });
                    }
                }
            }
        }

        private static void RecoverIfNeeded(string path, string tempPath, string backupPath)
        {
            // If main file is missing but temp exists → promote temp
            if (!File.Exists(path) && File.Exists(tempPath))
            {
                File.Move(tempPath, path);
                return;
            }

            // If backup exists and main file is missing → restore backup
            if (File.Exists(backupPath) && !File.Exists(path))
            {
                File.Move(backupPath, path);
                return;
            }

            // Clean stray temp if main file exists
            if (File.Exists(path) && File.Exists(tempPath))
                File.Delete(tempPath);
        }

        private static void ReplaceWithBackup(string path, string tempPath, string backupPath)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (File.Exists(backupPath))
                    File.Delete(backupPath);

                File.Replace(tempPath, path, backupPath);
            }
            else
            {
                if (File.Exists(backupPath))
                    File.Delete(backupPath);

                File.Move(path, backupPath);
                File.Move(tempPath, path);
            }
        }

        private static void Rollback(string path, string backupPath)
        {
            if (File.Exists(path))
                File.Delete(path);

            if (File.Exists(backupPath))
                File.Move(backupPath, path);
        }
        private struct Frame
        {
            public int Depth;
            public uint Node;
        }
    }
}
