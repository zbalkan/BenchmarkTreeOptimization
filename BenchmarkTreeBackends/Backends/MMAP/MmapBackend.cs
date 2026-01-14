using BenchmarkTreeBackends.Codecs;
using System;
using System.Collections;
using System.Collections.Generic;
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
            _file = new MmapFile(path);
            _reader = new MmapTrieReader(_file);
            _writer = new MmapTrieWriter(_file);
            _codec = codec;
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

                // No existing - create new
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
    }
}