using BenchmarkTreeOptimization.Codecs;
using LightningDB;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace BenchmarkTreeOptimization.Backends.LMDB
{
    /// <summary>
    /// LMDB-backed IBackend<TKey, TValue>.
    /// The user controls key serialization via ConvertToByteKey().
    /// </summary>
    public abstract class LmdbBackend<TKey, TValue> : IBackend<TKey, TValue>, IDisposable
        where TValue : class
    {
        #region variables

        protected readonly IValueCodec<TValue> _codec;
        protected readonly LightningDatabase _db;
        protected readonly LightningEnvironment _env;
        private readonly ThreadLocal<LightningTransaction?> _readTx = new ThreadLocal<LightningTransaction?>(() => null, trackAllValues: true);

        private readonly ThreadLocal<int> _seenWriteVersion = new ThreadLocal<int>(() => -1, trackAllValues: true);

        private volatile bool _disposed;
        private int _writeVersion;

        #endregion variables

        #region constructor

        protected LmdbBackend(
    string environmentPath,
    string? databaseName = null,
    DatabaseConfiguration? databaseConfiguration = null,
    LmdbTreeOptions? options = null,
    IValueCodec<TValue>? codec = null)
        {
            if (string.IsNullOrWhiteSpace(environmentPath))
                throw new ArgumentException("Environment path must be non-empty.", nameof(environmentPath));

            options ??= new LmdbTreeOptions();
            _codec = codec ?? throw new ArgumentNullException(nameof(codec));

            var envConfig = new EnvironmentConfiguration
            {
                MapSize = options.MapSizeBytes,
                MaxDatabases = options.MaxDatabases,
                MaxReaders = options.MaxReaders
            };

            _env = new LightningEnvironment(environmentPath, envConfig);
            _env.Open(EnvironmentOpenFlags.None);

            databaseConfiguration ??= new DatabaseConfiguration
            {
                Flags = DatabaseOpenFlags.Create
            };

            using var tx = _env.BeginTransaction();
            _db = databaseName is null
                ? tx.OpenDatabase(configuration: databaseConfiguration)
                : tx.OpenDatabase(databaseName, databaseConfiguration);

            tx.Commit().ThrowOnError();
        }

        #endregion constructor

        #region public
        public void Add(TKey key, TValue value)
        {
            ThrowIfDisposed();

            var k = GetKeyBytes(key);

            using var tx = _env.BeginTransaction();
            var rc = tx.Put(_db, k, _codec.Encode(value), PutOptions.NoOverwrite);

            if (rc == MDBResultCode.KeyExist)
                throw new ArgumentException("Key already exists.", nameof(key));

            rc.ThrowOnError();
            tx.Commit().ThrowOnError();
            OnWriteCommitted();
        }

        public TValue AddOrUpdate(
            TKey key,
            Func<TKey, TValue> addValueFactory,
            Func<TKey, TValue, TValue> updateValueFactory)
        {
            ThrowIfDisposed();

            var k = GetKeyBytes(key);

            using var tx = _env.BeginTransaction();

            if (tx.TryGet(_db, k, out var bytes) && bytes is not null)
            {
                var existing = _codec.Decode(bytes);
                var updated = updateValueFactory(key, existing);
                tx.Put(_db, k, _codec.Encode(updated)).ThrowOnError();
                tx.Commit().ThrowOnError();
                OnWriteCommitted();
                return updated;
            }
            else
            {
                var added = addValueFactory(key);
                tx.Put(_db, k, _codec.Encode(added)).ThrowOnError();
                tx.Commit().ThrowOnError();
                OnWriteCommitted();
                return added;
            }
        }

        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
            => AddOrUpdate(key, _ => addValue, updateValueFactory);

        public void Clear()
        {
            ThrowIfDisposed();

            using var tx = _env.BeginTransaction();
            _db.Truncate(tx);
            tx.Commit().ThrowOnError();
            OnWriteCommitted();
        }

        public bool ContainsKey(TKey key)
        {
            ThrowIfDisposed();

            var k = GetKeyBytes(key);
            var tx = GetReadTx();
            return tx.ContainsKey(_db, k);
        }

        // User-provided key serialization
        public abstract byte[]? ConvertToByteKey(TKey key, bool throwException = true);

        public IEnumerator<TValue> GetEnumerator()
        {
            ThrowIfDisposed();

            return new LmdbEnumerator(this, _db, _codec, reverse: false);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            ThrowIfDisposed();

            var k = GetKeyBytes(key);

            var rtx = GetReadTx();
            {
                if (rtx.TryGet(_db, k, out var bytes) && bytes is not null)
                    return _codec.Decode(bytes);
            }

            using var wtx = _env.BeginTransaction();

            if (wtx.TryGet(_db, k, out var existing) && existing is not null)
                return _codec.Decode(existing);

            var created = valueFactory(key);
            wtx.Put(_db, k, _codec.Encode(created), PutOptions.NoOverwrite).ThrowOnError();
            wtx.Commit().ThrowOnError();
            OnWriteCommitted();
            return created;
        }

        public TValue GetOrAdd(TKey key, TValue value)
            => GetOrAdd(key, _ => value);

        public IEnumerable<TValue> GetReverseEnumerable()
            => new ReverseEnumerable(this);

        public bool TryAdd(TKey key, TValue? value)
        {
            ThrowIfDisposed();

            if (value is null) return false;

            var k = GetKeyBytes(key);

            using var tx = _env.BeginTransaction();
            var rc = tx.Put(_db, k, _codec.Encode(value), PutOptions.NoOverwrite);

            if (rc == MDBResultCode.KeyExist)
                return false;

            rc.ThrowOnError();
            tx.Commit().ThrowOnError();
            OnWriteCommitted();
            return true;
        }

        public bool TryGet(TKey key, [NotNullWhen(true)] out TValue value)
        {
            ThrowIfDisposed();

            var k = GetKeyBytes(key);

            var tx = GetReadTx();

            if (tx.TryGet(_db, k, out var bytes) && bytes is not null)
            {
                value = _codec.Decode(bytes);
                return true;
            }

            value = null!;
            return false;
        }

        public bool TryRemove(TKey key, out TValue? value)
        {
            ThrowIfDisposed();

            var k = GetKeyBytes(key);

            using var tx = _env.BeginTransaction();

            if (!tx.TryGet(_db, k, out var bytes) || bytes is null)
            {
                value = null;
                return false;
            }

            value = _codec.Decode(bytes);

            tx.Delete(_db, k).ThrowOnError();
            var res = tx.Commit().ThrowOnError();
            OnWriteCommitted();
            return res.HasFlag(MDBResultCode.Success);
        }

        public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
        {
            ThrowIfDisposed();

            var k = GetKeyBytes(key);

            var newBytes = _codec.Encode(newValue);
            var cmpBytes = _codec.Encode(comparisonValue);

            using var tx = _env.BeginTransaction();

            if (!tx.TryGet(_db, k, out var existing) || existing is null)
                return false;

            if (!existing.AsSpan().SequenceEqual(cmpBytes))
                return false;

            tx.Put(_db, k, newBytes).ThrowOnError();
            tx.Commit().ThrowOnError();
            OnWriteCommitted();
            return true;
        }

        #endregion public

        #region private
        private byte[] GetKeyBytes(TKey key)
        {
            var bytes = ConvertToByteKey(key, true) ?? throw new InvalidOperationException("ConvertToByteKey returned null.");
            if (bytes.Length == 0)
            {
                // An empty key is not allowed in LMDB, but in DNS specifications it means root.
                // So we map it to an empty array for external values, single 0 byte for internal values.
                bytes = new byte[] { 0 };
            }

            return bytes;
        }

        private LightningTransaction GetReadTx()
        {
            ObjectDisposedException.ThrowIf(_disposed, nameof(_readTx));

            int currentVersion = Volatile.Read(ref _writeVersion);

            var tx = _readTx.Value;
            var seen = _seenWriteVersion.Value;

            if (tx == null)
            {
                tx = _env.BeginTransaction(TransactionBeginFlags.ReadOnly);
                _readTx.Value = tx;
                _seenWriteVersion.Value = currentVersion;
                return tx;
            }

            if (seen != currentVersion)
            {
                // Release reader slot + snapshot
                tx.Reset();

                // Refresh snapshot cheaply
                tx.Renew();

                _seenWriteVersion.Value = currentVersion;
            }

            return tx;
        }

        private void OnWriteCommitted()
        {
            Interlocked.Increment(ref _writeVersion);
        }

        private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, nameof(LmdbBackend<TKey, TValue>));
        #endregion private


        #region properties
        public bool IsEmpty
        {
            get
            {
                var tx = GetReadTx();
                return tx.GetEntriesCount(_db) == 0;
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

        #endregion properties

        #region Dispose

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true;   // hard guard first

                if (disposing)
                {
                    // Best-effort: dispose all per-thread read transactions
                    foreach (var tx in _readTx.Values)
                        tx?.Dispose();

                    _readTx.Dispose();
                    _seenWriteVersion.Dispose();

                    _env.Flush(true);
                    _env.CheckStaleReaders();
                    _env.Dispose();
                }
            }
        }

        #endregion Dispose

        private sealed class LmdbEnumerator : IEnumerator<TValue>, IDisposable
        {
            #region variables

            private readonly IValueCodec<TValue> _codec;
            private readonly LightningDatabase _db;
            private readonly LightningEnvironment _env;
            private readonly bool _reverse;

            private TValue? _current;
            private LightningCursor? _cursor;
            private bool _finished;
            private bool _started;
            private LightningTransaction? _tx;
            private bool disposedValue;

            #endregion variables

            #region constructor

            public LmdbEnumerator(
                LmdbBackend<TKey, TValue> owner,
                LightningDatabase db,
                IValueCodec<TValue> codec,
                bool reverse)
            {
                _env = owner._env;
                _db = db;
                _codec = codec;
                _reverse = reverse;
            }

            #endregion constructor

            public TValue Current => _current ?? throw new InvalidOperationException();
            object IEnumerator.Current => Current;

            public bool MoveNext()
            {
                if (_finished) return false;
                EnsureOpen();

                if (!_started)
                {
                    var first = _reverse ? _cursor!.Last() : _cursor!.First();
                    if (first.resultCode != MDBResultCode.Success)
                    {
                        _finished = true;
                        return false;
                    }

                    _current = _codec.Decode(first.value.AsSpan());
                    _started = true;
                    return true;
                }

                var (resultCode, _, value) = _reverse ? _cursor!.Previous() : _cursor!.Next();
                if (resultCode != MDBResultCode.Success)
                {
                    _finished = true;
                    return false;
                }

                _current = _codec.Decode(value.AsSpan());
                return true;
            }

            public void Reset() => Dispose();

            private void EnsureOpen()
            {
                if (_tx != null) return;
                _tx = _env.BeginTransaction(TransactionBeginFlags.ReadOnly);
                _cursor = _tx.CreateCursor(_db);
            }

            #region Dispose

            public void Dispose()
            {
                // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
                Dispose(disposing: true);
                GC.SuppressFinalize(this);
            }

            private void Dispose(bool disposing)
            {
                if (!disposedValue)
                {
                    if (disposing)
                    {
                        _cursor?.Dispose();
                        _tx?.Dispose();
                        _cursor = null;
                        _tx = null;
                        _current = null;
                        _started = false;
                        _finished = false;
                    }

                    disposedValue = true;
                }
            }

            #endregion Dispose
        }

        private sealed class ReverseEnumerable : IEnumerable<TValue>
        {
            private readonly LmdbBackend<TKey, TValue> _owner;

            public ReverseEnumerable(LmdbBackend<TKey, TValue> owner) => _owner = owner;

            public IEnumerator<TValue> GetEnumerator()
            {
                _owner.ThrowIfDisposed();
                return new LmdbEnumerator(_owner, _owner._db, _owner._codec, reverse: true);
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
    }

    public sealed class LmdbTreeOptions
    {
        public long MapSizeBytes { get; set; } = 1L << 30; // 1GB by default
        public int MaxDatabases { get; set; } = 1;
        public int MaxReaders { get; set; } = 126;
    }
}