using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace BenchmarkTreeBackends.Backends.Graph
{
    public abstract class GraphDnsBackend<TKey, TValue> : IBackend<TKey, TValue> where TValue : class
    {
        protected readonly ConcurrentDictionary<TKey, ConcurrentBag<TValue>> _reverseIndex = new();
        protected readonly ConcurrentDictionary<TKey, DnsZoneNode<TValue>> _nodes = new();
        // ========== IBackend IMPLEMENTATION ==========

        public bool IsEmpty => _nodes.IsEmpty;

        public DnsZoneNode<TValue> this[TKey? key]
        {
            get => key is null ? throw new ArgumentNullException(nameof(key)) : GetOrCreateNode(key);
            set => AddOrUpdateNode(key, value);
        }
        TValue IBackend<TKey, TValue>.this[TKey? key] { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public void Add(TKey key, DnsZoneNode<TValue> value)
        {
            if (_nodes.TryAdd(key, value))
                IndexReverseRecords(key, value);
        }

        public void Add(TKey key, TValue value)
        {
            Add(key, value as DnsZoneNode<TValue> ?? throw new InvalidOperationException("Invalid value type"));
        }

        public DnsZoneNode<TValue> AddOrUpdate(TKey key, Func<TKey, DnsZoneNode<TValue>> addValueFactory, Func<TKey, DnsZoneNode<TValue>, DnsZoneNode<TValue>> updateValueFactory)
        {
            return _nodes.AddOrUpdate(key, addValueFactory, (k, node) => updateValueFactory(k, node));
        }

        public DnsZoneNode<TValue> AddOrUpdate(TKey key, DnsZoneNode<TValue> addValue, Func<TKey, DnsZoneNode<TValue>, DnsZoneNode<TValue>> updateValueFactory)
        {
            var result = _nodes.AddOrUpdate(key, addValue, (k, node) => updateValueFactory(k, node));
            IndexReverseRecords(key, result);
            return result;
        }

        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            return _nodes.AddOrUpdate(key,
                k => addValueFactory(k) as DnsZoneNode<TValue> ?? throw new InvalidOperationException("Invalid value type"),
                (k, node) => updateValueFactory(k, node as TValue ?? throw new InvalidOperationException("Invalid value type")) as DnsZoneNode<TValue> ?? throw new InvalidOperationException("Invalid value type")) as TValue ?? throw new InvalidOperationException("Invalid value type");
        }

        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            return _nodes.AddOrUpdate(key,
                addValue as DnsZoneNode<TValue> ?? throw new InvalidOperationException("Invalid value type"),
                (k, node) => updateValueFactory(k, node as TValue ?? throw new InvalidOperationException("Invalid value type")) as DnsZoneNode<TValue> ?? throw new InvalidOperationException("Invalid value type")) as TValue ?? throw new InvalidOperationException("Invalid value type");
        }

        public void Clear()
        {
            _nodes.Clear();
            _reverseIndex.Clear();
        }

        public bool ContainsKey(TKey key) => _nodes.ContainsKey(key);

        public abstract byte[]? ConvertToByteKey(TKey key, bool throwException = true);

        // ENUMERABLE SUPPORT
        public IEnumerator<DnsZoneNode<TValue>> GetEnumerator() => _nodes.Values.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator()
        {
            return _nodes.Values.Cast<TValue>().GetEnumerator();
        }

        public DnsZoneNode<TValue> GetOrAdd(TKey key, Func<TKey, DnsZoneNode<TValue>> valueFactory)
        {
            var node = _nodes.GetOrAdd(key, valueFactory);
            IndexReverseRecords(key, node);
            return node;
        }

        public DnsZoneNode<TValue> GetOrAdd(TKey key, DnsZoneNode<TValue> value)
        {
            var node = _nodes.GetOrAdd(key, value);
            IndexReverseRecords(key, node);
            return node;
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            return _nodes.GetOrAdd(key,
                k => valueFactory(k) as DnsZoneNode<TValue> ?? throw new InvalidOperationException("Invalid value type")) as TValue ?? throw new InvalidOperationException("Invalid value type");
        }

        public TValue GetOrAdd(TKey key, TValue value)
        {
            return _nodes.GetOrAdd(key,
                value as DnsZoneNode<TValue> ?? throw new InvalidOperationException("Invalid value type")) as TValue ?? throw new InvalidOperationException("Invalid value type");
        }

        public IEnumerable<DnsZoneNode<TValue>> GetReverseEnumerable()
        {
            return _nodes.Values.Reverse();
        }

        IEnumerable<TValue> IBackend<TKey, TValue>.GetReverseEnumerable()
        {
            return _nodes.Values.Reverse().Cast<TValue>();
        }

        protected bool TryAdd(TKey key, DnsZoneNode<TValue> value)
        {
            if (value is null || !_nodes.TryAdd(key, value))
                return false;
            IndexReverseRecords(key, value);
            return true;
        }

        public bool TryAdd(TKey key, TValue? value)
        {
            return TryAdd(key, new DnsZoneNode<TValue>(value) ?? throw new InvalidOperationException("Invalid value type"));
        }

        public abstract bool TryGet(TKey key, out TValue value);

        public bool TryRemove(TKey key, out TValue? value)
        {
            value = null;
            DnsZoneNode<TValue>? nodeValue;
            if (TryRemove(key, out nodeValue))
            {
                value = nodeValue as TValue;
                return true;
            }
            else return false;
        }

        public bool TryUpdate(TKey key, DnsZoneNode<TValue> newValue, DnsZoneNode<TValue> comparisonValue)
        {
            if (_nodes.TryUpdate(key, newValue, comparisonValue))
            {
                IndexReverseRecords(key, newValue);
                return true;
            }
            return false;
        }

        public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
        {
            return TryUpdate(key,
                newValue as DnsZoneNode<TValue> ?? throw new InvalidOperationException("Invalid value type"),
                comparisonValue as DnsZoneNode<TValue> ?? throw new InvalidOperationException("Invalid value type"));
        }

        protected abstract void IndexReverseRecords(TKey name, DnsZoneNode<TValue> node);

        protected abstract void UnindexReverseRecords(TKey name, DnsZoneNode<TValue> node);

        private void AddOrUpdateNode(TKey key, DnsZoneNode<TValue> value)
        {
            if (_nodes.TryGetValue(key, out var existing))
            {
                // Merge records
                foreach (var record in value.RawRecords)
                    existing.AddRecord(record);
            }
            else
            {
                _nodes[key] = value;
                IndexReverseRecords(key, value);
            }
        }

        private DnsZoneNode<TValue> GetOrCreateNode(TKey name)
        {
            var n = name as TValue;
            return _nodes.GetOrAdd(name, _ => new DnsZoneNode<TValue>(n));
        }

        private bool TryRemove(TKey key, out DnsZoneNode<TValue>? value)
        {
            if (_nodes.TryRemove(key, out value))
            {
                UnindexReverseRecords(key, value);
                return true;
            }
            return false;
        }
    }
}