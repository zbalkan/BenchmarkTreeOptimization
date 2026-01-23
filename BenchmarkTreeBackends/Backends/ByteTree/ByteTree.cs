/*
Technitium DNS Server
Copyright (C) 2025  Shreyas Zare (shreyas@technitium.com)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

*/

using System;
using System.Collections;
using System.Collections.Generic;

namespace BenchmarkTreeBackends.Backends.ByteTree
{
    public abstract partial class ByteTree<TKey, TValue> : IBackend<TKey, TValue> where TValue : class
    {
        #region variables

        protected readonly int _keySpace;
        protected readonly Node<TValue> _root;

        #endregion variables

        #region constructor

        protected ByteTree(int keySpace)
        {
            if (keySpace < 0 || keySpace > 256)
                throw new ArgumentOutOfRangeException(nameof(keySpace));

            _keySpace = keySpace;
            _root = new Node<TValue>(null, 0, _keySpace, null);
        }

        #endregion constructor

        #region protected

        public abstract byte[]? ConvertToByteKey(TKey key, bool throwException = true);

        internal bool TryRemove(TKey key, out TValue? value, out Node<TValue>? currentNode)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            byte[]? bKey = ConvertToByteKey(key, false);
            if (bKey is null)
            {
                value = default;
                currentNode = default;
                return false;
            }

            NodeValue<TValue>? removedValue = _root.RemoveNodeValue(bKey, out currentNode);
            if (removedValue is null)
            {
                value = default;
                return false;
            }

            //by default TryRemove wont call currentNode.CleanThisBranch() so that operations are atomic but will use up memory since stem nodes wont be cleaned up
            //override the public method if the implementation requires to save memory and take a chance of remove operation deleting an added NodeValue<TValue> due to race condition

            value = removedValue.Value;
            return true;
        }

        internal bool TryGet(TKey? key, out TValue? value, out Node<TValue>? currentNode)
        {
            if (key is null) throw new ArgumentNullException(nameof(key));

            byte[]? bKey = ConvertToByteKey(key, false);
            if (bKey is null)
            {
                value = default;
                currentNode = default;
                return false;
            }

            NodeValue<TValue>? nodeValue = _root.FindNodeValue(bKey, out currentNode);
            if (nodeValue is null)
            {
                value = default;
                return false;
            }

            value = nodeValue.Value;
            return true;
        }

        #endregion protected

        #region public

        public void Clear()
        {
            _root.ClearNode();
        }

        public void Add(TKey key, TValue value)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            byte[]? bKey = ConvertToByteKey(key);

            if (!_root.AddNodeValue(bKey, delegate () { return new NodeValue<TValue>(bKey!, value); }, _keySpace, out _, out _))
                throw new ArgumentException("Key already exists.");
        }

        public bool TryAdd(TKey key, TValue? value)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            byte[]? bKey = ConvertToByteKey(key, false);
            if (bKey is null)
            {
                value = default;
                return false;
            }

            return _root.AddNodeValue(bKey, delegate () { return new NodeValue<TValue>(bKey, value); }, _keySpace, out _, out _);
        }

        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            byte[] bKey = ConvertToByteKey(key);

            if (_root.AddNodeValue(bKey, delegate () { return new NodeValue<TValue>(bKey, addValueFactory(key)); }, _keySpace, out NodeValue<TValue> addedValue, out NodeValue<TValue> existingValue))
                return addedValue.Value;

            TValue updateValue = updateValueFactory(key, existingValue.Value);
            existingValue.Value = updateValue;
            return updateValue;
        }

        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            return AddOrUpdate(key, delegate (TKey k) { return addValue; }, updateValueFactory);
        }

        public bool ContainsKey(TKey key)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            byte[] bKey = ConvertToByteKey(key, false);
            if (bKey is null)
                return false;

            return _root.FindNodeValue(bKey, out _) is not null;
        }

        public bool TryGet(TKey key, out TValue value)
        {
            return TryGet(key, out value, out _);
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            byte[]? bKey = ConvertToByteKey(key);

            if (_root.AddNodeValue(bKey, delegate () { return new NodeValue<TValue>(bKey, valueFactory(key)); }, _keySpace, out NodeValue<TValue> addedValue, out NodeValue<TValue> existingValue))
                return addedValue.Value;

            return existingValue.Value;
        }

        public TValue GetOrAdd(TKey key, TValue value)
        {
            return GetOrAdd(key, delegate (TKey k) { return value; });
        }

        public virtual bool TryRemove(TKey key, out TValue? value)
        {
            return TryRemove(key, out value, out _);
        }

        public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            byte[] bKey = ConvertToByteKey(key, false);
            if (bKey is null)
                return false;

            NodeValue<TValue> nodeValue = _root.FindNodeValue(bKey, out _);
            if (nodeValue is null)
                return false;

            return nodeValue.TryUpdateValue(newValue, comparisonValue);
        }

        public IEnumerator<TValue> GetEnumerator()
        {
            return new ByteTreeEnumerator(_root, false);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new ByteTreeEnumerator(_root, false);
        }

        public IEnumerable<TValue> GetReverseEnumerable()
        {
            return new ByteTreeReverseEnumerable(_root);
        }

        #endregion public

        #region properties

        public bool IsEmpty
        { get { return _root.IsEmpty; } }

        public TValue this[TKey? key]
        {
            get
            {
                if (key is null)
                    throw new ArgumentNullException(nameof(key));

                byte[]? bKey = ConvertToByteKey(key);

                NodeValue<TValue>? nodeValue = _root.FindNodeValue(bKey, out _);
                return nodeValue is null ? throw new KeyNotFoundException() : nodeValue.Value;
            }
            set
            {
                AddOrUpdate(key!, delegate (TKey k) { return value; }, delegate (TKey k, TValue v) { return value; });
            }
        }

        #endregion properties

        private sealed class ByteTreeReverseEnumerable : IEnumerable<TValue>
        {
            #region variables

            private readonly Node<TValue> _root;

            #endregion variables

            #region constructor

            public ByteTreeReverseEnumerable(Node<TValue> root)
            {
                _root = root;
            }

            #endregion constructor

            #region public

            public IEnumerator<TValue> GetEnumerator()
            {
                return new ByteTreeEnumerator(_root, true);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new ByteTreeEnumerator(_root, true);
            }

            #endregion public
        }

        protected sealed class ByteTreeEnumerator : IEnumerator<TValue>
        {
            #region variables

            private readonly Node<TValue> _root;
            private readonly bool _reverse;

            private Node<TValue>? _current;
            private NodeValue<TValue>? _value;
            private bool _finished;

            #endregion variables

            #region constructor

            internal ByteTreeEnumerator(Node<TValue> root, bool reverse)
            {
                _root = root;
                _reverse = reverse;
            }

            #endregion constructor

            #region public

            public void Dispose()
            {
                //do nothing
            }

            public TValue Current
            {
                get
                {
                    if (_value is null)
                        return default;

                    return _value.Value;
                }
            }

            object? IEnumerator.Current
            {
                get
                {
                    if (_value is null)
                        return default;

                    return _value.Value;
                }
            }

            public void Reset()
            {
                _current = null;
                _value = null;
                _finished = false;
            }

            public bool MoveNext()
            {
                if (_finished)
                    return false;

                if (_current is null)
                {
                    if (_reverse)
                    {
                        _current = _root.GetLastNodeWithValue();
                        if (_current is null)
                        {
                            //tree has no data
                            _value = null;
                            _finished = true;
                            return false;
                        }
                    }
                    else
                    {
                        _current = _root;
                    }

                    NodeValue<TValue> value = _current.Value;
                    if (value is not null)
                    {
                        _value = value;
                        return true;
                    }
                }

                do
                {
                    if (_reverse)
                        _current = _current.GetPreviousNodeWithValue(_root.Depth);
                    else
                        _current = _current.GetNextNodeWithValue(_root.Depth);

                    if (_current is null)
                    {
                        _value = null;
                        _finished = true;
                        return false;
                    }

                    NodeValue<TValue> value = _current.Value;
                    if (value is not null)
                    {
                        _value = value;
                        return true;
                    }
                }
                while (true);
            }

            #endregion public
        }
    }
}