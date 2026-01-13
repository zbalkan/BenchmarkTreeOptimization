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
using System.Threading;

namespace BenchmarkTreeOptimization
{
    public sealed class NodeValue<T> where T : class
    {
        #region variables

        private readonly byte[] _key;
        private T _value;

        #endregion variables

        #region constructor

        public NodeValue(byte[] key, T value)
        {
            _key = key;
            _value = value;
        }

        #endregion constructor

        #region public

        public bool TryUpdateValue(T newValue, T comparisonValue)
        {
            T originalValue = Interlocked.CompareExchange(ref _value, newValue, comparisonValue);
            return ReferenceEquals(originalValue, comparisonValue);
        }

        public override string ToString()
        {
            return Convert.ToHexString(_key).ToLower() + ": " + _value.ToString();
        }

        #endregion public

        #region properties

        public byte[] Key
        { get { return _key; } }

        public T Value
        {
            get { return _value; }
            set { _value = value; }
        }

        #endregion properties
    }
}