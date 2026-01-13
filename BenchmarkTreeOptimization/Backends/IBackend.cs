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
using System.Collections.Generic;

namespace BenchmarkTreeOptimization.Backends
{
    public interface IBackend<TKey, TValue> : IEnumerable<TValue> where TValue : class
    {
        TValue this[TKey? key] { get; set; }

        bool IsEmpty { get; }

        void Add(TKey key, TValue value);
        TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory);
        TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory);
        void Clear();
        bool ContainsKey(TKey key);
        byte[]? ConvertToByteKey(TKey key, bool throwException = true);
        TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory);
        TValue GetOrAdd(TKey key, TValue value);
        IEnumerable<TValue> GetReverseEnumerable();
        bool TryAdd(TKey key, TValue? value);
        bool TryGet(TKey key, out TValue value);
        bool TryRemove(TKey key, out TValue? value);
        bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue);
    }
}