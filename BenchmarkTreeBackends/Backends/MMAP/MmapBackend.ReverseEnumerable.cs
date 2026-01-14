using System.Collections;
using System.Collections.Generic;


namespace BenchmarkTreeBackends.Backends.MMAP
{
    public abstract unsafe partial class MmapBackend<TKey, TValue> where TValue : class
    {
        private sealed class ReverseEnumerable : IEnumerable<TValue>
        {
            private readonly MmapBackend<TKey, TValue> _owner;
            public ReverseEnumerable(MmapBackend<TKey, TValue> owner) => _owner = owner;

            public IEnumerator<TValue> GetEnumerator() => new DfsEnumerator(_owner, reverse: true);
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
    }
}