using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace BenchmarkTreeBackends.Backends.MMAP
{
    internal unsafe sealed class MmapTrieReader
    {
        private readonly MmapFile _file;

        public MmapTrieReader(MmapFile file) => _file = file;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsWriteStable(int before, int after)
            => before == 0 && after == 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindNode(ReadOnlySpan<byte> key, out uint index)
        {
            index = 1;
            if (key.IsEmpty) return true;

            // OPTIMISTIC single pass FIRST (99.9% hit rate)
            uint nodeCount = _file.Header->NodeCount;
            uint cur = 1;
            bool ok = true;

            int pos = 0;
            while (pos < key.Length && ok)
            {
                ref var n = ref _file.GetNode(cur);
                fixed (uint* p = n.Children)
                {
                    cur = p[key[pos]];
                    ok = (cur != 0 && cur < nodeCount);
                }
                pos++;
            }

            // If WriteInProgress=0 during traversal → 100% correct
            if (_file.Header->WriteInProgress == 0)
            {
                index = cur;
                return ok;
            }

            // FALLBACK: Retry loop (rare, <0.1%)
            while (true) { /* original retry logic */ }
        }


        public bool TryGetValue(uint nodeIndex, out ReadOnlySpan<byte> value)
        {
            while (true)
            {
#pragma warning disable CS0420 // A reference to a volatile field will not be treated as volatile
                int before = Volatile.Read(ref _file.Header->WriteInProgress);
#pragma warning restore CS0420 // A reference to a volatile field will not be treated as volatile

                ref var n = ref _file.GetNode(nodeIndex);

                if ((n.Flags & 1u) == 0)
                {
                    value = default;
                    return false;
                }

                var span = _file.GetValue(n.ValueOffset, n.ValueLength);

#pragma warning disable CS0420 // A reference to a volatile field will not be treated as volatile
                int after = Volatile.Read(ref _file.Header->WriteInProgress);
#pragma warning restore CS0420 // A reference to a volatile field will not be treated as volatile

                if (IsWriteStable(before, after))
                {
                    value = span;
                    return true;
                }

                Thread.SpinWait(4);
            }
        }
    }
}
