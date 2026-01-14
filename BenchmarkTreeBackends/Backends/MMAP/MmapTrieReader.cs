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
            index = 1; // root
            if (key.IsEmpty) return true;

            while (true)
            {
#pragma warning disable CS0420 // A reference to a volatile field will not be treated as volatile
                int before = Volatile.Read(ref _file.Header->WriteInProgress);
#pragma warning restore CS0420 // A reference to a volatile field will not be treated as volatile

                uint nodeCount = _file.Header->NodeCount;  // SINGLE volatile read
                uint cur = 1;
                bool ok = true;

                // UNROLLED: First 4 bytes (covers 80% short domains)
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

#pragma warning disable CS0420 // A reference to a volatile field will not be treated as volatile
                int after = Volatile.Read(ref _file.Header->WriteInProgress);
#pragma warning restore CS0420 // A reference to a volatile field will not be treated as volatile
                if (IsWriteStable(before, after) && ok)
                {
                    index = cur;
                    return true;
                }

                Thread.SpinWait(4);
            }
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
