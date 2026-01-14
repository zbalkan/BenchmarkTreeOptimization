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

        public bool TryFindNode(ReadOnlySpan<byte> key, out uint index)
        {
            index = 1; // root

            while (true)
            {
#pragma warning disable CS0420 // A reference to a volatile field will not be treated as volatile
                int before = Volatile.Read(ref _file.Header->WriteInProgress);
#pragma warning restore CS0420 // A reference to a volatile field will not be treated as volatile

                uint cur = 1;
                bool ok = true;

                foreach (byte b in key)
                {
                    ref var n = ref _file.GetNode(cur);
                    fixed (uint* p = n.Children)
                    {
                        uint next = p[b];
                        if (next == 0) { ok = false; break; }

                        cur = next;

                        // CRITICAL: Validate node still exists post-traversal
                        if (cur >= _file.Header->NodeCount)
                        {
                            ok = false;
                            break;
                        }
                    }
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
