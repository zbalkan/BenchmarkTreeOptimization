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
                int before = Volatile.Read(ref _file.Header->WriteInProgress);

                uint cur = 1;
                bool ok = true;

                foreach (byte b in key)
                {
                    ref var n = ref _file.GetNode(cur);
                    fixed (uint* p = n.Children)
                    {
                        uint next = p[b];
                        if (next == 0)
                        {
                            ok = false;
                            break;
                        }
                        cur = next;
                    }
                }

                int after = Volatile.Read(ref _file.Header->WriteInProgress);

                if (IsWriteStable(before, after))
                {
                    index = cur;
                    return ok;
                }

                // Writer was active – retry
                Thread.SpinWait(4);
            }
        }

        public bool TryGetValue(uint nodeIndex, out ReadOnlySpan<byte> value)
        {
            while (true)
            {
                int before = Volatile.Read(ref _file.Header->WriteInProgress);

                ref var n = ref _file.GetNode(nodeIndex);

                if ((n.Flags & 1u) == 0)
                {
                    value = default;
                    return false;
                }

                var span = _file.GetValue(n.ValueOffset, n.ValueLength);

                int after = Volatile.Read(ref _file.Header->WriteInProgress);

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
