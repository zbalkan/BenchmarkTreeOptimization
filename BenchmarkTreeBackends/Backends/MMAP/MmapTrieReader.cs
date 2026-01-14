using System;


namespace BenchmarkTreeBackends.Backends.MMAP
{
    internal unsafe sealed class MmapTrieReader
    {
        private readonly MmapFile _file;

        public MmapTrieReader(MmapFile file) => _file = file;

        public bool TryFindNode(ReadOnlySpan<byte> key, out uint index)
        {
            index = 1; // root

            foreach (byte b in key)
            {
                ref var n = ref _file.GetNode(index);
                fixed (uint* p = n.Children)
                {
                    uint next = p[b];
                    if (next == 0) return false;
                    index = next;
                }
            }
            return true;
        }

        public bool TryGetValue(uint nodeIndex, out ReadOnlySpan<byte> value)
        {
            ref var n = ref _file.GetNode(nodeIndex);
            if ((n.Flags & 1u) == 0)
            {
                value = default;
                return false;
            }

            value = _file.GetValue(n.ValueOffset, n.ValueLength);
            return true;
        }
    }
}