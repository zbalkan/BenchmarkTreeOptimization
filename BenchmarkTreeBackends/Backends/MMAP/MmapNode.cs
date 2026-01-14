namespace BenchmarkTreeBackends.Backends.MMAP
{
    internal unsafe struct MmapNode
    {
        public uint Flags;         // bit0 = HasValue
        public long ValueOffset;
        public int ValueLength;

        public fixed uint Children[256];
    }
}