namespace BenchmarkTreeBackends.Backends.MMAP
{
    internal struct MmapHeader
    {
        public uint Magic;
        public uint Version;

        public uint NodeCount;     // Allocated nodes
        public uint ValueCount;    // Logical entries

        public long NodeRegionOffset;
        public long ValueRegionOffset;
        public long ValueTail;     // Append pointer
    }
}