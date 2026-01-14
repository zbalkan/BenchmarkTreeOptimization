using System;


namespace BenchmarkTreeBackends.Backends.MMAP
{
    internal unsafe sealed class MmapTrieWriter
    {
        private readonly MmapFile _file;

        public MmapTrieWriter(MmapFile file) => _file = file;

        public bool InsertOrUpdate(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool overwrite)
        {
            uint index = 1;

            foreach (byte b in key)
            {
                ref var n = ref _file.GetNode(index);
                fixed (uint* p = n.Children)
                {
                    if (p[b] == 0)
                    {
                        uint newIdx = _file.Header->NodeCount++;
                        p[b] = newIdx;
                    }
                    index = p[b];
                }
            }

            ref var node = ref _file.GetNode(index);

            if (!overwrite && (node.Flags & 1u) != 0)
                return false;

            long off = _file.Header->ValueTail;
            _file.Header->ValueTail += 4 + value.Length;

            *(int*)(_file.BasePtr + _file.Header->ValueRegionOffset + off) = value.Length;
            value.CopyTo(new Span<byte>(
                _file.BasePtr + _file.Header->ValueRegionOffset + off + 4,
                value.Length));

            if ((node.Flags & 1u) == 0)
                _file.Header->ValueCount++;

            node.ValueOffset = off;
            node.ValueLength = value.Length;
            node.Flags |= 1u;

            return true;
        }

        public bool TryRemove(ReadOnlySpan<byte> key)
        {
            uint index = 1;

            foreach (byte b in key)
            {
                ref var n = ref _file.GetNode(index);
                fixed (uint* p = n.Children)
                {
                    if (p[b] == 0) return false;
                    index = p[b];
                }
            }

            ref var node = ref _file.GetNode(index);
            if ((node.Flags & 1u) == 0) return false;

            node.Flags &= ~1u;
            _file.Header->ValueCount--;
            return true;
        }
    }
}