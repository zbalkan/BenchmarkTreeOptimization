using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace BenchmarkTreeBackends.Backends.MMAP
{
    internal unsafe sealed class MmapTrieWriter
    {
        private readonly MmapFile _file;

        public MmapTrieWriter(MmapFile file) => _file = file;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BeginWrite()
        {
            // Publish "writer active" BEFORE touching shared trie/value state.
            Volatile.Write(ref _file.Header->WriteInProgress, 1);
            Thread.MemoryBarrier();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EndWrite()
        {
            // Ensure all trie/value writes are globally visible BEFORE clearing the flag.
            Thread.MemoryBarrier();
            Volatile.Write(ref _file.Header->WriteInProgress, 0);
        }

        public bool InsertOrUpdate(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool overwrite)
        {
            BeginWrite();
            try
            {
                // CRITICAL: Calculate max nodes from layout
                long maxNodes = (_file.Header->ValueRegionOffset - _file.Header->NodeRegionOffset) / sizeof(MmapNode);

                uint index = 1; // root
                foreach (byte b in key)
                {
                    ref var n = ref _file.GetNode(index);
                    fixed (uint* p = n.Children)
                    {
                        uint next = p[b];
                        if (next == 0)
                        {
                            // FIXED: Capacity check BEFORE increment
                            if (_file.Header->NodeCount >= (uint)maxNodes)
                                throw new InvalidOperationException($"Node capacity exceeded: {_file.Header->NodeCount}/{maxNodes}");

                            uint newIdx = _file.Header->NodeCount++;
                            p[b] = newIdx;
                            next = newIdx;
                        }
                        index = next;
                    }
                }

                ref var node = ref _file.GetNode(index);

                bool hadValue = (node.Flags & 1u) != 0;
                if (!overwrite && hadValue)
                    return false;

                // Append value blob: [int32 length][payload]
                long off = _file.Header->ValueTail;
                _file.Header->ValueTail = off + 4L + value.Length;

                *(int*)(_file.BasePtr + _file.Header->ValueRegionOffset + off) = value.Length;
                value.CopyTo(new Span<byte>(
                    _file.BasePtr + _file.Header->ValueRegionOffset + off + 4,
                    value.Length));

                if (!hadValue)
                    _file.Header->ValueCount++;

                node.ValueOffset = off;
                node.ValueLength = value.Length;
                node.Flags |= 1u;

                return true;
            }
            finally
            {
                EndWrite();
            }
        }

        public bool TryRemove(ReadOnlySpan<byte> key)
        {
            BeginWrite();
            try
            {
                uint index = 1;

                foreach (byte b in key)
                {
                    ref var n = ref _file.GetNode(index);
                    fixed (uint* p = n.Children)
                    {
                        uint next = p[b];
                        if (next == 0)
                            return false;

                        index = next;
                    }
                }

                ref var node = ref _file.GetNode(index);
                if ((node.Flags & 1u) == 0)
                    return false;

                // Soft delete: keep offsets (no reclaim); just clear HasValue and decrement count.
                node.Flags &= ~1u;
                _file.Header->ValueCount--;

                return true;
            }
            finally
            {
                EndWrite();
            }
        }
    }
}
