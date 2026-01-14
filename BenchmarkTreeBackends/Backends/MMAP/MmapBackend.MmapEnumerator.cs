// MmapBackend.MmapEnumerator.cs
//
// High-performance, memory-mapped backend for IBackend<TKey,TValue> with BLUE/GREEN publishing.
//
// Key rules:
// - Reads are always served from an immutable, memory-mapped “active” snapshot.
// - All mutations go only to an in-memory “staging” trie.
// - Mutations DO NOT affect reads until you call Swap().
// - Swap() builds a brand-new file (temp + replace), then atomically switches “active” to the new mapping.
// - If the file path does not exist, a new empty mmap file is created.
//
// File format (v1):
// [MmapHeader][MmapNode array][Value blob region]
//
// Value storage:
// - Node stores (ValueOffset, ValueLength).
// - ValueOffset is relative to header.ValueRegionOffset and points to a 4-byte length prefix.
// - Blob entry layout: [int32 length][byte[length] payload]
// - ValueLength is the payload length (excluding the 4-byte prefix).
//
// Build requirements: <AllowUnsafeBlocks>true</AllowUnsafeBlocks>
//
// Concurrency model:
// - Reads are lock-free w.r.t swap using ref-counted snapshot leases so Swap can safely retire old mappings.
// - Staging mutations are protected by a private lock and never touch the active mapping.
//
// Safety model:
// - Bounds checks remain enabled by default; define MMAP_UNSAFE_FAST only for trusted files.

using System;
using System.Collections;
using System.Collections.Generic;

namespace BenchmarkTreeBackends.Backends.MMAP
{
    public abstract unsafe partial class MmapBackend<TKey, TValue> where TValue : class
    {
        // -----------------------------
        // Enumerator over active mmap snapshot
        // -----------------------------

        private sealed class MmapEnumerator : IEnumerator<TValue>
        {
            private readonly MmapBackend<TKey, TValue> _owner;
            private ActiveLease _lease;
            private readonly bool _reverse;

            private uint[] _stack;
            private int _sp;
            private bool _started;
            private TValue? _current;

            public MmapEnumerator(MmapBackend<TKey, TValue> owner, ActiveLease lease, bool reverse)
            {
                _owner = owner;
                _lease = lease;
                _reverse = reverse;
                _stack = new uint[256];
                _sp = 0;
            }

            public TValue Current => _current ?? throw new InvalidOperationException();
            object IEnumerator.Current => Current;

            public bool MoveNext()
            {
                ObjectDisposedException.ThrowIf(_owner._disposed, _owner);

                if (!_started)
                {
                    _started = true;
                    Push(1); // root index
                }

                while (_sp > 0)
                {
                    uint idx = Pop();
                    ref readonly var node = ref _lease.State.GetNodeAtIndex(idx);

                    // Children traversal:
                    // Forward: visit 0..255 => push reverse so stack pops ascending.
                    // Reverse: visit 255..0 => push ascending so stack pops descending.
                    unsafe
                    {
                        fixed (MmapNode* pn = &node)
                        {
                            uint* p = pn->Children;

                            if (_reverse)
                            {
                                for (int b = 0; b <= 255; b++)
                                {
                                    uint c = p[b];
                                    if (c != 0) Push(c);
                                }
                            }
                            else
                            {
                                for (int b = 255; b >= 0; b--)
                                {
                                    uint c = p[b];
                                    if (c != 0) Push(c);
                                }
                            }
                        }
                    }

                    if (_lease.State.TryReadValueBytes(node, out var payload))
                    {
                        _current = _owner._codec.Decode(payload);
                        return true;
                    }
                }

                _current = null;
                return false;
            }

            public void Reset()
            {
                _sp = 0;
                _started = false;
                _current = null;
            }

            public void Dispose() => _lease.Dispose();

            private void Push(uint pos)
            {
                if (_sp == _stack.Length)
                {
                    var n = new uint[_stack.Length * 2];
                    Array.Copy(_stack, n, _stack.Length);
                    _stack = n;
                }
                _stack[_sp++] = pos;
            }

            private uint Pop() => _stack[--_sp];
        }
    }
}