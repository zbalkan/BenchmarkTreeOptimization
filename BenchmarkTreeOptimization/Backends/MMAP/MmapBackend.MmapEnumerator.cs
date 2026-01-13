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

namespace BenchmarkTreeOptimization.Backends.MMAP
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

            private long[] _stack;
            private int _sp;
            private bool _started;
            private TValue? _current;

            public MmapEnumerator(MmapBackend<TKey, TValue> owner, ActiveLease lease, bool reverse)
            {
                _owner = owner;
                _lease = lease;
                _reverse = reverse;
                _stack = new long[256];
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
                    Push(_lease.State.RootPos);
                }

                while (_sp > 0)
                {
                    long pos = Pop();
                    ref readonly var node = ref _lease.State.GetNodeAt(pos);

                    // DFS stack ordering:
                    // - Forward: children are stored sorted ascending; push in reverse so visit ascending.
                    // - Reverse: push in ascending so visit descending.
                    if (node.ChildCount != 0 && node.FirstChildPos != 0)
                    {
                        if (_reverse)
                        {
                            for (long i = 0; i < node.ChildCount; i++)
                                Push(node.FirstChildPos + i * sizeof(MmapNode));
                        }
                        else
                        {
                            for (long i = (long)node.ChildCount - 1; i >= 0; i--)
                                Push(node.FirstChildPos + i * sizeof(MmapNode));
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

            private void Push(long pos)
            {
                if (_sp == _stack.Length)
                {
                    var n = new long[_stack.Length * 2];
                    Array.Copy(_stack, n, _stack.Length);
                    _stack = n;
                }
                _stack[_sp++] = pos;
            }

            private long Pop() => _stack[--_sp];
        }
    }
}
