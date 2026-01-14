using System;
using System.Collections;
using System.Collections.Generic;


namespace BenchmarkTreeBackends.Backends.MMAP
{
    public abstract unsafe partial class MmapBackend<TKey, TValue> where TValue : class
    {
        private struct DfsEnumerator : IEnumerator<TValue>
        {
            private readonly MmapBackend<TKey, TValue> _owner;
            private readonly bool _reverse;
            private Stack<uint> _stack;
            private TValue? _current;

            public DfsEnumerator(MmapBackend<TKey, TValue> owner, bool reverse)
            {
                _owner = owner;
                _reverse = reverse;
                _stack = new Stack<uint>(capacity: 64);
                _stack.Push(1); // root
                _current = default;
            }

            public TValue Current => _current!;
            object IEnumerator.Current => Current;

            public bool MoveNext()
            {
                while (_stack.Count > 0)
                {
                    uint idx = _stack.Pop();
                    ref var n = ref _owner._file.GetNode(idx);

                    // Push children so pop order matches requested traversal.
                    unsafe
                    {
                        fixed (uint* p = n.Children)
                        {
                            if (_reverse)
                            {
                                // Reverse traversal: visit 255 -> 0
                                // Push 0..255 so stack pops 255..0
                                for (int i = 0; i < 256; i++)
                                {
                                    uint c = p[i];
                                    if (c != 0) _stack.Push(c);
                                }
                            }
                            else
                            {
                                // Forward traversal: visit 0 -> 255
                                // Push 255..0 so stack pops 0..255
                                for (int i = 255; i >= 0; i--)
                                {
                                    uint c = p[i];
                                    if (c != 0) _stack.Push(c);
                                }
                            }
                        }
                    }

                    if ((n.Flags & 1u) != 0 && _owner._reader.TryGetValue(idx, out var payload))
                    {
                        _current = _owner._codec.Decode(payload);
                        return true;
                    }
                }

                _current = default;
                return false;
            }

            public void Reset() => throw new NotSupportedException();
            public void Dispose() { /* nothing */ }
        }
    }
}