// MmapBackend.TrieNode.cs
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

namespace BenchmarkTreeBackends.Backends.MMAP
{
    public abstract unsafe partial class MmapBackend<TKey, TValue> where TValue : class
    {
        private sealed class TrieNode
        {
            // 256-way child table (ByteTree-faithful)
            private TrieNode?[]? _children;

            public byte[]? ValueBytes;

            private TrieNode() { }

            public static TrieNode CreateRoot() => new TrieNode();

            public int ChildrenCount
            {
                get
                {
                    if (_children is null)
                        return 0;

                    int count = 0;
                    for (int i = 0; i < 256; i++)
                    {
                        if (_children[i] is not null)
                            count++;
                    }
                    return count;
                }
            }

            public TrieNode GetOrCreateChild(byte label)
            {
                _children ??= new TrieNode?[256];

                var child = _children[label];
                if (child is null)
                {
                    child = new TrieNode();
                    _children[label] = child;
                }

                return child;
            }

            public bool TryGetChild(byte label, out TrieNode child)
            {
                if (_children is null)
                {
                    child = null!;
                    return false;
                }

                child = _children[label]!;
                return child is not null;
            }

            public void RemoveChild(byte label)
            {
                if (_children is null)
                    return;

                _children[label] = null;

                // Optional cleanup: release array if empty
                for (int i = 0; i < 256; i++)
                {
                    if (_children[i] is not null)
                        return;
                }

                _children = null;
            }

            public System.Collections.Generic.IEnumerable<(byte label, TrieNode node)> GetChildrenSorted()
            {
                if (_children is null)
                    yield break;

                // Already in ascending byte order: 0..255
                for (byte b = 0; b <= 255; b++)
                {
                    var c = _children[b];
                    if (c is not null)
                        yield return (b, c);
                }
            }

            public TrieNode Clone()
            {
                var n = new TrieNode();

                if (ValueBytes is not null)
                {
                    var copy = new byte[ValueBytes.Length];
                    Buffer.BlockCopy(ValueBytes, 0, copy, 0, copy.Length);
                    n.ValueBytes = copy;
                }

                if (_children is null)
                    return n;

                var newChildren = new TrieNode?[256];

                for (int i = 0; i < 256; i++)
                {
                    var c = _children[i];
                    if (c is not null)
                        newChildren[i] = c.Clone();
                }

                n._children = newChildren;
                return n;
            }
        }
    }
}
