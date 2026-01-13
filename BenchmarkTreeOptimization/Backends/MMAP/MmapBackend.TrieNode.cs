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
using System.Collections.Generic;

namespace BenchmarkTreeOptimization.Backends.MMAP
{
public abstract unsafe partial class MmapBackend<TKey, TValue> where TValue : class
    {
        private sealed class TrieNode
        {
            private Dictionary<byte, TrieNode>? _children;

            public byte[]? ValueBytes;

            private TrieNode() { }

            public static TrieNode CreateRoot() => new TrieNode();

            public int ChildrenCount => _children?.Count ?? 0;

            public TrieNode GetOrCreateChild(byte label)
            {
                _children ??= new Dictionary<byte, TrieNode>(capacity: 4);
                if (!_children.TryGetValue(label, out var child))
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
                return _children.TryGetValue(label, out child!);
            }

            public void RemoveChild(byte label)
            {
                _children?.Remove(label);
                if (_children is { Count: 0 })
                    _children = null;
            }

            public List<(byte label, TrieNode node)> GetChildrenSorted()
            {
                if (_children is null || _children.Count == 0)
                    return new List<(byte, TrieNode)>(0);

                var list = new List<(byte, TrieNode)>(_children.Count);
                foreach (var kv in _children)
                    list.Add((kv.Key, kv.Value));

                list.Sort(static (a, b) => a.Item1.CompareTo(b.Item1));
                return list;
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

                if (_children is null || _children.Count == 0)
                    return n;

                n._children = new Dictionary<byte, TrieNode>(_children.Count);
                foreach (var kv in _children)
                    n._children[kv.Key] = kv.Value.Clone();

                return n;
            }
        }
    }
}
