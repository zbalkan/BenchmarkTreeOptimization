// MmapNode.cs
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

using System.Runtime.InteropServices;

namespace BenchmarkTreeOptimization.Backends.MMAP
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct MmapNode
    {
        public uint LabelId;        // for this trie, we use the byte itself as LabelId (0..255); root uses 0
        public long FirstChildPos;  // file offset (bytes) to first child node in node array; 0 if none
        public uint ChildCount;     // number of children

        public long ValueOffset;    // relative to ValueRegionOffset; 0 = no value
        public int ValueLength;     // payload length in bytes (excluding the 4-byte length prefix)
    }
}
