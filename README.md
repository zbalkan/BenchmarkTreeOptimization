# DomainTree Storage Backends – Performance & Architecture

This repository explores multiple storage backends for a high-performance DNS `DomainTree` / `ByteTree` implementation, focusing on **lookup speed**, **memory behavior**, and **correctness under real-world workloads**.

The same logical API (`IBackend<TKey, TValue>`) is implemented using three different storage strategies:

| Backend                   | Storage Model          | Primary Goal                   | Codec                      |
| ------------------------- | ---------------------- | ------------------------------ | -------------------------- |
| **ConcurrentDictionary**  | In-memory dictionary   | Baseline                       | None                       |
| **DefaultDomainTree**     | In-memory object graph | Baseline correctness           | Native UTF8 string to byte |
| **LmdbBackedDomainTree**  | LMDB + serialization   | Persistence                    | MessagePack                |
| **MmapBackedDomainTree**  | Memory-mapped file     | Read performance + Persistence | MessagePack                |
| **MmapBackedDomainTree2** | Memory-mapped file     | Read performance + Persistence | Native UTF8 string to byte |
| **DnsTrie**               | In-memory QP Trie      | DNS-optimized trie             | Internal                   |

---

## Benchmark Environment

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.7840/25H2/2025Update/HudsonValley2)
AMD Ryzen AI 5 PRO 340 w/ Radeon 840M 2.00GHz, 1 CPU, 12 logical and 6 physical cores
.NET SDK 9.0.311
  [Host]     : .NET 9.0.13 (9.0.13, 9.0.1326.6317), X64 RyuJIT x86-64-v4 [AttachedDebugger]
  DefaultJob : .NET 9.0.13 (9.0.13, 9.0.1326.6317), X64 RyuJIT x86-64-v4
```

Workload:
Large realistic domain trees with deep hierarchies and frequent lookups.

---

## Benchmark Results

| Method                | Mean        | Error     | StdDev    | Ratio | RatioSD | Gen0        | Allocated   | Alloc Ratio |
|---------------------- |------------:|----------:|----------:|------:|--------:|------------:|------------:|------------:|
| ConcurrentDictionary  |    97.97 ms |  1.946 ms |  4.736 ms |  1.00 |    0.07 |           - |           - |          NA |
| InMemoryDomainTree    |   868.52 ms |  4.411 ms |  3.910 ms |  8.88 |    0.41 |  53000.0000 | 446153824 B |          NA |
| LmdbBackedDomainTree  | 1,393.77 ms | 21.452 ms | 17.914 ms | 14.26 |    0.68 | 111000.0000 | 935384968 B |          NA |
| LmdbBackedDomainTree2 | 1,164.14 ms | 17.799 ms | 15.778 ms | 11.91 |    0.57 |  93000.0000 | 781538696 B |          NA |
| MmapBackedDomainTree  | 1,265.08 ms | 24.009 ms | 21.283 ms | 12.94 |    0.63 |  93000.0000 | 781538696 B |          NA |
| MmapBackedDomainTree2 |   813.77 ms | 14.079 ms | 17.291 ms |  8.32 |    0.42 |  75000.0000 | 627692424 B |          NA |
| DnsTrie               |   361.87 ms |  8.666 ms | 24.157 ms |  3.70 |    0.30 |           - |           - |          NA |
| DnsTrieWireFormat     |   545.00 ms |  6.730 ms |  5.620 ms |  5.57 |    0.26 |           - |           - |          NA |


### Key observations

* Disk-backed serialization is **significantly slower** and allocates more.
* MMAP achieves the **fastest lookup performance**.
* MMAP does **not increase GC pressure** compared to the in-memory version.
* Lookup performance is dominated by traversal, not deserialization.
* C# version of the DNS-optimized QP trie by Tony Finch <dot@dotat.at> seems to be faster, with the optimizations to minimize allocations, etc. 

---

## Architectural Models

### 1. DefaultDomainTree (In-Memory)

* Pure object graph
* Fully mutable
* Easy to reason about
* High allocation pressure
* Serves as correctness reference

### 2. LmdbBackedDomainTree (Serialized)

* Stores nodes on LMDB database as a KV-store setup where keys are reversed: com.google.www, etc.
* Requires per-lookup deserialization
* Heavy allocations
* Poor cache locality
* Useful mainly for persistence experiments

### 3. MmapBackedDomainTree (Immutable Snapshot)

* Data stored in a **memory-mapped file**
* Nodes and values accessed via raw memory
* No per-lookup allocations
* Excellent cache locality
* **Immutable** – all mutations use blue/green publishing

This backend is optimized for **read-heavy workloads** such as DNS resolution.

### 4. DnsTrie (In-Memory QP Trie)
* In-memory implementation of a DNS-optimized QP trie
* Uses a compact node structure with bit-packed labels
* Minimizes allocations by using value types and spans
* Optimized for DNS workloads with common suffixes and deep hierarchies
* Offers a different tradeoff between memory usage and lookup speed compared to the other backends
* Serves as a specialized implementation for DNS use cases, while the other backends are more general-purpose
* Created based on Tony Finch's C implementation, adapted to C# with optimizations for .NET's memory model and performance characteristics

---

## Blue/Green Publishing Model (MMAP)

MMAP never mutates the active file.

All changes happen in an in-memory **staging trie**:

1. Reads always use the **active immutable snapshot**
2. Writes go to the **staging tree**
3. `Swap()`:

   * Builds a new MMAP file
   * Atomically replaces the old file
   * Readers switch to the new snapshot
4. Old snapshot is disposed after readers finish

This guarantees:

* No partial updates
* No torn reads
* No corruption
* Safe concurrent readers

---

## Value Serialization

All values are encoded using either native UTF8 string converter or MessagePack implementing the interface:

```csharp
IValueCodec<TValue>
```

The codec is responsible for:

* Encoding `TValue → byte[]`
* Decoding `ReadOnlySpan<byte> → TValue`

MMAP never allocates new arrays during reads; decoding happens directly from mapped memory.

P.S: The `DnsTrie` backend uses an internal encoding optimized for DNS labels, so it does not rely on the `IValueCodec` interface.

---

## Functional Parity

All backends (except `ConcurrentDictionary` and `DnsTrie`) implement:

```csharp
IBackend<TKey, TValue>
```

Including:

* Add / TryAdd
* Remove / TryRemove
* Get / TryGet
* AddOrUpdate
* Enumeration
* IsEmpty

The functions are extracted from `ByteTree`.

---

## Project Status

Current focus:

* Correctness over micro-optimizations
* Stable MMAP file format
* Robust blue/green swapping
* Clean separation of read vs write paths
* Eliminating unsafe assumptions

Future work:

* Span-based key encoding
* SIMD-assisted label matching
* Parallel file builders
* Optional compression
* Multi-value nodes

---

## Summary

As expected, in-memory data is the winner. Our custom memory-mapped file-backed binary ByteTree has shown similar performance to LMDB. The serialization and memory allocations were the bottlenecks for resource usage.

The DNS-optimized QP trie shows promising performance, but further optimizations and comparisons are needed to fully understand its tradeoffs.

---

P.S: The initial version of this README is generated by AI based on BenchmarkDotnet reports and source code.
