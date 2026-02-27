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
  [Host]     : .NET 9.0.13 (9.0.13, 9.0.1326.6317), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 9.0.13 (9.0.13, 9.0.1326.6317), X64 RyuJIT x86-64-v4
```

Workload:
Large realistic domain trees with deep hierarchies and frequent lookups.

---

## Benchmark Results

| Method                | Mean       | Error    | StdDev    | Median     | Ratio | RatioSD | Gen0        | Allocated   | Alloc Ratio |
|---------------------- |-----------:|---------:|----------:|-----------:|------:|--------:|------------:|------------:|------------:|
| ConcurrentDictionary  |   157.6 ms | 11.69 ms |  34.48 ms |   160.0 ms |  1.06 |    0.36 |           - |           - |          NA |
| InMemoryDomainTree    |   645.8 ms | 14.46 ms |  42.64 ms |   627.5 ms |  4.33 |    1.14 |  53000.0000 | 446153824 B |          NA |
| LmdbBackedDomainTree  | 1,065.5 ms | 13.49 ms |  11.96 ms | 1,066.2 ms |  7.14 |    1.81 | 111000.0000 | 935384968 B |          NA |
| LmdbBackedDomainTree2 | 1,603.2 ms | 75.31 ms | 212.41 ms | 1,658.1 ms | 10.75 |    3.09 |  93000.0000 | 781538696 B |          NA |
| MmapBackedDomainTree  | 1,335.6 ms | 53.60 ms | 151.18 ms | 1,277.5 ms |  8.96 |    2.50 |  93000.0000 | 781538696 B |          NA |
| MmapBackedDomainTree2 | 1,163.0 ms | 47.30 ms | 139.47 ms | 1,119.2 ms |  7.80 |    2.20 |  75000.0000 | 627692424 B |          NA |
| DnsTrie               |   348.8 ms |  3.70 ms |   3.80 ms |   348.5 ms |  2.34 |    0.59 |           - |           - |          NA |
| DnsTrieWireFormat     |   774.8 ms | 33.14 ms |  94.01 ms |   724.1 ms |  5.20 |    1.47 |           - |           - |          NA |

### Key observations

* Disk-backed serialization is **significantly slower** and allocates more.
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

The `DnsTrie`, as the C# reimplementation of the QP-trie, uses managed allocations to improve latency and throughput. Reducing GC allocation rate improves latency stability (fewer and shorter pauses) and CPU efficiency (less time in the allocator). It does not reduce the working set — the total RAM consumed by the live trie. The current C# implementation uses roughly 10–15× more memory per stored entry than BIND's chunk-based layout. That gap is structural: it follows from the CLR object model, the three-object-per-branch CAS design, and the dual key storage per leaf. Closing it requires architectural changes to the concurrency model or the API contract, not changes to the hot-path allocation strategy.

---

P.S: The initial version of this README is generated by AI based on BenchmarkDotnet reports and source code.
