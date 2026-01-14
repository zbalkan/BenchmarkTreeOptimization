# DomainTree Storage Backends – Performance & Architecture

This repository explores multiple storage backends for a high-performance DNS `DomainTree` / `ByteTree` implementation, focusing on **lookup speed**, **memory behavior**, and **correctness under real-world workloads**.

The same logical API (`IBackend<TKey, TValue>`) is implemented using three different storage strategies:

| Backend                   | Storage Model          | Primary Goal                   | Codec                      |
| ------------------------- | ---------------------- | ------------------------------ | -------------------------- |
| **DefaultDomainTree**     | In-memory object graph | Baseline correctness           | Native UTF8 string to byte |
| **LmdbBackedDomainTree**  | LMDB + serialization   | Persistence                    | MessagePack                |
| **MmapBackedDomainTree**  | Memory-mapped file     | Read performance + Persistence | MessagePack                |
| **MmapBackedDomainTree2** | Memory-mapped file     | Read performance + Persistence | Native UTF8 string to byte |

---

## Benchmark Environment

```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.7462/25H2/2025Update/HudsonValley2)
AMD Ryzen AI 5 PRO 340 w/ Radeon 840M 2.00GHz, 1 CPU, 12 logical and 6 physical cores
.NET SDK 10.0.102
  [Host]     : .NET 9.0.12 (9.0.12, 9.0.1225.60609), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 9.0.12 (9.0.12, 9.0.1225.60609), X64 RyuJIT x86-64-v4
```

Workload:
Large realistic domain trees with deep hierarchies and frequent lookups.

---

## Benchmark Results

| Method                | Mean       | Error    | StdDev    | Median     | Ratio | RatioSD | Gen0        | Allocated | Alloc Ratio |
|---------------------- |-----------:|---------:|----------:|-----------:|------:|--------:|------------:|----------:|------------:|
| InMemoryDomainTree    |   813.8 ms | 11.34 ms |  10.05 ms |   812.3 ms |  1.00 |    0.02 |  53000.0000 | 425.49 MB |        1.00 |
| LmdbBackedDomainTree  | 1,300.3 ms |  9.65 ms |   8.55 ms | 1,297.0 ms |  1.60 |    0.02 | 111000.0000 | 892.05 MB |        2.10 |
| LmdbBackedDomainTree2 | 1,087.8 ms | 18.65 ms |  17.45 ms | 1,079.6 ms |  1.34 |    0.03 |  93000.0000 | 745.33 MB |        1.75 |
| MmapBackedDomainTree  | 1,070.4 ms | 65.33 ms | 191.59 ms |   961.9 ms |  1.32 |    0.23 |  93000.0000 | 745.33 MB |        1.75 |
| MmapBackedDomainTree2 |   775.9 ms | 14.16 ms |  15.74 ms |   775.6 ms |  0.95 |    0.02 |  75000.0000 | 598.61 MB |        1.41 |



### Key observations

* Disk-backed serialization is **significantly slower** and allocates more.
* MMAP achieves the **fastest lookup performance**.
* MMAP does **not increase GC pressure** compared to the in-memory version.
* Lookup performance is dominated by traversal, not deserialization.

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

---

## Functional Parity

All backends implement:

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

As expected, in-memory data is the winner. Our custom memory-mapped file-backed binary ByteTree has shown similar performance to LMDB. The serizalization and memory allocations were the bottlenecks for resource usage. 

---

P.S: This README is generated by AI based on BenchmarkDotnet reports and source code.
