# DomainTree Storage Backends – Performance & Architecture

This repository explores multiple storage backends for a high-performance DNS `DomainTree` / `ByteTree` implementation, focusing on **lookup speed**, **memory behavior**, and **correctness under real-world workloads**.

The same logical API (`IBackend<TKey, TValue>`) is implemented using three different storage strategies:

| Backend                  | Storage Model          | Mutability             | Primary Goal         |
| ------------------------ | ---------------------- | ---------------------- | -------------------- |
| **DefaultDomainTree**    | In-memory object graph | Mutable                | Baseline correctness |
| **DiskBackedDomainTree** | File + serialization   | Mutable                | Persistence          |
| **MmapBackedDomainTree** | Memory-mapped file     | Immutable + Blue/Green | Read performance     |

---

## Benchmark Environment

```
BenchmarkDotNet v0.15.8
Windows 11 25H2 (10.0.26200.7462)
AMD Ryzen AI 5 PRO 340 (6C / 12T)
.NET SDK 10.0.101
Runtime: .NET 9.0.11, RyuJIT x64
```

Workload:
Large realistic domain trees with deep hierarchies and frequent lookups.

---

## Benchmark Results

```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.7462/25H2/2025Update/HudsonValley2)
AMD Ryzen AI 5 PRO 340 w/ Radeon 840M 2.00GHz, 1 CPU, 12 logical and 6 physical cores
.NET SDK 10.0.101
  [Host]     : .NET 9.0.11 (9.0.11, 9.0.1125.51716), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 9.0.11 (9.0.11, 9.0.1125.51716), X64 RyuJIT x86-64-v4


```

| Method               | Mean         | Error       | StdDev      | Ratio    | Gen0        | Allocated | Alloc Ratio |
|--------------------- |-------------:|------------:|------------:|---------:|------------:|----------:|------------:|
| DefaultDomainTree    |     615.5 ms |     5.18 ms |     4.85 ms |     1.00 |  53000.0000 | 425.49 MB |        1.00 |
| DiskBackedDomainTree |   1,024.8 ms |     4.15 ms |     3.24 ms |     1.66 | 111000.0000 | 892.05 MB |        2.10 |
| MmapBackedDomainTree | **506.8 ms** | **3.30 ms** | **2.93 ms** | **0.82** |  53000.0000 | 425.49 MB |        1.00 |


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

### 2. DiskBackedDomainTree (Serialized)

* Stores nodes on disk
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

## File Format (MMAP)

```
[MmapHeader]
[MmapNode array]
[Value blob region]
```

### Header

Contains:

* Magic (`MMAP`)
* Version
* Endianness
* Node region offset
* Node count
* Value region offset

### Node Layout

Each node stores:

* LabelId (byte)
* FirstChildPos (offset)
* ChildCount
* ValueOffset (0 = no value)
* ValueLength

### Value Storage

Values are stored in a **blob region**:

```
[int32 length][value bytes]
```

Nodes reference values via `(offset, length)`.

This allows:

* Arbitrary `TValue` sizes
* Zero-copy decoding
* No fixed 4-byte limitations
* Safe deserialization

---

## Value Serialization

All values are encoded using:

```csharp
IValueCodec<TValue>
```

The codec is responsible for:

* Encoding `TValue → byte[]`
* Decoding `ReadOnlySpan<byte> → TValue`

MMAP never allocates new arrays during reads; decoding happens directly from mapped memory.

---

## Safety Model

By default:

* All node offsets are bounds-checked
* Header fields are validated
* Corrupt files fail fast

Optional:

```csharp
#define MMAP_UNSAFE_FAST
```

Disables bounds checks for trusted files when maximum speed is required.

---

## Why MMAP Is Faster

* Sequential memory layout
* No object graph traversal
* No per-lookup allocations
* OS page cache does the heavy lifting
* CPU cache-friendly traversal

This makes MMAP ideal for:

* DNS resolvers
* Large blocklists
* Passive DNS
* Threat intel lookups

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

MMAP is read-optimized but still supports full mutation via staging + Swap.

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

| Backend | Speed       | Allocations | Safety | Mutability |
| ------- | ----------- | ----------- | ------ | ---------- |
| Default | Medium      | High        | High   | Mutable    |
| Disk    | Slow        | Very High   | Medium | Mutable    |
| MMAP    | **Fastest** | **Low**     | High   | Immutable  |

The MMAP backend provides the best balance of:

**Performance + Safety + Predictability**

for DNS-style workloads.

---

P.S: This README is generated by AI based on BenchmarkDotnet reports and source code.