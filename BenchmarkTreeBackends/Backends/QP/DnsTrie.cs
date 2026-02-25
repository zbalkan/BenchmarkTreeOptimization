// DnsTrie.cs — Lock-Free DNS QP-Trie for .NET 9 / C# 13
// Original algorithm: Tony Finch <dot@dotat.at> (CC0 Public Domain)
// See README.md for usage. See ARCHITECTURE.md for design rationale,
// data structures, algorithms, complexity, and change history.

using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

using System.Runtime.Intrinsics;

using System.Threading;

namespace BenchmarkTreeBackends.Backends.QP
{
    //  Public types
    /// <summary>
    /// Diagnostic statistics returned by <see cref="DnsTrie{TValue}.GetStats"/>.
    /// </summary>
    public readonly struct TrieStats(int nodeCount, int leafCount, int maxDepth)
    {
        /// <summary>Total number of trie nodes (branch + leaf).</summary>
        public int NodeCount { get; } = nodeCount;

        /// <summary>Number of leaf nodes (= number of stored entries).</summary>
        public int LeafCount { get; } = leafCount;

        /// <summary>Maximum depth from root to any leaf.</summary>
        public int MaxDepth { get; } = maxDepth;

        /// <inheritdoc/>
        public override string ToString() =>
            $"Nodes={NodeCount}, Leaves={LeafCount}, MaxDepth={MaxDepth}";
    }

    /// <summary>
    /// Read-only view of a <see cref="DnsTrie{TValue}"/>.
    /// Exposes lookup, cursor-based ordered iteration, and cardinality.
    /// Safe for concurrent use; zero CAS on all members.
    /// </summary>
    /// <typeparam name="TValue">The type of values stored in the trie.</typeparam>
    public interface IReadOnlyDnsTrie<TValue> : IEnumerable<KeyValuePair<string, TValue>>
    {
        /// <summary>
        /// Approximate entry count.  Exact when no concurrent writes are in progress.
        /// <b>Must not be used for coordination logic</b> (e.g. "if Count == 0, skip
        /// lookup") — use <see cref="TryGet(string, out TValue)"/> for that.
        /// </summary>
        int Count { get; }

        /// <summary>Retrieves the value associated with <paramref name="name"/>.</summary>
        bool TryGet(string name, out TValue value);

        /// <summary>
        /// Retrieves the value for the RFC 1035 wire-format name
        /// <paramref name="wireName"/>.
        /// </summary>
        bool TryGet(ReadOnlySpan<byte> wireName, out TValue value);

        /// <summary>Returns <see langword="true"/> if <paramref name="name"/> is present.</summary>
        bool ContainsKey(string name);

        /// <summary>
        /// Returns the value for <paramref name="name"/>, or <c>default</c> when absent.
        /// </summary>
        TValue? GetValueOrDefault(string name);

        /// <summary>
        /// Advances an in-order cursor.  Pass <see langword="null"/> for the first entry.
        /// </summary>
        bool TryGetNext(string? currentName, out string nextName, out TValue nextValue);

        /// <summary>Returns diagnostic node, leaf, and depth statistics.</summary>
        TrieStats GetStats();
    }

    /// <summary>
    /// A generic, lock-free, ordered associative map keyed by domain names,
    /// implemented as a QP-Trie tuned for the DNS hostname alphabet.
    /// Safe for concurrent use by multiple readers and writers without external locking.
    /// </summary>
    /// <typeparam name="TValue">
    /// The type of values stored in the trie.  Unconstrained: both reference types
    /// and value types (structs) are supported without boxing.
    /// </typeparam>
    public sealed class DnsTrie<TValue>
        : IReadOnlyDnsTrie<TValue>, IEnumerable<KeyValuePair<string, TValue>>
    {
        // Bitmap bit-position constants. Bits 0–1 reserved; excluded from PopCount via BitmapMask.
        // See ARCHITECTURE.md §3 for the full 64-bit word layout.
        private const byte BNobyte = 2;   // label-separator / end-of-key sentinel
        private const byte BBlock0 = 3;   // split block: control chars 0x00–0x1F
        private const byte BBlockA1 = 4;   // split block: 0x20–0x2C
        private const byte BHyphen = 5;   // '-' — direct single-bit
        private const byte BDot = 6;   // '.' — direct single-bit
        private const byte BSlash = 7;   // '/' — direct single-bit
        private const byte BDigit = 8;   // digits 0–9 → bits 8–17 (10 bits)
        private const byte BBlockC1 = 18;  // split block: 0x3A–0x3F  (:;<=>?)
        private const byte BBlock2 = 19;  // split block: 0x40 + 0x5B–0x5E ([\]^)
        private const byte BUnder = 20;  // '_' — direct single-bit
        private const byte BBackq = 21;  // '`' — direct single-bit
        private const byte BLetter = 22;  // letters a–z → bits 22–47 (26 bits)
        private const byte BBlock3 = 48;  // split block: 0x7B–0x7F ({|}~ DEL)
        private const byte BBlock4 = 49;  // split block: 0x80–0x9F
        private const byte BBlock5 = 50;  // split block: 0xA0–0xBF
        private const byte BBlock6 = 51;  // split block: 0xC0–0xDF
        private const byte BBlock7 = 52;  // split block: 0xE0–0xFF

        // Lower-half base for split bytes: BLower + (byte % 32). BLower+32 < 53 keeps
        // the range within BitmapMask and below the key-offset field at bit 53.
        private const byte BLower = BBlock0;

        // Bits 2–52 (51 bits). Excludes key-offset field (53+) and reserved bits 0–1.
        private const ulong BitmapMask = (1UL << 53) - 1 & ~3UL;

        // Bytes whose source value maps to one of these bits require a second key byte
        // encoding the low 5 bits (3+5 split scheme). See ARCHITECTURE.md §3.1.
        private const ulong SplitMask =
            1UL << BBlock0 | 1UL << BBlockA1 | 1UL << BBlockC1 |
            1UL << BBlock2 | 1UL << BBlock3 | 1UL << BBlock4 |
            1UL << BBlock5 | 1UL << BBlock6 | 1UL << BBlock7;

        // Worst case: 255 bytes × 2 (all split) + 2 terminators = 512; +2 for safe terminator write.
        private const int KeyCapacity = 514;

        // RFC 1035 §2.3.4: max 127 non-root labels.
        private const int MaxLabelCount = 127;

        // Params BuildBulk: ≤ this many items use direct Set() rather than sort+build.
        private const int DirectInsertThreshold = 16;

        private static readonly byte[] s_insensitiveByteToBit = BuildTable(caseInsensitive: true);
        private static readonly byte[] s_sensitiveByteToBit = BuildTable(caseInsensitive: false);

        private static byte[] BuildTable(bool caseInsensitive)
        {
            var t = new byte[256];
            for (int b = 0; b < 256; b++)
            {
                t[b] = b switch
                {
                    < 0x20 => BBlock0,
                    < '-' => BBlockA1,
                    '-' => BHyphen,
                    '.' => BDot,
                    '/' => BSlash,
                    >= '0' and <= '9' => (byte)(BDigit + b - '0'),
                    // 0x3A–0x3F (:;<=>?) → BBlockC1
                    // 0x40 (@)           → BBlock2  (block 2: 64÷32 = 2)
                    // "> '9' and < 'A'" would incorrectly capture '@' in BBlockC1.
                    > '9' and < '@' => BBlockC1,
                    '@' => BBlock2,
                    >= 'A' and <= 'Z' when caseInsensitive => (byte)(BLetter + b - 'A'),
                    >= 'A' and <= 'Z' => BBlock2,
                    > 'Z' and < '_' => BBlock2,
                    '_' => BUnder,
                    '`' => BBackq,
                    >= 'a' and <= 'z' => (byte)(BLetter + b - 'a'),
                    <= 0x7F => BBlock3,
                    < 0xA0 => BBlock4,
                    < 0xC0 => BBlock5,
                    < 0xE0 => BBlock6,
                    _ => BBlock7,
                };
            }
            return t;
        }

        private static readonly SearchValues<char> s_backslash = SearchValues.Create("\\");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool KeysEqual(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
            => a.SequenceEqual(b);

        // Returns index of first differing byte, or -1 when spans are identical.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FirstDiffOffset(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
        {
            int common = a.CommonPrefixLength(b);
            return common == a.Length && common == b.Length ? -1 : common;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte[] AllocKey(ReadOnlySpan<byte> src)
        {
            byte[] arr = GC.AllocateUninitializedArray<byte>(src.Length);
            src.CopyTo(arr);
            return arr;
        }

        /// <summary>
        /// Immutable snapshot of a branch node's children.
        /// Published atomically via Interlocked.CompareExchange.
        /// Readers acquire a coherent (Bitmap, Twigs[]) pair via a single volatile read.
        /// </summary>
        private sealed class BranchState(ulong bitmap, TrieNode[] twigs)
        {
            internal readonly ulong Bitmap = bitmap;
            internal readonly TrieNode[] Twigs = twigs;
        }

        private abstract class TrieNode { }

        private sealed class BranchNode(int keyOffset, BranchState initialState) : TrieNode
        {
            internal readonly int KeyOffset = keyOffset;
            internal volatile BranchState _state = initialState;
        }

        /// <summary>
        /// Fully immutable leaf.
        /// <see cref="EncodedKey"/> is stored to allow SIMD leaf verification and
        /// </summary>
        private sealed class LeafNode(string key, byte[] encodedKey, TValue value) : TrieNode
        {
            internal readonly string Key = key;
            internal readonly byte[] EncodedKey = encodedKey; // length excludes double-BNobyte terminator
            internal readonly TValue Value = value;
        }

        private volatile TrieNode? _root;
        private int _count;
        private readonly byte[] _table;         // s_insensitiveByteToBit or s_sensitiveByteToBit
        private readonly bool _wire;          // true → string API encodes labels TLD-first
        private readonly bool _caseSensitive; // stored for SIMD encoding path

        /// <summary>
        /// Approximate entry count.  Exact when no concurrent writes are in progress;
        /// may transiently lag by ±1 under concurrent modification.
        /// <b>Do not use for coordination logic.</b>
        /// </summary>
        public int Count => Volatile.Read(ref _count);

        /// <param name="caseSensitive">
        ///   When <see langword="false"/> (default), upper- and lower-case ASCII
        ///   letters map to the same trie bit — RFC 4343 correct for DNS.
        /// </param>
        /// <param name="wireFormat">
        ///   When <see langword="true"/>, the string-key API encodes labels
        ///   right-to-left (TLD first) so iteration order matches RFC 4034 canonical
        ///   DNS ordering.  Wire-byte overloads always use TLD-first encoding.
        /// </param>
        public DnsTrie(bool caseSensitive = false, bool wireFormat = false)
        {
            _caseSensitive = caseSensitive;
            _table = caseSensitive ? s_sensitiveByteToBit : s_insensitiveByteToBit;
            _wire = wireFormat;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong BitsBelow(int bit) => (1UL << bit) - 1 & BitmapMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool HasTwig(BranchState s, byte bit) =>
            (s.Bitmap & 1UL << bit) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int TwigOffset(BranchState s, byte bit) =>
            BitOperations.PopCount(s.Bitmap & BitsBelow(bit));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int TwigCount(BranchState s) =>
            BitOperations.PopCount(s.Bitmap & BitmapMask);

        // NearTwig falls back to slot 0 when the exact bit is absent.
        // This allows trie descent to continue towards an arbitrary nearby leaf
        // even when the key is not present — required by the insert and iterate paths.
        // IMPORTANT: using TwigOffset directly when HasTwig is false causes an
        // out-of-bounds array access; always use NearTwig for speculative descent.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int NearTwig(BranchState s, byte bit) =>
            HasTwig(s, bit) ? TwigOffset(s, bit) : 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsSplit(byte bit) => (SplitMask & 1UL << bit) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte SplitLower(byte ch) => (byte)(BLower + ch % 32);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte KeyBit(scoped ReadOnlySpan<byte> key, int keyLen, int off) =>
            (uint)off < (uint)keyLen ? key[off] : BNobyte;

        //
        //  NormaliseName:  strips ALL trailing dots; "example.com..." → "example.com".
        //                  A lone "." (root zone) is preserved.
        //                   original stripped only one dot, causing mismatched
        //                  keys for double-qualified names like "example.com..".
        //
        //  DecodeChar / SkipChar — RFC 1035 §5.1 escape sequences (slow path only):
        //    \DDD  three decimal digits → byte value 100·d₀ + 10·d₁ + d₂  [0..255]
        //    \X    any non-digit        → literal byte value of X
        //    Activated only when SearchValues detects a backslash in the input.
        //
        //  EncodeText / EncodeWire — each has two internal paths:
        //    Fast (no backslash detected): direct table lookup, no escape branching.
        //      EncodeWire fast path also uses SIMD IndexOf('.') for label boundaries.
        //    Slow (backslash present): full DecodeChar / SkipChar per character.
        //
        //  EncodeWireRaw — genuine RFC 1035 length-prefixed wire bytes.
        //    No escapes possible; always takes the direct path; no SearchValues guard.
        //  strip ALL trailing dots, not just one.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ReadOnlySpan<char> NormaliseName(ReadOnlySpan<char> name)
        {
            while (name.Length > 1 && name[^1] == '.')
                name = name[..^1];
            return name;
        }

        // RFC 1035 §5.1 single-character decoder — slow path only.
        //  \DDD values above 255 are rejected (RFC requires a valid octet).
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte DecodeChar(ReadOnlySpan<char> name, ref int i)
        {
            char c = name[i++];
            if (c != '\\') return c < 128 ? (byte)c : (byte)0xFF;
            if (i >= name.Length) return (byte)'\\';

            if (name[i] >= '0' && name[i] <= '9')
            {
                if (i + 2 < name.Length
                    && name[i + 1] >= '0' && name[i + 1] <= '9'
                    && name[i + 2] >= '0' && name[i + 2] <= '9')
                {
                    int v = (name[i] - '0') * 100
                          + (name[i + 1] - '0') * 10
                          + (name[i + 2] - '0');
                    //  RFC 1035 §5.1 — \DDD must represent an octet (0–255).
                    if ((uint)v > 255) ThrowDecodeOverflow();
                    i += 3;
                    return (byte)v;
                }
            }

            char x = name[i++];
            return x < 128 ? (byte)x : (byte)0xFF;
        }

        // RFC 1035 §5.1 character skipper — advance past one escaped character.
        // The inner `else i++` prevents an infinite loop on malformed \D input.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SkipChar(ReadOnlySpan<char> name, ref int i)
        {
            if (name[i++] != '\\' || i >= name.Length) return;
            if (name[i] >= '0' && name[i] <= '9')
            {
                if (i + 2 < name.Length
                    && name[i + 1] >= '0' && name[i + 1] <= '9'
                    && name[i + 2] >= '0' && name[i + 2] <= '9')
                    i += 3;
                else
                    i++;
            }
            else { i++; }
        }

        // Emit one decoded source byte as one or two key bytes.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void EmitByte(byte ch, byte[] table, Span<byte> key, ref int off)
        {
            byte bit = table[ch];
            key[off++] = bit;
            if (IsSplit(bit)) key[off++] = SplitLower(ch);
        }

        //
        // SIMD fast path: encodes 16 elements per iteration using arithmetic instead
        // of table lookup. Only valid for the clean DNS alphabet — chars outside
        // [0x2D,0x39] or [0x5F,0x7A] cause an immediate break to the scalar tail.
        // See ARCHITECTURE.md §4.5 for the arithmetic derivation.
        /// <summary>
        /// Encodes as many chars as possible from <paramref name="chars"/> using
        /// the SIMD arithmetic fast path, writing the results to
        /// <paramref name="key"/>[<paramref name="off"/>..].
        /// </summary>
        /// <returns>Number of input chars consumed (always a multiple of 16).</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int SimdEncodeClean(
            ReadOnlySpan<char> chars,
            Span<byte> key,
            ref int off,
            bool caseInsensitive)
        {
            if (!Vector128.IsHardwareAccelerated || chars.Length < 16) return 0;

            ref char src = ref MemoryMarshal.GetReference(chars);
            ref byte dest = ref MemoryMarshal.GetReference(key);

            // Constant vectors — JIT hoists these out of the loop as XMM constants.
            var vAUp = Vector128.Create((byte)'A');   // 0x41  uppercase fold lower bound
            var vZUp = Vector128.Create((byte)'Z');   // 0x5A  uppercase fold upper bound
            var v0x20 = Vector128.Create((byte)0x20);  // ASCII lowercase bit
            var vLo = Vector128.Create((byte)0x2D);  // clean low  group start: '-'
            var vLoHi = Vector128.Create((byte)0x39);  // clean low  group end:   '9'
            var vHiLo = Vector128.Create((byte)0x5F);  // clean high group start: '_'
            var vHiHi = Vector128.Create((byte)0x7A);  // clean high group end:   'z'
            var v40 = Vector128.Create((byte)40);    // subtract for low  group
            var v75 = Vector128.Create((byte)75);    // subtract for high group

            int i = 0;
            while (i + 16 <= chars.Length)
            {
                // Destination bounds guard — must precede StoreUnsafe.
                // For any RFC-compliant name this branch is never taken (max encoded
                // length = 512 bytes; KeyCapacity = 514).  For crafted overlong input
                // it breaks cleanly to the scalar tail, which uses safe span indexing
                // and will throw IndexOutOfRangeException rather than corrupt the stack.
                if (off + 16 > key.Length) break;

                // Load 16 UTF-16 chars as two vectors of 8 ushorts each.
                var lo = Vector128.LoadUnsafe(
                    ref Unsafe.As<char, ushort>(ref Unsafe.Add(ref src, i)));
                var hi = Vector128.LoadUnsafe(
                    ref Unsafe.As<char, ushort>(ref Unsafe.Add(ref src, i + 8)));

                // Non-ASCII check: OR both vectors, shift away the low byte; any
                // remaining non-zero lane means a char > 0x7F — bail to scalar.
                if (!Vector128.EqualsAll(
                        Vector128.ShiftRightLogical(lo | hi, 8),
                        Vector128<ushort>.Zero))
                    break;

                // Narrow ushort→byte (safe: high bytes verified zero above).
                // lower 8 chars → first 8 lanes; upper 8 chars → last 8 lanes.
                var narrow = Vector128.Narrow(lo, hi);

                // Fold A–Z → a–z for case-insensitive mode.
                if (caseInsensitive)
                {
                    var isUpper = Vector128.GreaterThanOrEqual(narrow, vAUp)
                                & Vector128.LessThanOrEqual(narrow, vZUp);
                    narrow |= isUpper & v0x20;
                }

                // Clean check: every lane must be in [0x2D,0x39] or [0x5F,0x7A].
                // inLow / inHigh are 0xFF per lane when true, 0x00 when false.
                var inLow = Vector128.GreaterThanOrEqual(narrow, vLo)
                           & Vector128.LessThanOrEqual(narrow, vLoHi);
                var inHigh = Vector128.GreaterThanOrEqual(narrow, vHiLo)
                           & Vector128.LessThanOrEqual(narrow, vHiHi);

                if (!Vector128.EqualsAll(inLow | inHigh, Vector128<byte>.AllBitsSet))
                    break;

                // Arithmetic encoding — no table load, no branch:
                //   inHigh lane 0xFF → choose (narrow − 75)
                //   inHigh lane 0x00 → choose (narrow − 40)
                var result = Vector128.ConditionalSelect(inHigh, narrow - v75, narrow - v40);

                result.StoreUnsafe(ref Unsafe.Add(ref dest, off));
                off += 16;
                i += 16;
            }
            return i;
        }

        /// <summary>
        /// Byte-span variant of <see cref="SimdEncodeClean"/> for use in
        /// <c>EncodeWireRaw</c> where the input is already RFC 1035 wire bytes
        /// (no UTF-16 narrowing required).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int SimdEncodeCleanBytes(
            ReadOnlySpan<byte> bytes,
            Span<byte> key,
            ref int off,
            bool caseInsensitive)
        {
            if (!Vector128.IsHardwareAccelerated || bytes.Length < 16) return 0;

            ref byte src = ref MemoryMarshal.GetReference(bytes);
            ref byte dest = ref MemoryMarshal.GetReference(key);

            var vAUp = Vector128.Create((byte)'A');
            var vZUp = Vector128.Create((byte)'Z');
            var v0x20 = Vector128.Create((byte)0x20);
            var v80 = Vector128.Create((byte)0x80);  // non-ASCII sentinel
            var vLo = Vector128.Create((byte)0x2D);
            var vLoHi = Vector128.Create((byte)0x39);
            var vHiLo = Vector128.Create((byte)0x5F);
            var vHiHi = Vector128.Create((byte)0x7A);
            var v40 = Vector128.Create((byte)40);
            var v75 = Vector128.Create((byte)75);

            int i = 0;
            while (i + 16 <= bytes.Length)
            {
                // Destination bounds guard — see SimdEncodeClean for rationale.
                if (off + 16 > key.Length) break;

                var narrow = Vector128.LoadUnsafe(ref Unsafe.Add(ref src, i));

                // Reject non-ASCII bytes (≥ 0x80): they produce split outputs.
                if (!Vector128.EqualsAll(
                        Vector128.LessThan(narrow, v80),
                        Vector128<byte>.AllBitsSet))
                    break;

                if (caseInsensitive)
                {
                    var isUpper = Vector128.GreaterThanOrEqual(narrow, vAUp)
                                & Vector128.LessThanOrEqual(narrow, vZUp);
                    narrow |= isUpper & v0x20;
                }

                var inLow = Vector128.GreaterThanOrEqual(narrow, vLo)
                           & Vector128.LessThanOrEqual(narrow, vLoHi);
                var inHigh = Vector128.GreaterThanOrEqual(narrow, vHiLo)
                           & Vector128.LessThanOrEqual(narrow, vHiHi);

                if (!Vector128.EqualsAll(inLow | inHigh, Vector128<byte>.AllBitsSet))
                    break;

                var result = Vector128.ConditionalSelect(inHigh, narrow - v75, narrow - v40);

                result.StoreUnsafe(ref Unsafe.Add(ref dest, off));
                off += 16;
                i += 16;
            }
            return i;
        }

        // Encode left-to-right with RFC 1035 §5.1 escape handling.
        [SkipLocalsInit]
        private int EncodeText(ReadOnlySpan<char> name, scoped Span<byte> key)
        {
            byte[] table = _table;
            int off = 0;

            if (name.IndexOfAny(s_backslash) < 0)
            {
                // Fast path: no escape sequences present:
                //  consume as many 16-char chunks as possible with
                // the SIMD arithmetic path; scalar loop covers the tail and any
                // chunk containing a split-producer or non-ASCII char.
                int i = SimdEncodeClean(name, key, ref off, !_caseSensitive);
                for (; i < name.Length; i++)
                {
                    char c = name[i];
                    byte ch = c < 128 ? (byte)c : (byte)0xFF;
                    byte bit = table[ch];
                    key[off++] = bit;
                    if (IsSplit(bit)) key[off++] = SplitLower(ch);
                }
            }
            else
            {
                // Slow path: \DDD / \X escape sequences:
                int i = 0;
                while (i < name.Length)
                    EmitByte(DecodeChar(name, ref i), table, key, ref off);
            }

            key[off] = BNobyte;
            return off;
        }

        // Encode in RFC 4034 canonical label-reversed order (TLD first).
        //
        // Pass 1 — locate label boundaries:
        //   Fast path: SIMD IndexOf('.') — one call per label, no per-char work.
        //   Slow path: SkipChar loop — escape-aware, treats '\.' as literal.
        //
        // Pass 2 — encode labels TLD-first with direct lookup (fast) or
        //   DecodeChar (slow when backslashes are present).
        //
        //  labelCount is bounds-checked before every write into lpos[] / lend[].
        [SkipLocalsInit]
        private int EncodeWire(ReadOnlySpan<char> name, scoped Span<byte> key)
        {
            Span<int> lpos = stackalloc int[MaxLabelCount + 1];
            Span<int> lend = stackalloc int[MaxLabelCount + 1];
            int labelCount = 0, i = 0;

            bool hasEscape = name.IndexOfAny(s_backslash) >= 0;

            // Pass 1: label boundary discovery:
            if (!hasEscape)
            {
                while (i < name.Length)
                {
                    //  enforce RFC 1035 §2.3.4 label limit before buffer write.
                    if (labelCount >= MaxLabelCount) ThrowTooManyLabels();

                    lpos[labelCount] = i;
                    int dot = name[i..].IndexOf('.');
                    if (dot < 0) { lend[labelCount++] = name.Length; break; }
                    lend[labelCount++] = i + dot;
                    i += dot + 1;
                }
            }
            else
            {
                while (i < name.Length)
                {
                    if (labelCount >= MaxLabelCount) ThrowTooManyLabels();

                    lpos[labelCount] = i;
                    while (i < name.Length && name[i] != '.') SkipChar(name, ref i);
                    lend[labelCount++] = i;
                    if (i < name.Length) i++;
                }
            }

            // Pass 2: encode labels TLD-first:
            byte[] table = _table;
            int off = 0;

            if (!hasEscape)
            {
                for (int li = labelCount - 1; li >= 0; li--)
                {
                    //  slice the label, run SIMD on whole 16-char
                    // chunks, scalar loop covers the tail (< 16 chars or any chunk
                    // with a split-producer).
                    ReadOnlySpan<char> label = name.Slice(lpos[li], lend[li] - lpos[li]);
                    int j = SimdEncodeClean(label, key, ref off, !_caseSensitive);
                    for (; j < label.Length; j++)
                    {
                        char c = label[j];
                        byte ch = c < 128 ? (byte)c : (byte)0xFF;
                        byte bit = table[ch];
                        key[off++] = bit;
                        if (IsSplit(bit)) key[off++] = SplitLower(ch);
                    }
                    key[off++] = BNobyte;
                }
            }
            else
            {
                for (int li = labelCount - 1; li >= 0; li--)
                {
                    int j = lpos[li], end = lend[li];
                    while (j < end) EmitByte(DecodeChar(name, ref j), table, key, ref off);
                    key[off++] = BNobyte;
                }
            }

            key[off] = BNobyte;
            return off;
        }

        // Encode genuine RFC 1035 length-prefixed wire bytes in label-reversed order.
        // Wire bytes carry no escape sequences — always the direct path.
        //
        //  labels is bounds-checked before every write into dope[].
        //  [SkipLocalsInit] added — dope[] is fully written before read.
        [SkipLocalsInit]
        private int EncodeWireRaw(ReadOnlySpan<byte> wire, scoped Span<byte> key)
        {
            Span<int> dope = stackalloc int[MaxLabelCount + 1];
            int labels = 0, p = 0;

            while (p < wire.Length)
            {
                byte len = wire[p];
                if (len == 0) break;
                //  enforce label limit before buffer write.
                if (labels >= MaxLabelCount) ThrowTooManyLabels();
                dope[labels++] = p;
                p += 1 + len;
            }

            byte[] table = _table;
            int off = 0;
            for (int li = labels - 1; li >= 0; li--)
            {
                int lenPos = dope[li];
                int len = wire[lenPos];
                //  slice the label bytes, run SIMD on 16-byte chunks.
                ReadOnlySpan<byte> labelBytes = wire.Slice(lenPos + 1, len);
                int j = SimdEncodeCleanBytes(labelBytes, key, ref off, !_caseSensitive);
                for (; j < labelBytes.Length; j++)
                    EmitByte(labelBytes[j], table, key, ref off);
                key[off++] = BNobyte;
            }
            key[off] = BNobyte;
            return off;
        }

        // Convert RFC 1035 wire bytes to canonical presentation string.
        // Case-folds to lower-case; joins labels with '.'.  Root zone → ".".
        private static string CanonicaliseWireName(ReadOnlySpan<byte> wire)
        {
            int p = 0, totalChars = 0, labelCount = 0;
            while (p < wire.Length)
            {
                byte len = wire[p++];
                if (len == 0) break;
                totalChars += len;
                labelCount++;
                p += len;
            }
            if (labelCount == 0) return ".";
            if (labelCount > 1) totalChars += labelCount - 1; // dots between labels

            char[] buf = ArrayPool<char>.Shared.Rent(totalChars);
            try
            {
                p = 0; int w = 0, written = 0;
                while (p < wire.Length)
                {
                    byte len = wire[p++];
                    if (len == 0) break;
                    for (int i = 0; i < len; i++)
                    {
                        byte b = wire[p++];
                        if (b >= 'A' && b <= 'Z') b = (byte)(b + 32);
                        buf[w++] = (char)b;
                    }
                    if (++written < labelCount) buf[w++] = '.';
                }
                return new string(buf, 0, totalChars);
            }
            finally { ArrayPool<char>.Shared.Return(buf); }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Encode(ReadOnlySpan<char> name, scoped Span<byte> key)
        {
            ReadOnlySpan<char> n = NormaliseName(name);
            return _wire ? EncodeWire(n, key) : EncodeText(n, key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Encode(string name, scoped Span<byte> key) => Encode(name.AsSpan(), key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CasRoot(TrieNode? desired, TrieNode? expected) =>
            Interlocked.CompareExchange(ref _root, desired, expected) == expected;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool CasState(BranchNode node, BranchState desired, BranchState expected) =>
            Interlocked.CompareExchange(ref node._state, desired, expected) == expected;

        private static TrieNode[] CloneWithReplacement(BranchState state, int s, TrieNode repl)
        {
            var twigs = (TrieNode[])state.Twigs.Clone();
            twigs[s] = repl;
            return twigs;
        }

        private static TrieNode[] CloneWithInsertion(BranchState state, int s, TrieNode inserted)
        {
            int m = state.Twigs.Length;
            var twigs = new TrieNode[m + 1];
            Array.Copy(state.Twigs, 0, twigs, 0, s);
            twigs[s] = inserted;
            Array.Copy(state.Twigs, s, twigs, s + 1, m - s);
            return twigs;
        }

        private static TrieNode[] CloneWithRemoval(BranchState state, int s)
        {
            int m = state.Twigs.Length - 1;
            var twigs = new TrieNode[m];
            Array.Copy(state.Twigs, 0, twigs, 0, s);
            Array.Copy(state.Twigs, s + 1, twigs, s, m - s);
            return twigs;
        }

        /// <summary>
        /// Retrieves the value associated with <paramref name="name"/>.
        /// </summary>
        [SkipLocalsInit]
        public bool TryGet(string name, out TValue value)
        {
            TrieNode? n = _root;
            if (n is null) { value = default!; return false; }

            Span<byte> key = stackalloc byte[KeyCapacity];
            int len = Encode(name, key);
            ReadOnlySpan<byte> keySpan = key[..len];

            while (n is BranchNode branch)
            {
                BranchState state = branch._state;
                byte bit = KeyBit(key, len, branch.KeyOffset);
                if (!HasTwig(state, bit)) { value = default!; return false; }
                n = state.Twigs[TwigOffset(state, bit)];
            }

            var leaf = (LeafNode)n;
            if (!KeysEqual(leaf.EncodedKey, keySpan)) { value = default!; return false; }
            value = leaf.Value;
            return true;
        }

        /// <summary>
        /// Retrieves the value for the RFC 1035 wire-format name
        /// <paramref name="wireName"/> (e.g. <c>\x03www\x07example\x03com\x00</c>).
        /// </summary>
        [SkipLocalsInit]
        public bool TryGet(ReadOnlySpan<byte> wireName, out TValue value)
        {
            TrieNode? n = _root;
            if (n is null) { value = default!; return false; }

            Span<byte> key = stackalloc byte[KeyCapacity];
            int len = EncodeWireRaw(wireName, key);
            ReadOnlySpan<byte> keySpan = key[..len];

            while (n is BranchNode branch)
            {
                BranchState state = branch._state;
                byte bit = KeyBit(key, len, branch.KeyOffset);
                if (!HasTwig(state, bit)) { value = default!; return false; }
                n = state.Twigs[TwigOffset(state, bit)];
            }

            var leaf = (LeafNode)n;
            if (!KeysEqual(leaf.EncodedKey, keySpan)) { value = default!; return false; }
            value = leaf.Value;
            return true;
        }

        /// <summary>
        /// Returns <see langword="true"/> if <paramref name="name"/> is present in the trie.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ContainsKey(string name) => TryGet(name, out _);

        /// <summary>
        /// Returns the value associated with <paramref name="name"/>,
        /// or <c>default</c> when the key is absent.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue? GetValueOrDefault(string name) =>
            TryGet(name, out TValue v) ? v : default;

        /// <summary>
        /// Inserts or updates <paramref name="name"/> → <paramref name="value"/>.
        /// </summary>
        /// <returns>
        ///   <see langword="true"/> if the key was newly inserted;
        ///   <see langword="false"/> if an existing entry was updated.
        /// </returns>
        [SkipLocalsInit]
        public bool Set(string name, TValue value)
        {
            Span<byte> newKey = stackalloc byte[KeyCapacity];
            int newLen = Encode(name.AsSpan(), newKey);
            return SetCoreImpl(newKey, newLen, storedKey: name, value);
        }

        /// <summary>
        /// Inserts or updates the entry identified by the RFC 1035 wire-format name
        /// <paramref name="wireName"/>.
        /// </summary>
        /// <returns>
        ///   <see langword="true"/> if the key was newly inserted;
        ///   <see langword="false"/> if an existing entry was updated.
        /// </returns>
        [SkipLocalsInit]
        public bool SetWire(ReadOnlySpan<byte> wireName, TValue value)
        {
            string canonical = CanonicaliseWireName(wireName);
            Span<byte> newKey = stackalloc byte[KeyCapacity];
            int newLen = EncodeWireRaw(wireName, newKey);
            return SetCoreImpl(newKey, newLen, storedKey: canonical, value);
        }

        // SetCoreImpl: unified CAS retry loop for Set and SetWire.
        // Correctness of single-descent update: when key K exists, HasTwig is true
        // at every node on its path, so NearTwig follows the identical descent.
        // The tracked nearParent is therefore the correct CAS target. A concurrent
        // modification invalidates the CAS, and the outer loop retries.
        private bool SetCoreImpl(
            scoped Span<byte> newKey, int newLen,
            string storedKey, TValue value)
        {
            ReadOnlySpan<byte> newKeySpan = newKey[..newLen];

            while (true)
            {
                TrieNode? root = _root;

                // Empty trie:
                if (root is null)
                {
                    if (CasRoot(new LeafNode(storedKey, AllocKey(newKeySpan), value), expected: null))
                    { Interlocked.Increment(ref _count); return true; }
                    continue;
                }

                // Pass 1: NearTwig descent + parent tracking:
                // Track the parent of the node we are about to land on, so that
                // the update case can CAS without a second descent.
                BranchNode? nearParent = null;
                BranchState? nearParentState = null;
                byte nearParentBit = 0;

                TrieNode n = root;
                while (n is BranchNode b)
                {
                    BranchState st = b._state;
                    byte bit = KeyBit(newKey, newLen, b.KeyOffset);
                    nearParent = b;
                    nearParentState = st;
                    nearParentBit = bit;
                    n = st.Twigs[NearTwig(st, bit)];
                }

                var nearLeaf = (LeafNode)n;
                int diffOff = FirstDiffOffset(newKeySpan, nearLeaf.EncodedKey);

                // Identical key: single-descent value update:
                if (diffOff < 0)
                {
                    var newLeaf = new LeafNode(storedKey, AllocKey(newKeySpan), value);
                    bool ok;
                    if (nearParent is null)
                    {
                        // Root is the lone leaf.
                        ok = CasRoot(newLeaf, expected: nearLeaf);
                    }
                    else
                    {
                        int s = TwigOffset(nearParentState!, nearParentBit);
                        var next = new BranchState(nearParentState!.Bitmap,
                                                   CloneWithReplacement(nearParentState!, s, newLeaf));
                        ok = CasState(nearParent, next, expected: nearParentState!);
                    }
                    if (ok) return false;
                    continue;
                }

                // Structural insert:
                byte newBit = newKey[diffOff];
                byte oldBit = (uint)diffOff < (uint)nearLeaf.EncodedKey.Length
                                ? nearLeaf.EncodedKey[diffOff]
                                : BNobyte;
                var insert = new LeafNode(storedKey, AllocKey(newKeySpan), value);

                // Pass 2: locate structural insertion point:
                BranchNode? parent = null;
                BranchState? parentState = null;
                byte parentBit = 0;
                n = root;

                while (n is BranchNode branch)
                {
                    BranchState state = branch._state;

                    if (branch.KeyOffset == diffOff)
                    {
                        // GROW: add newBit to existing branch at diffOff.
                        // If newBit is already present, a concurrent insert raced us; restart.
                        if (HasTwig(state, newBit)) break;

                        var grown = new BranchState(
                            state.Bitmap | 1UL << newBit,
                            CloneWithInsertion(state, TwigOffset(state, newBit), insert));

                        if (CasState(branch, grown, expected: state))
                        { Interlocked.Increment(ref _count); return true; }
                        break;
                    }

                    if (branch.KeyOffset > diffOff) break; // NEW BRANCH needed above here

                    byte bit = KeyBit(newKey, newLen, branch.KeyOffset);
                    if (!HasTwig(state, bit)) break;       // NEW BRANCH (path absent)

                    parent = branch;
                    parentState = state;
                    parentBit = bit;
                    n = state.Twigs[TwigOffset(state, bit)];
                }

                // NEW BRANCH: 2-child node at diffOff:
                {
                    ulong bitmap = 1UL << newBit | 1UL << oldBit;
                    // The lower-numbered bit occupies slot 0 (TwigOffset counts bits strictly below).
                    TrieNode[] newTwigs = newBit < oldBit
                        ? [insert, n]
                        : [n, insert];

                    var newSt = new BranchState(bitmap, newTwigs);
                    var newBr = new BranchNode(diffOff, newSt);

                    bool ok = parent is null
                        ? CasRoot(newBr, expected: root)
                        : CasState(parent,
                              new BranchState(parentState!.Bitmap,
                                  CloneWithReplacement(parentState!,
                                      TwigOffset(parentState!, parentBit), newBr)),
                              expected: parentState!);

                    if (ok) { Interlocked.Increment(ref _count); return true; }
                }
            }
        }

        /// <summary>
        /// Removes the entry keyed by <paramref name="name"/> and returns the stored
        /// key and value.
        /// </summary>
        /// <returns><see langword="true"/> if found and removed.</returns>
        /// <remarks>
        /// <para>Three single-CAS cases:</para>
        /// <list type="number">
        ///   <item>Root is the lone leaf → CAS root to null.</item>
        ///   <item>Parent has 2 children → CAS grandparent to bypass parent (collapse).</item>
        ///   <item>Parent has 3+ children → CAS parent to shrunk BranchState.</item>
        /// </list>
        /// </remarks>
        [SkipLocalsInit]
        public bool Delete(string name, out string foundKey, out TValue foundValue)
        {
            Span<byte> key = stackalloc byte[KeyCapacity];
            int len = Encode(name, key);
            return DeleteCore(key[..len], key, len, out foundKey, out foundValue);
        }

        /// <summary>
        /// Removes the entry keyed by <paramref name="name"/>.
        /// </summary>
        /// <returns><see langword="true"/> if the key was found and removed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Delete(string name) => Delete(name, out _, out _);

        /// <summary>
        /// Removes the entry identified by the RFC 1035 wire-format name
        /// <paramref name="wireName"/>.
        /// </summary>
        [SkipLocalsInit]
        public bool DeleteWire(ReadOnlySpan<byte> wireName, out string foundKey, out TValue foundValue)
        {
            Span<byte> key = stackalloc byte[KeyCapacity];
            int len = EncodeWireRaw(wireName, key);
            return DeleteCore(key[..len], key, len, out foundKey, out foundValue);
        }

        [SkipLocalsInit]
        private bool DeleteCore(
            ReadOnlySpan<byte> keySpan, Span<byte> key, int len,
            out string foundKey, out TValue foundValue)
        {
            while (true)
            {
                TrieNode? root = _root;
                if (root is null)
                { foundKey = default!; foundValue = default!; return false; }

                BranchNode? grandParent = null;
                BranchState? grandParentState = null;
                byte grandBit = 0;
                BranchNode? parent = null;
                BranchState? parentState = null;
                byte parentBit = 0;
                TrieNode n = root;

                while (n is BranchNode branch)
                {
                    BranchState state = branch._state;
                    byte bit = KeyBit(key, len, branch.KeyOffset);
                    if (!HasTwig(state, bit))
                    { foundKey = default!; foundValue = default!; return false; }

                    grandParent = parent;
                    grandParentState = parentState;
                    grandBit = parentBit;
                    parent = branch;
                    parentState = state;
                    parentBit = bit;
                    n = state.Twigs[TwigOffset(state, bit)];
                }

                var leaf = (LeafNode)n;
                if (!KeysEqual(leaf.EncodedKey, keySpan))
                { foundKey = default!; foundValue = default!; return false; }

                foundKey = leaf.Key;
                foundValue = leaf.Value;

                // Case 1: sole entry.
                if (parent is null)
                {
                    if (CasRoot(desired: null, expected: leaf))
                    { Interlocked.Decrement(ref _count); return true; }
                    continue;
                }

                int deletedSlot = TwigOffset(parentState!, parentBit);
                int childCount = TwigCount(parentState!);

                // Case 2: collapse — parent had exactly 2 children.
                if (childCount == 2)
                {
                    TrieNode sibling = parentState!.Twigs[deletedSlot == 0 ? 1 : 0];
                    bool ok;
                    if (grandParent is null)
                        ok = CasRoot(sibling, expected: parent);
                    else
                    {
                        int gs = TwigOffset(grandParentState!, grandBit);
                        ok = CasState(grandParent,
                                 new BranchState(grandParentState!.Bitmap,
                                     CloneWithReplacement(grandParentState!, gs, sibling)),
                                 expected: grandParentState!);
                    }
                    if (ok) { Interlocked.Decrement(ref _count); return true; }
                    continue;
                }

                // Case 3: shrink — parent had 3+ children.
                {
                    var next = new BranchState(
                        parentState!.Bitmap & ~(1UL << parentBit),
                        CloneWithRemoval(parentState!, deletedSlot));
                    if (CasState(parent, next, expected: parentState!))
                    { Interlocked.Decrement(ref _count); return true; }
                    continue;
                }
            }
        }

        /// <summary>
        /// Advances an in-order cursor to the lexicographically next entry.
        /// Pass <see langword="null"/> to return the first entry.
        /// </summary>
        /// <remarks>
        /// The cursor is a plain <c>string</c>; no iterator object is allocated.
        /// Snapshot semantics: a concurrent delete may cause a removed key to appear once.
        /// </remarks>
        [SkipLocalsInit]
        public bool TryGetNext(string? currentName, out string nextName, out TValue nextValue)
        {
            TrieNode? root = _root;
            if (root is null) { nextName = default!; nextValue = default!; return false; }

            Span<byte> queryKey = stackalloc byte[KeyCapacity];
            int queryLen;
            if (currentName is null)
            {
                // diffOff=0 causes Pass 2 to break immediately; leftmost-leaf walk then returns the first entry.
                queryLen = 0;
                queryKey[0] = BNobyte;
            }
            else
            {
                queryLen = Encode(currentName, queryKey);
            }

            // Pass 1: NearTwig descent to a nearby leaf:
            TrieNode n = root;
            while (n is BranchNode b)
            {
                BranchState st = b._state;
                n = st.Twigs[NearTwig(st, KeyBit(queryKey, queryLen, b.KeyOffset))];
            }

            //  use nearLeaf.EncodedKey directly (already encoded); no re-encode.
            // The diffOff computation mirrors the C original's loop:
            //   for(off = 0; off <= newl; off++) — tests up to and including the
            //   BNobyte terminator at index queryLen.
            // We replicate this by slicing queryLen+1 bytes from queryKey and up to
            // queryLen+1 bytes from nearKey (the virtual terminator beyond nearKey.Length
            // is BNobyte; when nearKey is shorter we pass it as-is and CommonPrefixLength
            // reports the mismatch at nearKey.Length, which is ≤ queryLen).
            var nearLeaf = (LeafNode)n;
            ReadOnlySpan<byte> nearKey = nearLeaf.EncodedKey;
            int diffOff = queryKey[..Math.Min(queryLen + 1, KeyCapacity)].CommonPrefixLength(
                              nearKey.Length > queryLen
                                  ? nearKey[..(queryLen + 1)]
                                  : nearKey);
            // Clamp: the C loop runs to off == newl (queryLen) at most.
            if (diffOff > queryLen) diffOff = queryLen;

            // Pass 2: walk the query path and record the deepest right-sibling subtree.
            // next = root for null currentName (first-entry); null otherwise.
            TrieNode? next = currentName is null ? root : null;
            n = root;

            while (n is BranchNode branch)
            {
                if (branch.KeyOffset >= diffOff) break;

                BranchState state = branch._state;
                byte bit = KeyBit(queryKey, queryLen, branch.KeyOffset);
                if (!HasTwig(state, bit)) break;

                int s = TwigOffset(state, bit);
                int m = TwigCount(state) - 1;
                if (s < m) next = state.Twigs[s + 1]; // right sibling subtree
                n = state.Twigs[s];
            }

            // Walk next down to its leftmost leaf.
            while (next is BranchNode nb)
            {
                BranchState st = nb._state;
                next = st.Twigs[0];
            }

            if (next is LeafNode result)
            {
                nextName = result.Key;
                nextValue = result.Value;
                return true;
            }

            nextName = default!;
            nextValue = default!;
            return false;
        }

        /// <summary>
        /// Returns diagnostic node, leaf, and depth counts for the current snapshot.
        /// Iterative BFS; snapshot semantics apply.
        /// </summary>
        public TrieStats GetStats()
        {
            TrieNode? root = _root;
            if (root is null) return new TrieStats(0, 0, 0);

            var stack = new Stack<(TrieNode Node, int Depth)>(64);
            stack.Push((root, 0));
            int nodes = 0, leaves = 0, maxDepth = 0;

            while (stack.Count > 0)
            {
                var (cur, depth) = stack.Pop();
                nodes++;
                if (depth > maxDepth) maxDepth = depth;

                if (cur is LeafNode) { leaves++; continue; }

                BranchState state = ((BranchNode)cur)._state;
                foreach (TrieNode child in state.Twigs)
                    stack.Push((child, depth + 1));
            }

            return new TrieStats(nodes, leaves, maxDepth);
        }

        /// <summary>
        /// Constructs a fully optimised trie from a data source in
        /// <b>O(n log n + n·k)</b>.  Duplicate keys use last-entry-wins semantics.
        /// </summary>
        [SkipLocalsInit]
        public static DnsTrie<TValue> BuildBulk(
            IEnumerable<(string Name, TValue Value)> items,
            bool caseSensitive = false,
            bool wireFormat = false)
        {
            ArgumentNullException.ThrowIfNull(items);

            var trie = new DnsTrie<TValue>(caseSensitive, wireFormat);
            var list = new List<BulkEntry>(capacity: 1024);

            // Hoisted above the loop; the buffer is fully overwritten before AllocKey reads it.
            // frame slot is reused on every iteration rather than growing the stack
            // by KeyCapacity bytes per entry.  The buffer is fully overwritten by
            // Encode* before AllocKey reads keyBuf[..len], so there is no
            // stale-data hazard.
            Span<byte> keyBuf = stackalloc byte[KeyCapacity];

            foreach (var (name, value) in items)
            {
                if (name is null) throw new ArgumentException("Item name must not be null.", nameof(items));

                ReadOnlySpan<char> normalised = NormaliseName(name.AsSpan());

                int len = wireFormat
                    ? trie.EncodeWire(normalised, keyBuf)
                    : trie.EncodeText(normalised, keyBuf);

                list.Add(new BulkEntry(normalised.ToString(), AllocKey(keyBuf[..len]), value));
            }

            if (list.Count == 0) return trie;

            list.Sort(static (a, b) =>
                a.EncodedKey.AsSpan().SequenceCompareTo(b.EncodedKey.AsSpan()));

            //  O(n) deduplication — single forward pass.
            // Sorted order guarantees duplicates are adjacent; we keep the last
            // entry for each key (last-wins, consistent with Set() semantics).
            int w = 0;
            for (int i = 0; i < list.Count; i++)
            {
                // If the next entry has the same encoded key, skip this one (next wins).
                if (i + 1 < list.Count &&
                    list[i].EncodedKey.AsSpan().SequenceEqual(list[i + 1].EncodedKey.AsSpan()))
                    continue;
                list[w++] = list[i];
            }
            if (w < list.Count)
                list.RemoveRange(w, list.Count - w);

            TrieNode builtRoot = BuildFromSorted(list, 0, list.Count, depth: 0);

            // volatile (release) write.  Passing a volatile field by ref to
            // Volatile.Write strips that guarantee and triggers CS0420.
            trie._root = builtRoot;
            Volatile.Write(ref trie._count, list.Count);
            return trie;
        }

        /// <summary>
        /// Inline overload using <c>params ReadOnlySpan</c> (C# 13 / .NET 9).
        /// For ≤ <c>16</c> items the compiler stack-allocates the tuple array and
        /// this overload inserts directly, with zero heap allocation at the call site.
        /// For larger sets the sorted bulk path is used.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The <c>params</c> parameter must be last (CS0231).  Pass <paramref name="caseSensitive"/>
        /// and <paramref name="wireFormat"/> as named arguments when non-default values are needed:
        /// </para>
        /// </remarks>
        /// <example>
        /// <code>
        /// // Default options — positional params, no named arguments needed:
        /// var trie = DnsTrie&lt;string&gt;.BuildBulk(
        ///     ("example.com",     "v4"),
        ///     ("ns1.example.com", "ns"));
        ///
        /// // Non-default options — pass them as named arguments before the tuples:
        /// var zone = DnsTrie&lt;string&gt;.BuildBulk(
        ///     caseSensitive: false, wireFormat: true,
        ///     ("example.com", "v4"), ("ns1.example.com", "ns"));
        /// </code>
        /// </example>
        public static DnsTrie<TValue> BuildBulk(
            bool caseSensitive = false,
            bool wireFormat = false,
            params ReadOnlySpan<(string Name, TValue Value)> items)
        {
            //  for small item counts, direct Set() is cheaper than
            // sort + bulk-build and preserves zero call-site heap allocation.
            if (items.Length <= DirectInsertThreshold)
            {
                var trie = new DnsTrie<TValue>(caseSensitive, wireFormat);
                foreach (var (name, value) in items)
                {
                    if (name is null) throw new ArgumentException("Item name must not be null.", nameof(items));
                    trie.Set(name, value);
                }
                return trie;
            }

            // For larger sets, fall through to the sorted bulk path.
            // The span must be materialised before calling the IEnumerable overload
            // because the span is only valid for this call frame.
            var list = new List<(string, TValue)>(items.Length);
            foreach (var item in items) list.Add(item);
            return BuildBulk((IEnumerable<(string, TValue)>)list, caseSensitive, wireFormat);
        }

        //  readonly record struct — three fields stored inline in
        // List<BulkEntry>, eliminating one heap allocation + GC root per entry.
        // Trade-off: if TValue is a large struct, copy-on-access cost replaces
        // allocation cost.  For the common DNS case (TValue is a reference type)
        // this is a net win of ~24 bytes of object header per entry.
        private readonly record struct BulkEntry(string Name, byte[] EncodedKey, TValue Value);

        private static TrieNode BuildFromSorted(List<BulkEntry> list, int start, int end, int depth)
        {
            if (end - start == 1)
            {
                var x = list[start];
                return new LeafNode(x.Name, x.EncodedKey, x.Value);
            }

            int splitOff = FindSplitOffset(list, start, end, depth);
            ulong bitmap = 0;
            var children = new List<TrieNode>(capacity: 8);

            int i = start;
            while (i < end)
            {
                byte bit = BulkKeyByte(list[i].EncodedKey, splitOff);
                int j = i + 1;
                while (j < end && BulkKeyByte(list[j].EncodedKey, splitOff) == bit) j++;
                bitmap |= 1UL << bit;
                children.Add(BuildFromSorted(list, i, j, splitOff + 1));
                i = j;
            }

            return new BranchNode(splitOff, new BranchState(bitmap, children.ToArray()));
        }

        // Scan forward from minOff to find the first key position at which the
        // entries in [start, end) are not all equal.
        // Complexity note: across the entire BuildFromSorted recursion tree, the
        // total work is O(n·k) because each (entry, bit-position) pair is examined
        // at most once — minOff only ever advances, never retreats.
        private static int FindSplitOffset(List<BulkEntry> list, int start, int end, int minOff)
        {
            byte[] firstKey = list[start].EncodedKey;
            for (int off = minOff; off < KeyCapacity; off++)
            {
                byte first = BulkKeyByte(firstKey, off);
                for (int i = start + 1; i < end; i++)
                    if (BulkKeyByte(list[i].EncodedKey, off) != first)
                        return off;
            }
            return KeyCapacity - 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte BulkKeyByte(byte[] key, int off) =>
            (uint)off < (uint)key.Length ? key[off] : BNobyte;

        /// <inheritdoc/>
        public IEnumerator<KeyValuePair<string, TValue>> GetEnumerator()
        {
            string? cursor = null;
            while (TryGetNext(cursor, out string name, out TValue val))
            {
                yield return new KeyValuePair<string, TValue>(name, val);
                cursor = name;
            }
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        [DoesNotReturn]
        private static void ThrowTooManyLabels() =>
            throw new ArgumentException(
                $"Domain name exceeds the RFC 1035 §2.3.4 limit of {MaxLabelCount} labels.");

        [DoesNotReturn]
        private static void ThrowDecodeOverflow() =>
            throw new ArgumentException(
                "DNS escape \\DDD value is not a valid octet (must be 0–255, RFC 1035 §5.1).");
    }
}