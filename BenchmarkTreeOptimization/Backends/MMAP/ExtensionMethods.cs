using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BenchmarkTreeOptimization.Backends.MMAP
{
    public static class ExtensionMethods
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteStruct<T>(this BinaryWriter bw, in T value) where T : struct
        {
            Span<byte> buf = stackalloc byte[Marshal.SizeOf<T>()];
            T tmp = value;
            MemoryMarshal.Write(buf, ref tmp);
            bw.Write(buf);
        }
    }
}