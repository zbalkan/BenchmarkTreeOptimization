using System;
using System.IO;
using System.Runtime.InteropServices;

namespace BenchmarkTreeOptimization.Backends.MMAP
{
    public static  class ExtensionMethods
    {
        public static void WriteStruct<T>(this BinaryWriter bw, T value) where T : struct
        {
            Span<byte> buf = stackalloc byte[Marshal.SizeOf<T>()];
            MemoryMarshal.Write(buf, ref value);
            bw.Write(buf);
        }

    }
}
