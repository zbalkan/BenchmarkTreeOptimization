using System;

namespace BenchmarkTreeOptimization.Codecs
{
    public interface IValueCodec<T> where T : class
    {
        byte[] Encode(T value);

        T Decode(ReadOnlySpan<byte> data);
    }
}