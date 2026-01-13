using System;

namespace BenchmarkTreeBackends.Codecs
{
    public interface IValueCodec<T> where T : class
    {
        byte[] Encode(T value);

        T Decode(ReadOnlySpan<byte> data);
    }
}