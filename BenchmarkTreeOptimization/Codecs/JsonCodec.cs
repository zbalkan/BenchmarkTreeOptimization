using System;
using System.Text.Json;

namespace BenchmarkTreeOptimization.Codecs
{
    public sealed class JsonCodec<T> : IValueCodec<T> where T : class
    {
        private readonly JsonSerializerOptions _opt = new() { WriteIndented = false };

        public byte[] Encode(T value) => JsonSerializer.SerializeToUtf8Bytes(value, _opt);

        public T Decode(ReadOnlySpan<byte> data) =>
            JsonSerializer.Deserialize<T>(data, _opt) ?? throw new InvalidOperationException("Decode failed.");
    }
}