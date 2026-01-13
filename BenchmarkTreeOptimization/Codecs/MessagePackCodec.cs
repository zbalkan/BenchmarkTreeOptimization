using MessagePack;
using System;

namespace BenchmarkTreeOptimization.Codecs
{
    public sealed class MessagePackCodec<T> : IValueCodec<T> where T : class
    {
        public byte[] Encode(T value) => MessagePackSerializer.Serialize(value, MessagePack.Resolvers.ContractlessStandardResolver.Options);
        public T Decode(ReadOnlySpan<byte> data) => MessagePackSerializer.Deserialize<T>(data.ToArray());
    }
}