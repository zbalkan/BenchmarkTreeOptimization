using System;
using System.Text;

namespace BenchmarkTreeBackends.Codecs
{
    public sealed class Utf8StringCodec : IValueCodec<string>
    {
        public byte[] Encode(string value)
            => Encoding.UTF8.GetBytes(value);

        public string Decode(ReadOnlySpan<byte> data)
            => Encoding.UTF8.GetString(data);
    }
}