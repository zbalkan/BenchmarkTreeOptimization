using System.Collections.Immutable;
using System.Linq;

namespace BenchmarkTreeBackends.Backends.Graph
{
    public class DnsZoneNode<T>
    {
        public DnsZoneNode(T name)
        {
            Name = name;
        }

        public T Name { get; init; }
        public ImmutableDictionary<RecordType, T[]> Records { get; private set; } = ImmutableDictionary<RecordType, T[]>.Empty;
        public ImmutableList<Record> RawRecords { get; private set; } = ImmutableList<Record>.Empty;

        public record Record(RecordType Type, T Target, int Priority, uint Ttl);

        public void AddRecord(Record record)
        {
            var newRaw = RawRecords.Add(record);
            var newRecords = ComputeRecords(newRaw);
            Records = newRecords;
            RawRecords = newRaw;
        }

        private static ImmutableDictionary<RecordType, T[]> ComputeRecords(ImmutableList<Record> records) =>
            records.GroupBy(r => r.Type)
                   .ToImmutableDictionary(
                       g => g.Key,
                       g => g.OrderBy(r => r.Priority).Select(r => r.Target).ToArray()
                   );
    }
}
