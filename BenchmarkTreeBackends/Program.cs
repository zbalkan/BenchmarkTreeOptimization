using BenchmarkDotNet.Running;

namespace BenchmarkTreeBackends
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            BenchmarkRunner.Run<DomainTreeBenchmark>();
        }
    }
}