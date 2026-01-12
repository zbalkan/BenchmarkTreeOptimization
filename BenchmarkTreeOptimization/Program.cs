using BenchmarkDotNet.Running;

namespace BenchmarkTreeOptimization
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            BenchmarkRunner.Run<DomainTreeBenchmark>();
        }
    }
}