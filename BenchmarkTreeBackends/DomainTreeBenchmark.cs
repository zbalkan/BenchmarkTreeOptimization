using BenchmarkDotNet.Attributes;
using BenchmarkTreeBackends.Backends;
using BenchmarkTreeBackends.Backends.ByteTree;
using BenchmarkTreeBackends.Backends.Graph;
using BenchmarkTreeBackends.Backends.LMDB;
using BenchmarkTreeBackends.Backends.MMAP;
using BenchmarkTreeBackends.Codecs;
using System.IO;
using System.Runtime.CompilerServices;

namespace BenchmarkTreeBackends
{
    [MemoryDiagnoser]
    public class DomainTreeBenchmark
    {
        private DomainTree<string> _defaultTree;
        private DatabaseBackedDomainTree<string> _dbBackedTree;
        private DatabaseBackedDomainTree<string> _dbBackedTree2;
        private MmapBackedDomainTree<string> _mmapBackedTree;
        private MmapBackedDomainTree<string> _mmapBackedTree2;
        private DomainGraph<string> _graph;

        private const int N = 10_000_000;

        // Only domains valid for BOTH implementations
        private static readonly string[] TestDomains =
        {
            "google.com", "www.google.com", "mail.google.com", "drive.google.com",
            "microsoft.com", "www.microsoft.com", "login.microsoft.com",
            "github.com", "www.github.com", "api.github.com",
            "example.com", "www.example.com", "api.example.com",

            "wikipedia.org", "www.wikipedia.org", "en.wikipedia.org",
            "mozilla.org", "developer.mozilla.org",

            "a.b.c.d.e.f.g.h.i.j.k.example.com",
            "bbc.co.uk", "news.bbc.co.uk",
            "golang.org", "pkg.go.dev",

            "a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.com",

            "xn--hxajbheg2az3al.gr",

            "*.google.com"
        };

        [GlobalSetup]
        public void Setup()
        {
            _defaultTree = new DomainTree<string>();
            _dbBackedTree = new DatabaseBackedDomainTree<string>("treetest", new MessagePackCodec<string>());
            _dbBackedTree2 = new DatabaseBackedDomainTree<string>("treetest2", new Utf8StringCodec());
            _mmapBackedTree = new MmapBackedDomainTree<string>("treetest_mmap", new MessagePackCodec<string>());
            _mmapBackedTree2 = new MmapBackedDomainTree<string>("treetest_mmap2", new Utf8StringCodec());
            _graph = new DomainGraph<string>();

            Seed(_defaultTree);
            Seed(_dbBackedTree);
            Seed(_dbBackedTree2);
            Seed(_mmapBackedTree);
            Seed(_mmapBackedTree2);
            Seed(_graph);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _defaultTree.Clear();

            _dbBackedTree.Dispose();
            if (Directory.Exists("treetest"))
            {
                Directory.Delete("treetest", true);
            }
        }

        private static void Seed(IBackend<string, string> tree)
        {
            _ = tree.TryAdd("com", "com-root");
            _ = tree.TryAdd("org", "org-root");

            var subs = new[] { "google", "microsoft", "github", "example" };
            foreach (var sub in subs)
            {
                _ = tree.TryAdd($"{sub}.com", sub);
                _ = tree.TryAdd($"www.{sub}.com", sub);
                _ = tree.TryAdd($"api.{sub}.com", sub);
                _ = tree.TryAdd($"mail.{sub}.com", sub);
            }

            var deep = "a";
            for (int i = 0; i < 25; i++)
                deep = $"{deep}.a";
            deep += ".com";

            _ = tree.TryAdd(deep, "deep");
        }

        // SAME workload, different trees
        [Benchmark(Baseline = true)]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void InMemoryDomainTree()
        {
            for (int i = 0; i < N; i++)
                _defaultTree.TryGet(TestDomains[i % TestDomains.Length], out _);
        }

        [Benchmark]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void LmdbBackedDomainTree()
        {
            for (int i = 0; i < N; i++)
                _dbBackedTree.TryGet(TestDomains[i % TestDomains.Length], out _);
        }

        [Benchmark]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void LmdbBackedDomainTree2()
        {
            for (int i = 0; i < N; i++)
                _dbBackedTree2.TryGet(TestDomains[i % TestDomains.Length], out _);
        }


        [Benchmark]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MmapBackedDomainTree()
        {
            for (int i = 0; i < N; i++)
                _mmapBackedTree.TryGet(TestDomains[i % TestDomains.Length], out _);
        }

        [Benchmark]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void MmapBackedDomainTree2()
        {
            for (int i = 0; i < N; i++)
                _mmapBackedTree2.TryGet(TestDomains[i % TestDomains.Length], out _);
        }

        [Benchmark]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void DomainGraph()
        {
            for (int i = 0; i < N; i++)
                _graph.TryGet(TestDomains[i % TestDomains.Length], out _);
        }
    }
}