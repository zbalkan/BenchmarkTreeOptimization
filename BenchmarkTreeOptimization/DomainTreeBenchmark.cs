using BenchmarkDotNet.Attributes;

namespace BenchmarkTreeOptimization
{
    [MemoryDiagnoser]
    public class DomainTreeBenchmark
    {
        private DefaultDomainTree<string> _defaultTree;
        private OptimizedDomainTree<string> _optimizedTree;

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
            _defaultTree = new DefaultDomainTree<string>();
            _optimizedTree = new OptimizedDomainTree<string>();

            LoadRealisticTree(_defaultTree);
            LoadRealisticTree(_optimizedTree);

            // Warm-up
            foreach (var d in TestDomains)
            {
                _defaultTree.TryGet(d, out var a);
                _optimizedTree.TryGet(d, out var b);

                if (!Equals(a, b))
                    throw new InvalidOperationException($"Mismatch for {d}");
            }
        }

        private void LoadRealisticTree(ByteTree<string,string> tree)
        {
            tree.Add("com", "com-root");
            tree.Add("org", "org-root");

            var subs = new[] { "google", "microsoft", "github", "example" };
            foreach (var sub in subs)
            {
                tree.Add($"{sub}.com", sub);
                tree.Add($"www.{sub}.com", sub);
                tree.Add($"api.{sub}.com", sub);
                tree.Add($"mail.{sub}.com", sub);
            }

            var deep = "a";
            for (int i = 0; i < 25; i++)
                deep = $"{deep}.a";
            deep += ".com";

            tree.Add(deep, "deep");
        }

        // SAME workload, different trees
        [Benchmark(Baseline = true)]
        public void DefaultDomainTree()
        {
            for (int i = 0; i < N; i++)
                _defaultTree.TryGet(TestDomains[i % 10], out _);
        }

        [Benchmark]
        public void OptimizedDomainTree()
        {
            for (int i = 0; i < N; i++)
                _optimizedTree.TryGet(TestDomains[i % 10], out _);
        }
    }
}