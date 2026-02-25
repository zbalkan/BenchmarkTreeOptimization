using System;
using System.Collections.Concurrent;

namespace BenchmarkTreeBackends.Backends.Graph
{
    public static class BackendExtensions
    {
        public static ConcurrentBag<string> GetOrAdd(this ConcurrentDictionary<string, ConcurrentBag<string>> dict, string key, Func<ConcurrentBag<string>, ConcurrentBag<string>> factory)
        {
            return dict.GetOrAdd(key, _ => factory(new ConcurrentBag<string>()));
        }
    }
}