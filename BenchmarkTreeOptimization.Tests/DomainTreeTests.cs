using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace BenchmarkTreeOptimization.Tests;

[TestClass]
public class DomainTreeTests
{
    private DefaultDomainTree<string> _defaultTree;
    private OptimizedDomainTree<string> _optimizedTree;

    [TestInitialize]
    public void Setup()
    {
        _defaultTree = new();
        _optimizedTree = new();
    }

    [TestMethod]
    [DataRow("")]
    [DataRow("google.com")]
    [DataRow("www.sub.domain.org")]
    [DataRow("*.wildcard.net")]
    [DataRow("*.*.wildcard.net")]
    [DataRow("my-server_1.internal")]
    [DataRow("a.b.c.d.e.f.g")]
    public void Parity_ValidDomains_Match(string domain)
    {
        string value = $"data-{domain}";

        // Test TryAdd parity
        bool addedDef = _defaultTree.TryAdd(domain, value);
        bool addedOpt = _optimizedTree.TryAdd(domain, value);
        Assert.AreEqual(addedDef, addedOpt, $"TryAdd mismatch for: {domain}");

        // Test TryGet parity
        bool foundDef = _defaultTree.TryGet(domain, out var valDef);
        bool foundOpt = _optimizedTree.TryGet(domain, out var valOpt);
        Assert.AreEqual(foundDef, foundOpt, $"TryGet mismatch for: {domain}");
        Assert.AreEqual(valDef, valOpt, $"Value mismatch for: {domain}");

        // Test TryRemove parity
        bool removedDef = _defaultTree.TryRemove(domain, out _);
        bool removedOpt = _optimizedTree.TryRemove(domain, out _);
        Assert.AreEqual(removedDef, removedOpt, $"TryRemove mismatch for: {domain}");
        Assert.AreEqual(_defaultTree.IsEmpty, _optimizedTree.IsEmpty, "Trees should have identical IsEmpty state");
    }

    [TestMethod]
    [DataRow(null, "null")]
    public void Parity_NullDomain_BothThrow(string domain, string expectedReason)
    {
        // Default Tree
        var exDef = Assert.ThrowsExactly<NullReferenceException>(() =>
            _defaultTree.ConvertToByteKey(domain!, true), "Default should fail");
        // Optimized Tree
        var exOpt = Assert.ThrowsExactly<NullReferenceException>(() =>
            _optimizedTree.ConvertToByteKey(domain!, true), "Optimized should fail");
        // Ensure both caught the same logical error
        Assert.IsNotNull(exDef);
        Assert.IsNotNull(exOpt);
    }


    [TestMethod]
    [DataRow("domain..com", "label length")]
    [DataRow(".prefix.com", "label length")]
    [DataRow("suffix.com.", "label length")]
    [DataRow("-prefix.com", "hyphen")]
    [DataRow("suffix-.com", "hyphen")]
    [DataRow("this-label-is-exactly-sixty-four-characters-long-and-should-fail.com", "63 bytes")]
    public void Parity_InvalidDomains_BothThrow(string domain, string expectedReason)
    {
        // Default Tree
        var exDef = Assert.ThrowsExactly<InvalidDomainNameException>(() =>
            _defaultTree.ConvertToByteKey(domain, true), $"Default should fail: {expectedReason}");

        // Optimized Tree
        var exOpt = Assert.ThrowsExactly<InvalidDomainNameException>(() =>
            _optimizedTree.ConvertToByteKey(domain, true), $"Optimized should fail: {expectedReason}");

        // Ensure both caught the same logical error
        Assert.IsNotNull(exDef);
        Assert.IsNotNull(exOpt);
    }

    [TestMethod]
    public void Parity_DeepHierarchy_Cleanup()
    {
        string deep = "very.deep.sub.domain.structure.com";
        _defaultTree.Add(deep, "test");
        _optimizedTree.Add(deep, "test");

        _defaultTree.TryRemove(deep, out _);
        _optimizedTree.TryRemove(deep, out _);

        // This ensures CleanThisBranch works identically in both
        Assert.IsTrue(_defaultTree.IsEmpty, "Default tree failed to clean branch");
        Assert.IsTrue(_optimizedTree.IsEmpty, "Optimized tree failed to clean branch");
    }
}