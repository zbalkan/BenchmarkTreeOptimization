using BenchmarkTreeBackends.Backends.LMDB;
using BenchmarkTreeBackends.Backends.Memory;
using BenchmarkTreeBackends.Backends.MMAP;
using BenchmarkTreeBackends.Codecs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace BenchmarkTreeBackends.Tests;

[TestClass]
public class DomainTreeTests
{
    private DomainTree<string> _defaultTree;
    private DatabaseBackedDomainTree<string> _databaseBackedTree;
    private MmapBackedDomainTree<string> _mmapBackedTree;

    [TestInitialize]
    public void Setup()
    {
        _defaultTree = new DomainTree<string>();
        _databaseBackedTree = new DatabaseBackedDomainTree<string>("treetest", new MessagePackCodec<string>());
        _mmapBackedTree = new MmapBackedDomainTree<string>("treetest_mmap", new MessagePackCodec<string>());
    }

    [TestCleanup]
    public void Cleanup()
    {
        _databaseBackedTree.Dispose();
        if (System.IO.Directory.Exists("treetest"))
        {
            System.IO.Directory.Delete("treetest", true);
        }

        _mmapBackedTree.Dispose();
        if (System.IO.Directory.Exists("treetest_mmap"))
        {
            System.IO.Directory.Delete("treetest_mmap", true);
        }
    }

    [TestMethod]
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
        bool addedBaseline = _defaultTree.TryAdd(domain, value);
        bool addedLmdb = _databaseBackedTree.TryAdd(domain, value);
        bool addedMmap = _mmapBackedTree.TryAdd(domain, value);
        Assert.AreEqual(addedBaseline, addedLmdb, $"TryAdd mismatch for: {domain}");
        Assert.AreEqual(addedBaseline, addedMmap, $"TryAdd mismatch for: {domain}");

        // Test TryGet parity
        bool foundBaseline = _defaultTree.TryGet(domain, out var valBaseline);
        bool foundLmdb = _databaseBackedTree.TryGet(domain, out var valLmdb);
        bool foundMmap = _mmapBackedTree.TryGet(domain, out var valMmap);
        Assert.AreEqual(foundBaseline, foundLmdb, $"TryGet mismatch for: {domain}");
        Assert.AreEqual(valBaseline, valLmdb, $"Value mismatch for: {domain}");
        Assert.AreEqual(foundBaseline, foundMmap, $"TryGet mismatch for: {domain}");
        Assert.AreEqual(valBaseline, valMmap, $"Value mismatch for: {domain}");

        // Test TryRemove parity
        bool removedBaseline = _defaultTree.TryRemove(domain, out _);
        bool removedLmdb = _databaseBackedTree.TryRemove(domain, out _);
        bool removedMmap = _mmapBackedTree.TryRemove(domain, out _);
        Assert.AreEqual(removedBaseline, removedLmdb, $"TryRemove mismatch for: {domain}");
        Assert.AreEqual(_defaultTree.IsEmpty, _databaseBackedTree.IsEmpty, "Trees should have identical IsEmpty state");
        Assert.AreEqual(removedBaseline, removedMmap, $"TryRemove mismatch for: {domain}");
        Assert.AreEqual(_defaultTree.IsEmpty, _mmapBackedTree.IsEmpty, "Trees should have identical IsEmpty state");
    }

    [TestMethod]
    [DataRow("", "empty string = root")]
    public void Parity_EmptyDomain_BothAccept(string domain, string expectedReason)
    {
        string value = $"data-{domain}";

        bool addedBaseline = _defaultTree.TryAdd(domain, value);
        bool addedLmdb = _databaseBackedTree.TryAdd(domain, value);
        bool addedMmap = _mmapBackedTree.TryAdd(domain, value);
        Assert.AreEqual(addedBaseline, addedLmdb, $"TryAdd mismatch for: {domain} for LMDB backend.");
        Assert.AreEqual(addedBaseline, addedMmap, $"TryAdd mismatch for: {domain} for MMAP backend");
    }

    [TestMethod]
    [DataRow(null, "null")]
    public void Parity_NullDomain_BothThrow(string domain, string expectedReason)
    {
        // Default Tree
        var exBaseline = Assert.ThrowsExactly<ArgumentNullException>(() =>
            _defaultTree.ConvertToByteKey(domain, true), "Default should fail");
        // Lmdb-backed Tree
        var exLmdb = Assert.ThrowsExactly<ArgumentNullException>(() =>
            _databaseBackedTree.ConvertToByteKey(domain, true), "Lmdb-backed should fail");
        // MMAP-backed Tree
        var exMmap = Assert.ThrowsExactly<ArgumentNullException>(() =>
            _mmapBackedTree.ConvertToByteKey(domain, true), "Mmap-backed should fail");

        Assert.IsNotNull(exBaseline);
        Assert.IsNotNull(exLmdb);
        Assert.IsNotNull(exMmap);
    }

    [TestMethod]
    [DataRow(null, "null")]
    public void Parity_NullDomain_BothFailSilently(string domain, string expectedReason)
    {
        // Default Tree
        var exBaseline = _defaultTree.ConvertToByteKey(domain, false);
        // Lmdb-backed Tree
        var exLmdb = _databaseBackedTree.ConvertToByteKey(domain, false);
        // MMAP-backed Tree
        var exMmap = _mmapBackedTree.ConvertToByteKey(domain, false);

        // Ensure all caught the same logical error
        Assert.IsNull(exBaseline);
        Assert.IsNull(exLmdb);
        Assert.IsNull(exMmap);
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
        var exBaseline = Assert.ThrowsExactly<InvalidDomainNameException>(() =>
            _defaultTree.ConvertToByteKey(domain, true), $"Default should fail: {expectedReason}");

        // Lmdb-backed Tree
        var exLmdb = Assert.ThrowsExactly<InvalidDomainNameException>(() =>
            _databaseBackedTree.ConvertToByteKey(domain, true), $"Lmdb-backed should fail: {expectedReason}");
        // MMAP-backed Tree
        var exMmap = Assert.ThrowsExactly<InvalidDomainNameException>(() =>
            _mmapBackedTree.ConvertToByteKey(domain, true), $"Mmap-backed should fail: {expectedReason}");

        // Ensure all caught the same logical error
        Assert.IsNotNull(exBaseline);
        Assert.IsNotNull(exLmdb);
        Assert.IsNotNull(exMmap);
    }

    [TestMethod]
    public void Parity_DeepHierarchy_Cleanup()
    {
        string deep = "very.deep.sub.domain.structure.com";
        _defaultTree.Add(deep, "test");
        _databaseBackedTree.Add(deep, "test");
        _mmapBackedTree.Add(deep, "test");

        _defaultTree.TryRemove(deep, out _);
        _databaseBackedTree.TryRemove(deep, out _);
        _mmapBackedTree.TryRemove(deep, out _);

        // This ensures CleanThisBranch works identically in both
        Assert.IsTrue(_defaultTree.IsEmpty, "Default tree failed to clean branch");
        Assert.IsTrue(_databaseBackedTree.IsEmpty, "Lmdb-backed tree failed to clean branch");
        Assert.IsTrue(_mmapBackedTree.IsEmpty, "Mmap-backed tree failed to clean branch");
    }
}