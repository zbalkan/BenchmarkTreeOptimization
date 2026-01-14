using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;

namespace BenchmarkTreeBackends.Backends.MMAP
{
    internal unsafe sealed class MmapFile : IDisposable
    {
        private readonly MemoryMappedFile _mmf;
        private readonly MemoryMappedViewAccessor _view;
        private readonly long _capacityBytes;

        public MmapHeader* Header;
        public byte* BasePtr;

        // ---------------------------
        // Constructors
        // ---------------------------


        // Main ctor
        public MmapFile(string path, uint nodeCapacity = 8_000_000u, long valueCapacityBytes = 512L * 1024 * 1024)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path must be non-empty.", nameof(path));
            if (nodeCapacity < 2)
                throw new ArgumentOutOfRangeException(nameof(nodeCapacity), "Must be >= 2 (sentinel + root).");
            ArgumentOutOfRangeException.ThrowIfLessThan(valueCapacityBytes, 1024);

            long headerSize = sizeof(MmapHeader);
            long nodeRegionSize = (long)nodeCapacity * sizeof(MmapNode);
            long totalSize = headerSize + nodeRegionSize + valueCapacityBytes;

            _capacityBytes = totalSize;
            var p = Path.GetFullPath(path);
            Directory.CreateDirectory(Path.GetDirectoryName(p)!);

            var fs = new FileStream(
                path,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.ReadWrite);

            if (fs.Length < totalSize)
                fs.SetLength(totalSize);

            _mmf = MemoryMappedFile.CreateFromFile(
                fs,
                mapName: null,
                capacity: totalSize,
                MemoryMappedFileAccess.ReadWrite,
                HandleInheritability.None,
                leaveOpen: false);

            _view = _mmf.CreateViewAccessor(0, totalSize, MemoryMappedFileAccess.ReadWrite);

            byte* basePtr = null;
            _view.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);

            BasePtr = basePtr;
            Header = (MmapHeader*)basePtr;

            // Initialize if new file
            if (Header->Magic != MmapConstants.Magic)
                InitializeNewFile(nodeCapacity, valueCapacityBytes);
        }

        // ---------------------------
        // Layout helpers
        // ---------------------------

        private void InitializeNewFile(uint nodeCapacity, long valueCapacityBytes)
        {
            Unsafe.InitBlockUnaligned(BasePtr, 0, (uint)_capacityBytes);

            Header->Magic = MmapConstants.Magic;
            Header->Version = 1;

            Header->NodeRegionOffset = sizeof(MmapHeader);
            Header->ValueRegionOffset = sizeof(MmapHeader) + (long)nodeCapacity * sizeof(MmapNode);

            Header->NodeCount = 2;   // 0 = sentinel, 1 = root
            Header->ValueCount = 0;
            Header->ValueTail = 0;

            ref var root = ref GetNode(1);
            root.Flags = 0;
            root.ValueOffset = 0;
            root.ValueLength = 0;

            fixed (uint* p = root.Children)
                for (int i = 0; i < 256; i++)
                    p[i] = 0;
        }

        // ---------------------------
        // Accessors
        // ---------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref MmapNode GetNode(uint index)
            => ref ((MmapNode*)(BasePtr + Header->NodeRegionOffset))[index];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> GetValue(long offset, int length)
            => new(BasePtr + Header->ValueRegionOffset + offset + 4, length);

        // ---------------------------
        // Cleanup
        // ---------------------------

        public void Dispose()
        {
            if (BasePtr != null)
                _view.SafeMemoryMappedViewHandle.ReleasePointer();

            _view.Dispose();
            _mmf.Dispose();
        }
    }

    internal static class MmapConstants
    {
        public const uint Magic = 0x4D4D4150; // "MMAP"
    }
}
