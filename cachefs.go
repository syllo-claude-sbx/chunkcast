// cachefs: a caching FUSE filesystem that mirrors a source directory
// and caches file reads in local block storage.
//
// Architecture:
//   source dir (gcsfuse)  →  cachefs (local cache + P2P)  →  consumers (archivefs, etc)
//
// On read:
//   1. Check local cache (block file on disk) → instant
//   2. Read from source dir (gcsfuse/NFS) → cache the block → return
//   3. (Future: check ChunkCast peers before source)
//
// Blocks are stored as files: <cache-dir>/<path-hash>/<block-number>
// Metadata (file size, block count) stored in <cache-dir>/<path-hash>/meta.json
//
// Usage:
//   cachefs --source /gcs/bucket --cache-dir /var/cache/chunkcast --block-size 33554432 /mnt/cached
//
// Then archivefs reads from /mnt/cached instead of /gcs/bucket directly.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// CacheFS is the root FUSE node for the caching filesystem.
type CacheFS struct {
	fs.Inode
	sourceDir string
	cacheDir  string
	blockSize int64
}

// CacheNode represents a file or directory in the cached filesystem.
type CacheNode struct {
	fs.Inode
	sourceDir string
	cacheDir  string
	blockSize int64
	relPath   string // path relative to source root
}

// CacheFileHandle is a file handle that reads from cache or source.
type CacheFileHandle struct {
	node      *CacheNode
	sourceFd  *os.File
	mu        sync.Mutex
	meta      *fileMeta
	blockDir  string
}

type fileMeta struct {
	Size       int64  `json:"size"`
	BlockSize  int64  `json:"block_size"`
	BlockCount int    `json:"block_count"`
	SourcePath string `json:"source_path"`
}

var _ = (fs.NodeLookuper)((*CacheFS)(nil))
var _ = (fs.NodeReaddirer)((*CacheFS)(nil))
var _ = (fs.NodeGetattrer)((*CacheFS)(nil))
var _ = (fs.NodeLookuper)((*CacheNode)(nil))
var _ = (fs.NodeReaddirer)((*CacheNode)(nil))
var _ = (fs.NodeGetattrer)((*CacheNode)(nil))
var _ = (fs.NodeOpener)((*CacheNode)(nil))

func pathHash(p string) string {
	h := sha256.Sum256([]byte(p))
	return hex.EncodeToString(h[:8])
}

// --- CacheFS (root node) ---

func (c *CacheFS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	st := syscall.Stat_t{}
	if err := syscall.Stat(c.sourceDir, &st); err != nil {
		return syscall.ENOENT
	}
	out.FromStat(&st)
	return fs.OK
}

func (c *CacheFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	sourcePath := filepath.Join(c.sourceDir, name)
	st := syscall.Stat_t{}
	if err := syscall.Lstat(sourcePath, &st); err != nil {
		return nil, syscall.ENOENT
	}
	out.FromStat(&st)

	child := &CacheNode{
		sourceDir: c.sourceDir,
		cacheDir:  c.cacheDir,
		blockSize: c.blockSize,
		relPath:   name,
	}

	mode := fuse.S_IFREG
	if st.Mode&syscall.S_IFDIR != 0 {
		mode = fuse.S_IFDIR
	}
	return c.NewInode(ctx, child, fs.StableAttr{Mode: uint32(mode), Ino: st.Ino}), fs.OK
}

func (c *CacheFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return readSourceDir(c.sourceDir)
}

// --- CacheNode ---

func (n *CacheNode) sourcePath() string {
	return filepath.Join(n.sourceDir, n.relPath)
}

func (n *CacheNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	st := syscall.Stat_t{}
	if err := syscall.Lstat(n.sourcePath(), &st); err != nil {
		return syscall.ENOENT
	}
	out.FromStat(&st)
	return fs.OK
}

func (n *CacheNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childRel := filepath.Join(n.relPath, name)
	sourcePath := filepath.Join(n.sourceDir, childRel)
	st := syscall.Stat_t{}
	if err := syscall.Lstat(sourcePath, &st); err != nil {
		return nil, syscall.ENOENT
	}
	out.FromStat(&st)

	child := &CacheNode{
		sourceDir: n.sourceDir,
		cacheDir:  n.cacheDir,
		blockSize: n.blockSize,
		relPath:   childRel,
	}

	mode := fuse.S_IFREG
	if st.Mode&syscall.S_IFDIR != 0 {
		mode = fuse.S_IFDIR
	}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: uint32(mode), Ino: st.Ino}), fs.OK
}

func (n *CacheNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return readSourceDir(n.sourcePath())
}

func (n *CacheNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	sourcePath := n.sourcePath()

	// Get file info for metadata
	info, err := os.Stat(sourcePath)
	if err != nil {
		return nil, 0, syscall.ENOENT
	}

	size := info.Size()
	blockCount := int((size + n.blockSize - 1) / n.blockSize)
	blockDir := filepath.Join(n.cacheDir, pathHash(n.relPath))
	os.MkdirAll(blockDir, 0755)

	meta := &fileMeta{
		Size:       size,
		BlockSize:  n.blockSize,
		BlockCount: blockCount,
		SourcePath: sourcePath,
	}

	// Save metadata
	metaBytes, _ := json.Marshal(meta)
	os.WriteFile(filepath.Join(blockDir, "meta.json"), metaBytes, 0644)

	return &CacheFileHandle{
		node:     n,
		meta:     meta,
		blockDir: blockDir,
	}, fuse.FOPEN_KEEP_CACHE, fs.OK
}

// Read serves data from local cache (hit) or source (miss + cache).
func (f *CacheFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= f.meta.Size {
		return fuse.ReadResultData(nil), fs.OK
	}

	// Calculate which blocks we need
	startBlock := int(off / f.meta.BlockSize)
	endOff := off + int64(len(dest))
	if endOff > f.meta.Size {
		endOff = f.meta.Size
	}
	endBlock := int((endOff - 1) / f.meta.BlockSize)

	// Read and assemble blocks
	var result []byte
	for blockIdx := startBlock; blockIdx <= endBlock; blockIdx++ {
		blockData, err := f.readBlock(blockIdx)
		if err != nil {
			log.Printf("cachefs: read block %d failed: %v", blockIdx, err)
			return nil, syscall.EIO
		}

		blockStart := int64(blockIdx) * f.meta.BlockSize
		// Calculate slice within this block
		sliceStart := int64(0)
		if off > blockStart {
			sliceStart = off - blockStart
		}
		sliceEnd := int64(len(blockData))
		if blockStart+sliceEnd > endOff {
			sliceEnd = endOff - blockStart
		}

		result = append(result, blockData[sliceStart:sliceEnd]...)
	}

	return fuse.ReadResultData(result), fs.OK
}

// readBlock gets a block from cache or source.
func (f *CacheFileHandle) readBlock(blockIdx int) ([]byte, error) {
	blockPath := filepath.Join(f.blockDir, fmt.Sprintf("%d.blk", blockIdx))

	// Try cache first
	if data, err := os.ReadFile(blockPath); err == nil && len(data) > 0 {
		return data, nil
	}

	// Cache miss: read from source
	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check after lock
	if data, err := os.ReadFile(blockPath); err == nil && len(data) > 0 {
		return data, nil
	}

	// Open source if not already open
	if f.sourceFd == nil {
		fd, err := os.Open(f.meta.SourcePath)
		if err != nil {
			return nil, err
		}
		f.sourceFd = fd
	}

	offset := int64(blockIdx) * f.meta.BlockSize
	size := f.meta.BlockSize
	if offset+size > f.meta.Size {
		size = f.meta.Size - offset
	}

	buf := make([]byte, size)
	n, err := f.sourceFd.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	buf = buf[:n]

	// Cache the block
	os.WriteFile(blockPath, buf, 0644)

	return buf, nil
}

func (f *CacheFileHandle) Release(ctx context.Context) syscall.Errno {
	if f.sourceFd != nil {
		f.sourceFd.Close()
	}
	return fs.OK
}

func (f *CacheFileHandle) Flush(ctx context.Context) syscall.Errno {
	return fs.OK
}

// --- helpers ---

func readSourceDir(dir string) (fs.DirStream, syscall.Errno) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, syscall.EIO
	}

	var fuseEntries []fuse.DirEntry
	for _, e := range entries {
		mode := fuse.S_IFREG
		if e.IsDir() {
			mode = fuse.S_IFDIR
		}
		// Skip hidden/special entries
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		fuseEntries = append(fuseEntries, fuse.DirEntry{
			Name: e.Name(),
			Mode: uint32(mode),
		})
	}
	return fs.NewListDirStream(fuseEntries), fs.OK
}
