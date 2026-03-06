// chunkcast-fuse: caching FUSE filesystem with P2P chunk distribution.
//
// Mirrors a source directory (e.g., gcsfuse mount) and caches file reads
// locally as blocks. On cache miss, reads from source and caches.
// Shares cached blocks with peer nodes via HTTP (ChunkCast protocol).
//
// Usage:
//   chunkcast-fuse --source /gcs/bucket --cache-dir /var/cache --block-size 33554432 /mnt/cached
//
// Then point archivefs at /mnt/cached instead of /gcs/bucket:
//   archivefs --root /mnt/cached --transparent /mnt/archive
//
// Reads go: app → archivefs → chunkcast-fuse → local cache → [peer | gcsfuse → GCS]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func main() {
	sourceDir := flag.String("source", "", "Source directory to mirror and cache (e.g., /gcs/bucket)")
	cacheDir := flag.String("cache-dir", "/var/cache/chunkcast", "Local cache directory for blocks")
	blockSize := flag.Int64("block-size", 32*1024*1024, "Block size in bytes (default 32MB)")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	if flag.NArg() != 1 || *sourceDir == "" {
		fmt.Fprintf(os.Stderr, "Usage: chunkcast-fuse --source <dir> [--cache-dir <dir>] [--block-size <bytes>] <mountpoint>\n")
		flag.PrintDefaults()
		os.Exit(1)
	}
	mountpoint := flag.Arg(0)

	// Verify source exists
	if _, err := os.Stat(*sourceDir); err != nil {
		log.Fatalf("Source directory not accessible: %s (%v)", *sourceDir, err)
	}

	// Create cache dir
	if err := os.MkdirAll(*cacheDir, 0755); err != nil {
		log.Fatalf("Cannot create cache dir: %s (%v)", *cacheDir, err)
	}

	root := &CacheFS{
		sourceDir: *sourceDir,
		cacheDir:  *cacheDir,
		blockSize: *blockSize,
	}

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: true,
			FsName:     "chunkcast",
			Name:       "chunkcast",
			Debug:      *debug,
			MaxReadAhead: 1024 * 1024, // 1MB read-ahead
		},
	}

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		log.Fatalf("Mount failed: %v", err)
	}

	log.Printf("chunkcast-fuse mounted: %s → %s (cache: %s, block: %dMB)",
		*sourceDir, mountpoint, *cacheDir, *blockSize/(1024*1024))

	// Signal handling
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Unmounting...")
		cancel()
		server.Unmount()
	}()
	_ = ctx

	server.Wait()
}
