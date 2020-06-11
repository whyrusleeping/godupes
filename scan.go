package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	chunker "github.com/ipfs/go-ipfs-chunker"
)

type fileChunk struct {
	fileid int
	offset int64
	length int64
	sum    uint64
}

func ScanFiles(filelist []string) error {
	var wait sync.WaitGroup

	lookup := make(map[uint64][]fileChunk)
	var lookupLk sync.Mutex

	jobs := make(chan int)

	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for fid := range jobs {
				file := filelist[fid]
				chunks, err := scanFile(fid, file)
				if err != nil {
					log.Printf("failed to hash file %q: %s", file, err)
				}

				lookupLk.Lock()
				for _, chunk := range chunks {
					lookup[chunk.sum] = append(lookup[chunk.sum], chunk)
				}
				lookupLk.Unlock()
				wait.Done()
			}

		}()
	}

	hashStart := time.Now()
	for f := range filelist {
		wait.Add(1)
		jobs <- f
	}
	close(jobs)

	wait.Wait()
	log.Printf("Hashing data took %s", time.Since(hashStart))

	for _, dupes := range lookup {
		if len(dupes) <= 1 {
			continue
		}
		for _, dupe := range dupes {
			fmt.Printf("%s %d:%d\n", filelist[dupe.fileid],
				dupe.offset, dupe.length)
		}
		fmt.Println()
	}

	return nil
}

func scanFile(fid int, f string) ([]fileChunk, error) {
	h := fnv.New64a()

	fi, err := os.Open(f)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	chunks := []fileChunk{}

	splitter := chunker.NewSizeSplitter(fi, 8192)
	offset := int64(0)
	for i := 0; ; i++ {
		bytes, err := splitter.NextBytes()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		h.Reset()
		_, err = h.Write(bytes)
		if err != nil {
			return nil, err
		}
		chunk := fileChunk{
			fileid: fid,
			offset: offset,
			length: int64(len(bytes)),
			sum:    h.Sum64(),
		}
		offset += chunk.length
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}
