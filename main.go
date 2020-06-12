package main

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	cli "github.com/urfave/cli/v2"
)

// parse the output of the 'fdupes' program
func parseFdupesFile(infi string) ([][]string, error) {
	fi, err := os.Open(infi)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	var out [][]string
	var cur []string
	scan := bufio.NewScanner(fi)
	for scan.Scan() {
		if len(scan.Text()) == 0 {
			if len(cur) > 0 {
				out = append(out, cur)
				cur = nil
			}
		} else {
			cur = append(cur, scan.Text())
		}
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}

	return out, nil
}

func parseScanLine(line string) (string, int64, int64) {
	var (
		file   string
		offset int64
		length int64
	)
	fmt.Sscanf(line, "%s %d:%d", &file, &offset, &length)
	return file, offset, length
}

func parseScanFile(infi string) ([]string, [][]fileChunk, error) {
	fi, err := os.Open(infi)
	if err != nil {
		return nil, nil, err
	}
	defer fi.Close()

	filelist := []string{}
	filemap := make(map[string]int)

	var out [][]fileChunk
	var cur []fileChunk
	scan := bufio.NewScanner(fi)
	for scan.Scan() {
		if len(scan.Text()) == 0 {
			if len(cur) > 0 {
				out = append(out, cur)
				cur = nil
			}
		} else {
			file, offset, length := parseScanLine(scan.Text())
			id, ok := filemap[file]
			if !ok {
				id = len(filelist)
				filelist = append(filelist, file)
				filemap[file] = id
			}
			cur = append(cur, fileChunk{
				fileid: id,
				offset: offset,
				length: length,
			})
		}
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}

	return filelist, out, nil
}

func main() {
	app := cli.NewApp()
	app.Commands = []*cli.Command{
		runDedupeCmd,
		findDupesCmd,
		findChunkDupesCmd,
		runChunkDedupeCmd,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}
}

var runDedupeCmd = &cli.Command{
	Name: "run-dedupe",
	Action: func(cctx *cli.Context) error {
		input, err := parseFdupesFile(cctx.Args().First())
		if err != nil {
			return err
		}
		total := 0
		for _, batch := range input {
			fmt.Println("Deduplicating: ", batch[0])
			for _, o := range batch[1:] {
				fmt.Printf("\t%s\n", o)
			}
			bytes, err := DedupeFiles(batch)
			if err != nil {
				return err
			}
			total += bytes
		}

		fmt.Printf("Deduped: %d\n", total)
		return nil
	},
}

var runChunkDedupeCmd = &cli.Command{
	Name: "run-chunk-dedupe",
	Action: func(cctx *cli.Context) error {
		files, dupes, err := parseScanFile(cctx.Args().First())
		if err != nil {
			return err
		}

		total := 0
		for _, chunks := range dupes {
			bytes, err := dedupeChunks(files, chunks)
			if err != nil {
				return err
			}
			total += bytes
		}

		fmt.Printf("Deduped: %d\n", total)
		return nil
	},
}

var findDupesCmd = &cli.Command{
	Name:  "find-dupes",
	Usage: "find duplicate files in the given directory",
	Action: func(cctx *cli.Context) error {
		filelist, err := walkPath(cctx.Args().First())
		if err != nil {
			return err
		}
		// TODO: caching the entire list of files on disk might be useful. On
		// large systems this takes a long time to generate

		lookup := make(map[string][]string)
		var lookupLk sync.Mutex
		var wait sync.WaitGroup

		jobs := make(chan string)

		for i := 0; i < runtime.NumCPU(); i++ {
			go func() {
				for f := range jobs {
					hkey, err := hashFile(f)
					if err != nil {
						log.Printf("failed to hash file %q: %s", f, err)
					}

					hks := string(hkey)
					lookupLk.Lock()
					lookup[hks] = append(lookup[hks], f)
					lookupLk.Unlock()
					wait.Done()
				}

			}()
		}

		hashStart := time.Now()
		for _, f := range filelist {
			wait.Add(1)
			jobs <- f
		}
		close(jobs)

		wait.Wait()

		log.Printf("Hashing data took %s", time.Since(hashStart))

		for _, matches := range lookup {
			if len(matches) > 1 {
				for _, m := range matches {
					absp, err := filepath.Abs(m)
					if err != nil {
						return fmt.Errorf("failed to get absolute path for %q: %w", m, err)
					}

					fmt.Println(absp)
				}
				fmt.Println()
			}
		}

		return nil
	},
}

var findChunkDupesCmd = &cli.Command{
	Name:  "find-chunk-dupes",
	Usage: "find duplicate chunks in the given directory",
	Action: func(cctx *cli.Context) error {
		filelist, err := walkPath(cctx.Args().First())
		if err != nil {
			return err
		}
		return ScanFiles(filelist)
	},
}

func hashFile(f string) ([]byte, error) {
	h := sha256.New()

	fi, err := os.Open(f)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	_, err = io.Copy(h, fi)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func walkPath(root string) ([]string, error) {
	var filelist []string
	walkStart := time.Now()
	// TODO: walk isnt super efficient, and doesnt let us do anything in parallel
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("walk function errored at %q: %w", p, err)
		}
		if info.Mode().IsRegular() {
			filelist = append(filelist, p)
		}
		return nil
	})
	log.Printf("Enumerating files took %s", time.Since(walkStart))
	return filelist, err
}
