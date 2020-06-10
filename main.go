package main

// #include <stdlib.h>
// #include <linux/fs.h>
// void file_dedupe_set_info(struct file_dedupe_range* arg, int i,
//     struct file_dedupe_range_info *info) {
//   arg->info[i] = *info;
// }
// void file_dedupe_get_info(struct file_dedupe_range* arg, int i,
//     struct file_dedupe_range_info *info) {
//   *info = arg->info[i];
// }
import "C"

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
	"syscall"
	"time"
	"unsafe"

	cli "github.com/urfave/cli/v2"
)

// parse the output of the 'fdupes' program
func parseFdupesFile(infi string) ([][]string, error) {
	fi, err := os.Open(infi)
	if err != nil {
		return nil, err
	}

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

func main() {
	app := cli.NewApp()
	app.Commands = []*cli.Command{
		runDedupeCmd,
		findDupesCmd,
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

		for _, batch := range input {
			fmt.Println("Deduplicating: ", batch[0])
			for _, o := range batch[1:] {
				fmt.Printf("\t%s\n", o)
			}
			if err := DedupeFiles(batch); err != nil {
				return err
			}
		}

		return nil
	},
}

var findDupesCmd = &cli.Command{
	Name:  "find-dupes",
	Usage: "find duplicate files in the given directory",
	Action: func(cctx *cli.Context) error {
		var filelist []string

		walkStart := time.Now()
		root := cctx.Args().First()
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
		if err != nil {
			return err
		}
		log.Printf("Enumerating files took %s", time.Since(walkStart))

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

func hashFile(f string) ([]byte, error) {
	h := sha256.New()

	fi, err := os.Open(f)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(h, fi)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func DedupeFiles(fis []string) error {
	f1, err := os.Open(fis[0])
	if err != nil {
		return err
	}
	defer f1.Close()

	f1s, err := f1.Stat()
	if err != nil {
		return err
	}

	size := C.sizeof_struct_file_dedupe_range
	size += C.sizeof_struct_file_dedupe_range_info * (len(fis) - 1)
	arg := (*C.struct_file_dedupe_range)(C.malloc(C.ulong(size)))
	defer C.free(unsafe.Pointer(arg))

	arg.src_length = C.ulonglong(f1s.Size())
	arg.src_offset = 0
	arg.dest_count = C.ushort(len(fis) - 1)

	for i := 0; i < len(fis)-1; i++ {
		destfi, err := os.Open(fis[i+1])
		if err != nil {
			return err
		}
		defer destfi.Close()

		var info C.struct_file_dedupe_range_info
		info.dest_fd = C.longlong(destfi.Fd())
		info.dest_offset = 0
		C.file_dedupe_set_info(arg, C.int(i), &info)
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f1.Fd(), C.FIDEDUPERANGE, uintptr(unsafe.Pointer(arg)))
	if errno != 0 {
		return errno
	}

	for i := 0; i < int(arg.dest_count); i++ {
		var info C.struct_file_dedupe_range_info
		C.file_dedupe_get_info(arg, C.int(i), &info)
		fmt.Println("resp status: ", info.status)
		fmt.Println("resp deduped: ", info.bytes_deduped)
	}
	return nil
}
