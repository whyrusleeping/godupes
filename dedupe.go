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
	"os"
	"syscall"
	"unsafe"
)

func DedupeFiles(fis []string) (int, error) {
	f1, err := os.Open(fis[0])
	if err != nil {
		return 0, err
	}
	defer f1.Close()

	f1s, err := f1.Stat()
	if err != nil {
		return 0, err
	}

	size := C.sizeof_struct_file_dedupe_range
	size += C.sizeof_struct_file_dedupe_range_info * (len(fis) - 1)
	arg := (*C.struct_file_dedupe_range)(C.malloc(C.ulong(size)))
	defer C.free(unsafe.Pointer(arg))

	arg.src_length = C.ulonglong(f1s.Size())
	arg.src_offset = 0
	arg.dest_count = C.ushort(len(fis) - 1)

	for i := 1; i < len(fis)-1; i++ {
		destfi, err := os.Open(fis[i])
		if err != nil {
			return 0, err
		}
		defer destfi.Close()

		var info C.struct_file_dedupe_range_info
		info.dest_fd = C.longlong(destfi.Fd())
		info.dest_offset = 0
		C.file_dedupe_set_info(arg, C.int(i), &info)
	}

	return dedupeExtents(f1.Fd(), arg)
}

func dedupeChunks(filenames []string, chunks []fileChunk) (int, error) {
	f1, err := os.Open(filenames[chunks[0].fileid])
	if err != nil {
		return 0, err
	}
	defer f1.Close()

	size := C.sizeof_struct_file_dedupe_range
	size += C.sizeof_struct_file_dedupe_range_info * (len(chunks) - 1)
	arg := (*C.struct_file_dedupe_range)(C.malloc(C.ulong(size)))
	defer C.free(unsafe.Pointer(arg))

	arg.src_length = C.ulonglong(chunks[0].length)
	arg.src_offset = C.ulonglong(chunks[0].offset)
	arg.dest_count = C.ushort(len(chunks) - 1)
	for i := 1; i < len(chunks); i++ {
		destfi, err := os.Open(filenames[chunks[i].fileid])
		if err != nil {
			return 0, err
		}
		defer destfi.Close()

		var info C.struct_file_dedupe_range_info
		info.dest_fd = C.longlong(destfi.Fd())
		info.dest_offset = C.ulonglong(chunks[i].offset)
		C.file_dedupe_set_info(arg, C.int(i-1), &info)
	}

	return dedupeExtents(f1.Fd(), arg)
}

func dedupeExtents(fd uintptr, arg *C.struct_file_dedupe_range) (int, error) {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd,
		C.FIDEDUPERANGE, uintptr(unsafe.Pointer(arg)))
	if errno != 0 {
		return 0, errno
	}
	totalBytesDeduped := 0
	for i := C.int(0); i < C.int(arg.dest_count); i++ {
		var info C.struct_file_dedupe_range_info
		C.file_dedupe_get_info(arg, i, &info)
		totalBytesDeduped += int(info.bytes_deduped)
	}
	return totalBytesDeduped, nil
}
