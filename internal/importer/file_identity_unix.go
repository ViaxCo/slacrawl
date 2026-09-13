//go:build darwin || linux

package importer

import (
	"io/fs"
	"syscall"
)

func physicalIdentity(info fs.FileInfo) (fileIdentity, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, false
	}
	return fileIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}, true
}
