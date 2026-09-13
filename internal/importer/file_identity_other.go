//go:build !darwin && !linux

package importer

import "io/fs"

func physicalIdentity(fs.FileInfo) (fileIdentity, bool) { return fileIdentity{}, false }
