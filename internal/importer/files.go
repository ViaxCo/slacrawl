package importer

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"iter"
	"math"
	"path"
	"sort"
	"strings"
)

type fileIdentity struct{ device, inode uint64 }
type messageFile struct {
	name       string
	zip        *zip.File
	identity   fileIdentity
	identified bool
	digest     [32]byte
}

func (f messageFile) baseName() string   { return path.Base(f.name) }
func messageDigest(blob []byte) [32]byte { return sha256.Sum256(blob) }

func (e *Export) reserveLocators(channels []ChannelInfo, strict bool) (map[string][]messageFile, error) {
	owners := map[string]string{}
	for _, channel := range channels {
		for _, name := range []string{channel.Name, channel.ID} {
			if prior, exists := owners[name]; exists && prior != channel.ID {
				return nil, errors.New("export conversation locators have conflicting owners")
			}
			owners[name] = channel.ID
		}
	}
	inventory := map[string][]messageFile{}
	physical := map[fileIdentity]string{}
	reserve := func(info fs.FileInfo, owner string) (fileIdentity, bool, error) {
		identity, known := physicalIdentity(info)
		if !known {
			if strict {
				return identity, false, errors.New("include_dms=false requires directory file identity support; use a workspace JSON ZIP export on this platform")
			}
			return identity, false, nil
		}
		if prior, exists := physical[identity]; exists && prior != owner {
			return identity, true, errors.New("export conversations share physical files or directories")
		}
		physical[identity] = owner
		return identity, true, nil
	}
	if e.zipFiles != nil {
		for logical, file := range e.zipFiles {
			name := path.Dir(logical)
			if _, owned := owners[name]; owned && strings.HasSuffix(logical, ".json") && !file.FileInfo().IsDir() {
				inventory[name] = append(inventory[name], messageFile{name: logical, zip: file})
			}
		}
	}
	for name, owner := range owners {
		if e.zipFiles == nil {
			dir, err := e.fs.Open(name)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, errors.New("cannot inspect export conversation directory")
			}
			info, err := dir.Stat()
			if err != nil {
				_ = dir.Close()
				return nil, errors.New("cannot stat export conversation directory")
			}
			_, _, err = reserve(info, owner)
			reader, ok := dir.(fs.ReadDirFile)
			var entries []fs.DirEntry
			if err == nil && ok && info.IsDir() {
				entries, err = reader.ReadDir(-1)
				if err != nil {
					err = errors.New("cannot list export conversation directory")
				}
			} else if err == nil {
				err = errors.New("invalid export conversation directory")
			}
			closeErr := dir.Close()
			if err != nil {
				return nil, err
			}
			if closeErr != nil {
				return nil, errors.New("cannot close export conversation directory")
			}
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
					continue
				}
				name := path.Join(name, entry.Name())
				file, err := e.fs.Open(name)
				if err != nil {
					return nil, errors.New("cannot inspect export message file")
				}
				info, err := file.Stat()
				if err != nil {
					_ = file.Close()
					return nil, errors.New("cannot stat export message file")
				}
				var identity fileIdentity
				var known bool
				if err == nil && info.Mode().IsRegular() {
					identity, known, err = reserve(info, owner)
				} else if err == nil {
					err = errors.New("export message file is not regular")
				}
				closeErr := file.Close()
				if err != nil {
					return nil, err
				}
				if closeErr != nil {
					return nil, errors.New("cannot close export message file")
				}
				inventory[path.Dir(name)] = append(inventory[path.Dir(name)], messageFile{name: name, identity: identity, identified: known})
			}
		}
		sort.Slice(inventory[name], func(i, j int) bool { return inventory[name][i].name < inventory[name][j].name })
	}
	if e.zipFiles != nil {
		type span struct {
			start, end uint64
			owner      string
		}
		spans := []span{}
		for name, files := range inventory {
			for _, file := range files {
				offset, err := file.zip.DataOffset()
				if err != nil || offset < 0 || file.zip.CompressedSize64 > math.MaxInt64-uint64(offset) {
					return nil, errors.New("invalid export ZIP payload extent")
				}
				spans = append(spans, span{uint64(offset), uint64(offset) + file.zip.CompressedSize64, owners[name]})
			}
		}
		sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
		var active span
		for _, current := range spans {
			if current.start < active.end && current.owner != active.owner {
				return nil, errors.New("export conversations share ZIP payload data")
			}
			if current.end > active.end {
				active = current
			}
		}
	}
	return inventory, nil
}

func (e *Export) readMessageFile(file messageFile) ([]byte, error) {
	var reader io.ReadCloser
	if file.zip != nil {
		var err error
		reader, err = file.zip.Open()
		if err != nil {
			return nil, errors.New("cannot open planned export message file")
		}
	} else {
		opened, err := e.fs.Open(file.name)
		if err != nil {
			return nil, errors.New("planned export message file is missing or unreadable")
		}
		info, err := opened.Stat()
		identity, known := fileIdentity{}, false
		if err == nil {
			identity, known = physicalIdentity(info)
		}
		if err != nil || !info.Mode().IsRegular() || (file.identified && (!known || identity != file.identity)) {
			_ = opened.Close()
			return nil, errors.New("planned export message file identity changed")
		}
		reader = opened
	}
	blob, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil || closeErr != nil {
		return nil, errors.New("cannot read planned export message file")
	}
	return blob, nil
}

func decodeMessages(blob []byte) ([]map[string]any, error) {
	if len(bytes.TrimSpace(blob)) == 0 {
		return nil, nil
	}
	var rows []map[string]any
	if err := json.Unmarshal(blob, &rows); err != nil {
		return nil, errors.New("parse messages file: invalid JSON message array")
	}
	for i, raw := range rows {
		if raw == nil {
			rows[i] = map[string]any{}
		}
	}
	return rows, nil
}

// Messages verifies the opened file and hashes the exact buffer it decodes.
// No path is reopened between the integrity check and row delivery.
func (p *Prepared) Messages(index int) iter.Seq2[MessageEnvelope, error] {
	return func(yield func(MessageEnvelope, error) bool) {
		if !p.scanned {
			yield(MessageEnvelope{}, errors.New("export bodies have not been prepared"))
			return
		}
		for _, file := range p.selected[index] {
			blob, err := p.export.readMessageFile(file)
			if err != nil {
				yield(MessageEnvelope{}, err)
				return
			}
			if messageDigest(blob) != file.digest {
				yield(MessageEnvelope{}, errors.New("planned export message file changed; prepare the import again"))
				return
			}
			rows, err := decodeMessages(blob)
			if err != nil {
				yield(MessageEnvelope{}, err)
				return
			}
			for _, raw := range rows {
				if !yield(MessageEnvelope{Date: strings.TrimSuffix(file.baseName(), ".json"), Raw: raw}, nil) {
					return
				}
			}
		}
	}
}
