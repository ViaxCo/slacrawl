package importer

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/slack-go/slack"
)

type Export struct {
	fs       fs.FS
	closer   io.Closer
	zipFiles map[string]*zip.File
}

type ChannelInfo struct {
	ID        string
	Name      string
	Kind      string
	IsPrivate bool
	RawJSON   []byte
}

type MessageEnvelope struct {
	Date string
	Raw  map[string]any
}

func Open(path string) (*Export, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		root, err := os.OpenRoot(path)
		if err != nil {
			return nil, err
		}
		return &Export{fs: root.FS(), closer: root}, nil
	}
	if strings.EqualFold(filepath.Ext(path), ".zip") {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return openZIP(file, info.Size(), file)
	}
	return nil, errors.New("unsupported export path: expected .zip file or directory")
}

func (e *Export) Close() error {
	if e == nil || e.closer == nil {
		return nil
	}
	return e.closer.Close()
}

func (e *Export) Users() ([]slack.User, error) {
	var users []slack.User
	_, err := e.readJSONOptional("users.json", &users)
	if err != nil {
		return nil, err
	}
	if users == nil {
		return []slack.User{}, nil
	}
	return users, nil
}

func (e *Export) catalogs(strict bool) ([]ChannelInfo, bool, error) {
	all := []ChannelInfo{}
	present := false
	for _, role := range []struct {
		name, kind string
		private    bool
	}{
		{"channels.json", "public", false}, {"groups.json", "private", true}, {"dms.json", "im", true}, {"mpims.json", "mpim", true},
	} {
		channels, exists, err := e.readChannelList(role.name, role.kind, role.private, strict)
		if err != nil {
			return nil, false, err
		}
		present = present || exists
		all = append(all, channels...)
	}
	return all, present, nil
}

type channelRecord struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	IsPrivate bool   `json:"is_private"`
}

func (e *Export) readChannelList(fileName, kind string, defaultPrivate, strict bool) ([]ChannelInfo, bool, error) {
	var rows []json.RawMessage
	ok, err := e.readJSONOptional(fileName, &rows)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return []ChannelInfo{}, false, nil
	}

	if strict && rows == nil {
		return nil, true, errors.New("unsupported export catalog: include_dms=false requires a JSON array, not null")
	}
	out := make([]ChannelInfo, 0, len(rows))
	for _, row := range rows {
		if strict {
			if err := validateUniqueJSONKeys(row, true); err != nil {
				return nil, true, err
			}
		}
		var rec channelRecord
		if err := json.Unmarshal(row, &rec); err != nil {
			return nil, true, fmt.Errorf("parse %s record: invalid conversation metadata", fileName)
		}
		if strict && strings.TrimSpace(rec.ID) == "" {
			return nil, true, fmt.Errorf("invalid %s id: missing identity", fileName)
		}
		if rec.ID == "" {
			continue
		}
		if rec.Name == "" {
			rec.Name = rec.ID
		}
		if err := validateChannelDirName(rec.ID); err != nil {
			return nil, true, fmt.Errorf("invalid %s id: invalid channel directory", fileName)
		}
		if err := validateChannelDirName(rec.Name); err != nil {
			return nil, true, fmt.Errorf("invalid %s name: invalid channel directory", fileName)
		}
		isPrivate := defaultPrivate
		if rec.IsPrivate {
			isPrivate = true
		}
		out = append(out, ChannelInfo{
			ID:        rec.ID,
			Name:      rec.Name,
			Kind:      kind,
			IsPrivate: isPrivate,
			RawJSON:   append([]byte(nil), row...),
		})
	}
	return out, true, nil
}

func validateChannelDirName(name string) error {
	if name == "." || !fs.ValidPath(name) || strings.ContainsAny(name, `/\`) {
		return errors.New("invalid channel directory")
	}
	return nil
}

func (e *Export) readJSONOptional(fileName string, out any) (bool, error) {
	blob, err := e.readFile(fileName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("read %s: unreadable export metadata", fileName)
	}
	if err := json.Unmarshal(blob, out); err != nil {
		return false, fmt.Errorf("parse %s: invalid JSON metadata", fileName)
	}
	return true, nil
}

func (e *Export) readFile(name string) ([]byte, error) {
	if e.zipFiles == nil {
		return fs.ReadFile(e.fs, name)
	}
	file, ok := e.zipFiles[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	reader, err := file.Open()
	if err != nil {
		return nil, err
	}
	blob, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil {
		return nil, err
	}
	return blob, closeErr
}

// Normalize exactly the path aliases accepted by archive/zip's fs view, but
// reject duplicate logical entries before selecting any catalog or payload.
func indexZIP(files []*zip.File) (map[string]*zip.File, error) {
	out := map[string]*zip.File{}
	raw := map[string]bool{}
	for _, file := range files {
		name := path.Clean(strings.ReplaceAll(file.Name, "\\", "/"))
		name = strings.TrimPrefix(name, "/")
		for strings.HasPrefix(name, "../") {
			name = strings.TrimPrefix(name, "../")
		}
		if raw[file.Name] || out[name] != nil {
			return nil, errors.New("ambiguous duplicate export ZIP entry")
		}
		if !fs.ValidPath(name) || name == "." {
			return nil, errors.New("invalid export ZIP entry path")
		}
		raw[file.Name] = true
		out[name] = file
	}
	for name := range out {
		for dir := path.Dir(name); dir != "."; dir = path.Dir(dir) {
			if file := out[dir]; file != nil && !file.FileInfo().IsDir() {
				return nil, errors.New("ambiguous export ZIP file/directory entry")
			}
		}
	}
	return out, nil
}
