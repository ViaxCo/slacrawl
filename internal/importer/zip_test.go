package importer

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/stretchr/testify/require"
)

func TestZIPHeaderSnapshotPreventsReadRedirection(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	files := [][2]string{
		{"channels.json", `[{"id":"C1","name":"room"}]`},
		{"dms.json", `[{"id":"D1","name":"direct"}]`},
		{"room/1.json", `[{"ts":"1","text":"safe"}]`},
		{"direct/1.json", `[{"ts":"1","text":"leak"}]`},
	}
	// Store entries without data descriptors. A zero CRC is legal to the
	// standard reader here, so a redirect would reach valid attacker JSON.
	for _, entry := range files {
		writer, err := zw.CreateRaw(&zip.FileHeader{Name: entry[0], Method: zip.Store, CompressedSize64: uint64(len(entry[1])), UncompressedSize64: uint64(len(entry[1]))})
		require.NoError(t, err)
		_, err = io.WriteString(writer, entry[1])
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	original := append([]byte(nil), buf.Bytes()...)
	for _, when := range []string{"before-prepare", "before-scan", "before-write"} {
		t.Run(when, func(t *testing.T) {
			blob := append([]byte(nil), original...)
			reader := bytes.NewReader(blob)
			ex, err := openZIP(reader, int64(len(blob)), io.NopCloser(bytes.NewReader(nil)))
			require.NoError(t, err)
			defer ex.Close()
			public := ex.zipFiles["room/1.json"]
			direct := ex.zipFiles["direct/1.json"]
			publicOffset, err := public.DataOffset()
			require.NoError(t, err)
			directOffset, err := direct.DataOffset()
			require.NoError(t, err)
			header := publicOffset - 30 - int64(len(public.Name))
			redirect := func() { binary.LittleEndian.PutUint16(blob[header+28:], uint16(directOffset-publicOffset)) }
			if when == "before-prepare" {
				redirect()
			}
			plan, err := ex.Prepare("T1", admission.Exclude)
			require.NoError(t, err)
			if when == "before-scan" {
				redirect()
			}
			require.NoError(t, plan.Scan(context.Background(), func(_ ChannelInfo, env MessageEnvelope) error { require.Equal(t, "safe", env.Raw["text"]); return nil }))
			if when == "before-write" {
				redirect()
			}
			offset, err := public.DataOffset()
			require.NoError(t, err)
			require.Equal(t, publicOffset, offset)
			rows := 0
			for env, err := range plan.Messages(0) {
				require.NoError(t, err)
				require.Equal(t, "safe", env.Raw["text"])
				rows++
			}
			require.Equal(t, 1, rows)
		})
	}
}

func TestHeaderReaderOverlappingReadsUseFrozenBytes(t *testing.T) {
	data := []byte("abcdefghijklmnop")
	reader := &headerReader{source: bytes.NewReader(data), captured: map[int64]byte{}}
	first := make([]byte, 4)
	_, err := reader.ReadAt(first, 4)
	require.NoError(t, err)
	copy(data[4:8], []byte("XXXX"))
	overlap := make([]byte, 8)
	_, err = reader.ReadAt(overlap, 2)
	require.NoError(t, err)
	require.Equal(t, "cdefghij", string(overlap))
	reader.freeze()
	copy(data, bytes.Repeat([]byte("Z"), len(data)))
	all := make([]byte, 12)
	_, err = reader.ReadAt(all, 0)
	require.NoError(t, err)
	require.Equal(t, "ZZcdefghijZZ", string(all))
	short := make([]byte, 4)
	n, err := reader.ReadAt(short, 14)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 2, n)
}

type failingMetadataFS struct {
	fs.FS
	mode string
}

func (f failingMetadataFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	if name == "direct" && (f.mode == "dir-stat" || f.mode == "readdir") || name == "direct/1.json" && f.mode == "file-stat" {
		return failingMetadataFile{File: file, mode: f.mode}, nil
	}
	return file, nil
}

type failingMetadataFile struct {
	fs.File
	mode string
}

func (f failingMetadataFile) Stat() (fs.FileInfo, error) {
	if f.mode != "readdir" {
		return nil, &fs.PathError{Op: "stat", Path: "excluded-dm-path-canary", Err: errors.New("private-detail-canary")}
	}
	return f.File.Stat()
}
func (f failingMetadataFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.mode == "readdir" {
		return nil, &fs.PathError{Op: "readdir", Path: "excluded-dm-path-canary", Err: errors.New("private-detail-canary")}
	}
	return f.File.(fs.ReadDirFile).ReadDir(n)
}
func TestExcludedLocatorMetadataErrorsStayPrivate(t *testing.T) {
	for _, mode := range []string{"dir-stat", "readdir", "file-stat"} {
		t.Run(mode, func(t *testing.T) {
			root := writeFixtureDir(t, map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`, "dms.json": `[{"id":"D1","name":"direct"}]`, "direct/1.json": "excluded"})
			ex, err := Open(root)
			require.NoError(t, err)
			defer ex.Close()
			ex.fs = failingMetadataFS{FS: ex.fs, mode: mode}
			_, err = ex.Prepare("T1", admission.Exclude)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "canary")
		})
	}
}

func TestPreparedZIPDetectsPayloadChange(t *testing.T) {
	path := writeFixtureZIP(t, map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`, "room/1.json": `[{"ts":"1","text":"safe"}]`})
	ex, err := Open(path)
	require.NoError(t, err)
	defer ex.Close()
	plan, err := ex.Prepare("T1", admission.Exclude)
	require.NoError(t, err)
	require.NoError(t, plan.Scan(context.Background(), func(ChannelInfo, MessageEnvelope) error { return nil }))
	offset, err := ex.zipFiles["room/1.json"].DataOffset()
	require.NoError(t, err)
	file, err := os.OpenFile(filepath.Clean(path), os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{0xff}, offset)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	var result error
	for _, err := range plan.Messages(0) {
		result = err
		break
	}
	require.Error(t, result)
}

type countedHeaderSource struct {
	io.ReaderAt
	calls int
}

func (s *countedHeaderSource) ReadAt(p []byte, offset int64) (int, error) {
	s.calls++
	return s.ReaderAt.ReadAt(p, offset)
}

type fullErrorHeaderSource struct{ err error }

func (s fullErrorHeaderSource) ReadAt(p []byte, _ int64) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), s.err
}

func TestHeaderCaptureReverseOrderAndFullReadErrors(t *testing.T) {
	source := &countedHeaderSource{ReaderAt: bytes.NewReader(bytes.Repeat([]byte("a"), 4096))}
	reader := &headerReader{source: source, captured: map[int64]byte{}}
	for offset := int64(4096 - 16); offset >= 0; offset -= 16 {
		buffer := make([]byte, 16)
		_, err := reader.ReadAt(buffer, offset)
		require.NoError(t, err)
	}
	require.Equal(t, 256, source.calls)
	require.Len(t, reader.captured, 4096)
	reader.freeze()
	require.Len(t, reader.ranges, 1)
	buffer := make([]byte, 4096)
	_, err := reader.ReadAt(buffer, 0)
	require.NoError(t, err)
	require.Equal(t, 256, source.calls)
	require.Equal(t, bytes.Repeat([]byte("a"), 4096), buffer)
	sentinel := errors.New("source read failure")
	for _, capture := range []bool{false, true} {
		for _, sourceErr := range []error{sentinel, io.EOF} {
			reader := &headerReader{source: fullErrorHeaderSource{err: sourceErr}}
			if capture {
				reader.captured = map[int64]byte{}
			}
			n, err := reader.ReadAt(make([]byte, 3), 0)
			require.Equal(t, 3, n)
			if sourceErr == io.EOF {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, sentinel)
			}
		}
	}
}

func TestPreparedExportCatalogPresence(t *testing.T) {
	for _, kind := range []string{"missing", "null", "empty", "null-plus-valid"} {
		for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
			files := map[string]string{"unrelated.json": "[]"}
			switch kind {
			case "null":
				files["channels.json"] = "null"
			case "empty":
				files["channels.json"] = "[]"
			case "null-plus-valid":
				files["channels.json"] = "[]"
				files["groups.json"] = "null"
			}
			ex, err := Open(writeFixtureZIP(t, files))
			require.NoError(t, err)
			plan, err := ex.Prepare("T1", policy)
			if policy == admission.Exclude && kind != "empty" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Empty(t, plan.Channels())
			}
			require.NoError(t, ex.Close())
		}
	}
}
