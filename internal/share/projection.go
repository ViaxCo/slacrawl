package share

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	projectionFormat       = "slacrawl-projection"
	projectionVersion      = 1
	projectionManifestName = "manifest.json"
	projectionMessagesName = "messages.jsonl"
)

type ProjectionPayload struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	Rows   int    `json:"rows"`
}

type ProjectionManifest struct {
	Format           string                   `json:"format"`
	Version          int                      `json:"version"`
	ProducerRevision string                   `json:"producer_revision"`
	WorkspaceID      string                   `json:"workspace_id"`
	WorkspaceLabel   string                   `json:"workspace_label"`
	Channels         []ExportChannelSelection `json:"channels"`
	AuthorIDs        []string                 `json:"author_ids"`
	Payload          ProjectionPayload        `json:"payload"`
}

// ProjectionReceipt describes bytes observed during verification, not their
// future contents, producer authenticity or permission to publish them.
type ProjectionReceipt struct {
	ManifestSHA256 string
	MessagesSHA256 string
	ManifestBytes  int64
	MessagesBytes  int64
	Rows           int
}

// WriteProjection claims a fresh directory and retains incomplete output on any
// error. It makes no atomic visibility or crash-durability guarantee.
func WriteProjection(ctx context.Context, dir string, expected ExportProjection, producerRevision string) (ProjectionReceipt, error) {
	if dir == "" {
		return ProjectionReceipt{}, errors.New("projection destination is required")
	}
	dir = filepath.Clean(dir)
	authors, err := validateProjectionExpected(expected, producerRevision)
	if err != nil {
		return ProjectionReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return ProjectionReceipt{}, err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return ProjectionReceipt{}, errors.New("projection destination must be new with an existing parent")
	}
	root, identity, err := openProjectionRoot(dir)
	if err != nil {
		return ProjectionReceipt{}, err
	}
	defer func() { _ = root.Close() }()
	file, err := root.OpenFile(projectionMessagesName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return ProjectionReceipt{}, errors.New("cannot create projection messages")
	}
	hash := sha256.New()
	var count int64
	err = finishProjectionFile(file, func(w io.Writer) error {
		for _, message := range expected.Messages {
			if err := ctx.Err(); err != nil {
				return err
			}
			body, err := json.Marshal(message)
			if err != nil {
				return err
			}
			body = append(body, '\n')
			if err := writeProjectionBytes(w, body); err != nil {
				return err
			}
			_, _ = hash.Write(body)
			count += int64(len(body))
		}
		return nil
	})
	if err != nil {
		return ProjectionReceipt{}, errors.New("cannot write, sync or close projection messages; incomplete destination retained")
	}
	manifest := ProjectionManifest{
		Format: projectionFormat, Version: projectionVersion, ProducerRevision: producerRevision,
		WorkspaceID: expected.WorkspaceID, WorkspaceLabel: expected.WorkspaceLabel, Channels: expected.Channels, AuthorIDs: authors,
		Payload: ProjectionPayload{Path: projectionMessagesName, SHA256: hex.EncodeToString(hash.Sum(nil)), Bytes: count, Rows: len(expected.Messages)},
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return ProjectionReceipt{}, errors.New("cannot encode projection manifest")
	}
	if err := ctx.Err(); err != nil {
		return ProjectionReceipt{}, err
	}
	file, err = root.OpenFile(projectionManifestName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return ProjectionReceipt{}, errors.New("cannot create projection manifest; incomplete destination retained")
	}
	if err := finishProjectionFile(file, func(w io.Writer) error { return writeProjectionBytes(w, append(body, '\n')) }); err != nil {
		return ProjectionReceipt{}, errors.New("cannot write, sync or close projection manifest; incomplete destination retained")
	}
	if err := root.Close(); err != nil {
		return ProjectionReceipt{}, errors.New("cannot close projection destination")
	}
	// Reopen through the independent reader; producer hashes are not receipts.
	receipt, err := VerifyProjection(ctx, dir, expected, producerRevision)
	if err != nil {
		return ProjectionReceipt{}, err
	}
	current, err := os.Lstat(dir)
	if err != nil || !current.IsDir() || !os.SameFile(identity, current) {
		return ProjectionReceipt{}, errors.New("projection destination identity changed")
	}
	return receipt, nil
}

type projectionFile interface {
	io.Writer
	Sync() error
	Close() error
}

func finishProjectionFile(file projectionFile, write func(io.Writer) error) error {
	err := write(file)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func writeProjectionBytes(w io.Writer, body []byte) error {
	n, err := w.Write(body)
	if err == nil && n != len(body) {
		return io.ErrShortWrite
	}
	return err
}

// This closed record deliberately does not reuse the producer's row emitter.
// Canonical roundtripping detects ignored, duplicated and misspelled fields.
type projectionRecord struct {
	ChannelID string  `json:"channel_id"`
	TS        string  `json:"ts"`
	UserID    *string `json:"user_id"`
	Text      string  `json:"text"`
	ThreadTS  *string `json:"thread_ts"`
	EditedTS  *string `json:"edited_ts"`
}

func VerifyProjection(ctx context.Context, dir string, expected ExportProjection, expectedRevision string) (ProjectionReceipt, error) {
	if dir == "" {
		return ProjectionReceipt{}, errors.New("projection destination is required")
	}
	dir = filepath.Clean(dir)
	authors, err := validateProjectionExpected(expected, expectedRevision)
	if err != nil {
		return ProjectionReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return ProjectionReceipt{}, err
	}
	root, identity, err := openProjectionRoot(dir)
	if err != nil {
		return ProjectionReceipt{}, err
	}
	defer func() { _ = root.Close() }()
	if err := projectionEntries(root); err != nil {
		return ProjectionReceipt{}, err
	}
	manifestFile, manifestInfo, err := openProjectionFile(root, projectionManifestName)
	if err != nil {
		return ProjectionReceipt{}, err
	}
	defer func() { _ = manifestFile.Close() }()
	messagesFile, messagesInfo, err := openProjectionFile(root, projectionMessagesName)
	if err != nil {
		return ProjectionReceipt{}, err
	}
	defer func() { _ = messagesFile.Close() }()
	if os.SameFile(manifestInfo, messagesInfo) {
		return ProjectionReceipt{}, errors.New("projection files must be distinct")
	}
	// Bound reads by the explicit expected content, not untrusted file metadata.
	bound := int64(1024 + 6*(len(expected.WorkspaceID)+len(expected.WorkspaceLabel)))
	for _, channel := range expected.Channels {
		bound += int64(64 + 6*(len(channel.ChannelID)+len(channel.Label)))
	}
	for _, author := range authors {
		bound += int64(8 + 6*len(author))
	}
	body, err := io.ReadAll(io.LimitReader(manifestFile, bound+1))
	if err != nil || int64(len(body)) > bound {
		return ProjectionReceipt{}, errors.New("cannot read bounded projection manifest")
	}
	var manifest ProjectionManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return ProjectionReceipt{}, errors.New("invalid projection manifest")
	}
	canonical, err := json.Marshal(manifest)
	if err != nil || !bytes.Equal(append(canonical, '\n'), body) || manifest.Channels == nil || manifest.AuthorIDs == nil {
		return ProjectionReceipt{}, errors.New("projection manifest is not canonical")
	}
	if manifest.Format != projectionFormat || manifest.Version != projectionVersion || manifest.ProducerRevision != expectedRevision ||
		manifest.WorkspaceID != expected.WorkspaceID || manifest.WorkspaceLabel != expected.WorkspaceLabel ||
		!slices.Equal(manifest.Channels, expected.Channels) || !slices.Equal(manifest.AuthorIDs, authors) ||
		manifest.Payload.Path != projectionMessagesName || manifest.Payload.Rows != len(expected.Messages) {
		return ProjectionReceipt{}, errors.New("projection manifest differs from expected fields")
	}
	channels := make(map[string]bool, len(manifest.Channels))
	for i, channel := range manifest.Channels {
		if channels[channel.ChannelID] || (i > 0 && channel.ChannelID <= manifest.Channels[i-1].ChannelID) {
			return ProjectionReceipt{}, errors.New("projection channels are not ordered and unique")
		}
		channels[channel.ChannelID] = true
	}
	hash := sha256.New()
	reader := bufio.NewReader(io.TeeReader(messagesFile, hash))
	rows := make(map[selectionMessageKey]projectionRecord, len(expected.Messages))
	actualAuthors := map[string]bool{}
	var messageBytes int64
	var previous selectionMessageKey
	for i, wanted := range expected.Messages {
		if err := ctx.Err(); err != nil {
			return ProjectionReceipt{}, err
		}
		lineBound := int64(128 + 6*(len(wanted.ChannelID)+len(wanted.TS)+len(wanted.Text)))
		for _, value := range []*string{wanted.UserID, wanted.ThreadTS, wanted.EditedTS} {
			if value != nil {
				lineBound += int64(6 * len(*value))
			}
		}
		line, err := readProjectionLine(reader, lineBound)
		if err != nil {
			return ProjectionReceipt{}, fmt.Errorf("cannot read bounded projection message %d", i)
		}
		var record projectionRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return ProjectionReceipt{}, fmt.Errorf("invalid projection message %d", i)
		}
		canonical, err := json.Marshal(record)
		if err != nil || !bytes.Equal(append(canonical, '\n'), line) {
			return ProjectionReceipt{}, fmt.Errorf("projection message %d is not canonical", i)
		}
		key := selectionMessageKey{record.ChannelID, record.TS}
		if !channels[record.ChannelID] || (i > 0 && (key.channel < previous.channel || (key.channel == previous.channel && key.ts <= previous.ts))) {
			return ProjectionReceipt{}, fmt.Errorf("projection message %d is outside ordered channel membership", i)
		}
		if record.ChannelID != wanted.ChannelID || record.TS != wanted.TS || record.Text != wanted.Text ||
			!projectionNullableEqual(record.UserID, wanted.UserID) || !projectionNullableEqual(record.ThreadTS, wanted.ThreadTS) || !projectionNullableEqual(record.EditedTS, wanted.EditedTS) {
			return ProjectionReceipt{}, fmt.Errorf("projection message %d differs from expected fields", i)
		}
		rows[key] = record
		previous = key
		if record.UserID != nil && *record.UserID != "" {
			actualAuthors[*record.UserID] = true
		}
		messageBytes += int64(len(line))
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		return ProjectionReceipt{}, errors.New("projection messages contain trailing content or a read failure")
	}
	for _, row := range rows {
		if row.ThreadTS != nil && *row.ThreadTS != "" && *row.ThreadTS != row.TS {
			parent, ok := rows[selectionMessageKey{row.ChannelID, *row.ThreadTS}]
			if !ok || (parent.ThreadTS != nil && *parent.ThreadTS != "" && *parent.ThreadTS != parent.TS) {
				return ProjectionReceipt{}, errors.New("projection reply has no selected root")
			}
		}
	}
	if len(actualAuthors) != len(manifest.AuthorIDs) {
		return ProjectionReceipt{}, errors.New("projection author inventory differs")
	}
	for _, author := range manifest.AuthorIDs {
		if !actualAuthors[author] {
			return ProjectionReceipt{}, errors.New("projection author inventory differs")
		}
	}
	messageSHA := hex.EncodeToString(hash.Sum(nil))
	if manifest.Payload.SHA256 != messageSHA || manifest.Payload.Bytes != messageBytes {
		return ProjectionReceipt{}, errors.New("projection payload checksum or byte count differs")
	}
	if err := checkProjectionFile(root, projectionManifestName, manifestFile, manifestInfo); err != nil {
		return ProjectionReceipt{}, err
	}
	if err := checkProjectionFile(root, projectionMessagesName, messagesFile, messagesInfo); err != nil {
		return ProjectionReceipt{}, err
	}
	if err := projectionEntries(root); err != nil {
		return ProjectionReceipt{}, err
	}
	current, err := os.Lstat(dir)
	if err != nil || !current.IsDir() || !os.SameFile(identity, current) {
		return ProjectionReceipt{}, errors.New("projection root identity changed")
	}
	if err := manifestFile.Close(); err != nil {
		return ProjectionReceipt{}, errors.New("cannot close verified projection manifest")
	}
	if err := messagesFile.Close(); err != nil {
		return ProjectionReceipt{}, errors.New("cannot close verified projection messages")
	}
	if err := root.Close(); err != nil {
		return ProjectionReceipt{}, errors.New("cannot close verified projection root")
	}
	if err := ctx.Err(); err != nil {
		return ProjectionReceipt{}, err
	}
	return ProjectionReceipt{ManifestSHA256: selectionRawSHA256(string(body)), MessagesSHA256: messageSHA, ManifestBytes: int64(len(body)), MessagesBytes: messageBytes, Rows: len(rows)}, nil
}

func validateProjectionExpected(expected ExportProjection, revision string) ([]string, error) {
	if len(revision) != 40 || strings.Trim(revision, "0123456789abcdef") != "" {
		return nil, errors.New("projection producer revision must be lowercase 40-hex")
	}
	if strings.TrimSpace(expected.WorkspaceID) == "" || !utf8.ValidString(expected.WorkspaceID) || !utf8.ValidString(expected.WorkspaceLabel) || len(expected.Channels) == 0 || len(expected.Messages) == 0 {
		return nil, errors.New("projection requires explicit workspace, channels and messages")
	}
	channels := map[string]bool{}
	for i, channel := range expected.Channels {
		if strings.TrimSpace(channel.ChannelID) == "" || !utf8.ValidString(channel.ChannelID) || !utf8.ValidString(channel.Label) || (i > 0 && channel.ChannelID <= expected.Channels[i-1].ChannelID) {
			return nil, errors.New("expected projection channels must be valid, ordered and unique")
		}
		channels[channel.ChannelID] = true
	}
	rows := map[selectionMessageKey]ExportMessage{}
	authors := []string{}
	for i, row := range expected.Messages {
		if !channels[row.ChannelID] || strings.TrimSpace(row.TS) == "" || !utf8.ValidString(row.TS) || !utf8.ValidString(row.Text) {
			return nil, fmt.Errorf("expected projection message %d has invalid identity or text", i)
		}
		if i > 0 {
			previous := expected.Messages[i-1]
			if row.ChannelID < previous.ChannelID || (row.ChannelID == previous.ChannelID && row.TS <= previous.TS) {
				return nil, errors.New("expected projection messages must be ordered and unique")
			}
		}
		for _, value := range []*string{row.UserID, row.ThreadTS, row.EditedTS} {
			if value != nil && !utf8.ValidString(*value) {
				return nil, fmt.Errorf("expected projection message %d has invalid nullable text", i)
			}
		}
		if row.UserID != nil && *row.UserID != "" {
			authors = append(authors, *row.UserID)
		}
		rows[selectionMessageKey{row.ChannelID, row.TS}] = row
	}
	for _, row := range rows {
		if row.ThreadTS != nil && *row.ThreadTS != "" && *row.ThreadTS != row.TS {
			parent, ok := rows[selectionMessageKey{row.ChannelID, *row.ThreadTS}]
			if !ok || (parent.ThreadTS != nil && *parent.ThreadTS != "" && *parent.ThreadTS != parent.TS) {
				return nil, errors.New("expected projection reply requires its selected root")
			}
		}
	}
	slices.Sort(authors)
	return slices.Compact(authors), nil
}

func projectionNullableEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func openProjectionRoot(dir string) (*os.Root, os.FileInfo, error) {
	before, err := os.Lstat(dir)
	if err != nil || !before.IsDir() {
		return nil, nil, errors.New("projection root must be a nonsymlink directory")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, nil, errors.New("cannot open projection root")
	}
	after, err := root.Stat(".")
	if err != nil || !os.SameFile(before, after) {
		_ = root.Close()
		return nil, nil, errors.New("projection root identity changed while opening")
	}
	return root, before, nil
}

func projectionEntries(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return errors.New("cannot read projection inventory")
	}
	entries, readErr := dir.ReadDir(3)
	closeErr := dir.Close()
	if (readErr != nil && readErr != io.EOF) || closeErr != nil || len(entries) != 2 {
		return errors.New("projection must contain exactly two files")
	}
	names := []string{entries[0].Name(), entries[1].Name()}
	slices.Sort(names)
	if !slices.Equal(names, []string{projectionManifestName, projectionMessagesName}) {
		return errors.New("projection contains unexpected entries")
	}
	return nil
}

func openProjectionFile(root *os.Root, name string) (*os.File, os.FileInfo, error) {
	before, err := root.Lstat(name)
	if err != nil || !before.Mode().IsRegular() {
		return nil, nil, errors.New("projection entries must be regular nonsymlink files")
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, nil, errors.New("cannot open projection file")
	}
	if err := checkProjectionFile(root, name, file, before); err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	return file, before, nil
}

func checkProjectionFile(root *os.Root, name string, file *os.File, before os.FileInfo) error {
	opened, err := file.Stat()
	if err != nil {
		return errors.New("cannot inspect projection file handle")
	}
	named, err := root.Lstat(name)
	if err != nil || !named.Mode().IsRegular() || !opened.Mode().IsRegular() ||
		!os.SameFile(before, opened) || !os.SameFile(opened, named) || before.Size() != opened.Size() || before.Size() != named.Size() ||
		before.Mode() != opened.Mode() || before.Mode() != named.Mode() ||
		!before.ModTime().Equal(opened.ModTime()) || !before.ModTime().Equal(named.ModTime()) {
		return errors.New("projection file identity or metadata changed")
	}
	return nil
}

func readProjectionLine(reader *bufio.Reader, bound int64) ([]byte, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if int64(len(line))+int64(len(part)) > bound {
			return nil, errors.New("projection row exceeds expected bound")
		}
		line = append(line, part...)
		if err == nil {
			return line, nil
		}
		if err != bufio.ErrBufferFull {
			return nil, err
		}
	}
}
