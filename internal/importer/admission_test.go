package importer

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/stretchr/testify/require"
)

func TestPreparedExportClassification(t *testing.T) {
	for _, tc := range []struct {
		name, role, flags, want string
		rejected                bool
	}{
		{"sparse-public", "channels", "", "public", false},
		{"sparse-private", "groups", "", "private", false},
		{"sparse-public-false", "channels", `,"is_private":false`, "public", false},
		{"sparse-private-true", "groups", `,"is_private":true`, "private", false},
		{"sparse-public-conflict", "channels", `,"is_private":true`, "", true},
		{"sparse-private-conflict", "groups", `,"is_private":false`, "", true},
		{"partial-channel", "channels", `,"is_channel":true`, "public", false},
		{"partial-group", "groups", `,"is_group":true`, "private", false},
		{"modern-private", "channels", `,"is_channel":true,"is_private":true`, "private", false},
		{"native-public-in-groups", "groups", `,"is_channel":true,"is_private":false`, "public", false},
		{"legacy-private-in-channels", "channels", `,"is_group":true,"is_private":true`, "private", false},
		{"group-without-private", "channels", `,"is_group":true`, "", true},
		{"negative-only", "channels", `,"is_im":false`, "", true},
		{"all-false", "channels", `,"is_channel":false,"is_group":false,"is_im":false,"is_mpim":false`, "", true},
		{"conflicting-native", "channels", `,"is_channel":true,"is_group":true`, "", true},
		{"null-native", "channels", `,"is_channel":null`, "", true},
		{"string-native", "channels", `,"is_channel":"true"`, "", true},
		{"numeric-native", "channels", `,"is_channel":1`, "", true},
		{"null-private", "channels", `,"is_private":null`, "", true},
		{"uppercase-public", "channels", `,"IS_PRIVATE":false`, "public", false},
		{"uppercase-private", "groups", `,"IS_PRIVATE":true`, "private", false},
		{"uppercase-public-conflict", "channels", `,"IS_PRIVATE":true`, "", true},
		{"uppercase-private-conflict", "groups", `,"IS_PRIVATE":false`, "", true},
		{"mixed-native-public", "groups", `,"Is_Channel":true,"IS_PRIVATE":false`, "public", false},
		{"mixed-native-private", "channels", `,"IS_CHANNEL":true,"Is_Private":true`, "private", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeFixtureZIP(t, map[string]string{tc.role + ".json": `[{"id":"NOTAPREFIX","name":"room"` + tc.flags + `}]`, "room/1.json": `[{"ts":"1","text":"kept"}]`})
			ex, err := Open(path)
			require.NoError(t, err)
			defer ex.Close()
			plan, err := ex.Prepare("T1", admission.Exclude)
			if tc.rejected {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, plan.Channels(), 1)
			require.Equal(t, tc.want, plan.Channels()[0].Kind)
			require.NoError(t, plan.Scan(context.Background(), func(ChannelInfo, MessageEnvelope) error { return nil }))
			count := 0
			for _, err := range plan.Messages(0) {
				require.NoError(t, err)
				count++
			}
			require.Equal(t, 1, count)
		})
	}
}

func TestPreparedExportDMVetoIncludesEveryCatalogOccurrence(t *testing.T) {
	for _, mode := range []string{"im", "mpim", "mixed-im", "mixed-mpim", "dm-catalog", "mpim-catalog"} {
		for _, reverse := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/forward", true: "/reverse"}[reverse], func(t *testing.T) {
				positive := `{"id":"CSAME","name":"room","is_channel":true}`
				flag := "is_" + mode
				if mode == "mixed-im" {
					flag = "Is_Im"
				} else if mode == "mixed-mpim" {
					flag = "IS_MPIM"
				}
				negative := `{"id":"CSAME","name":"room","is_channel":true,"` + flag + `":true}`
				files := map[string]string{"room/1.json": "excluded-invalid-json-canary"}
				if mode == "dm-catalog" || mode == "mpim-catalog" {
					files["channels.json"] = "[" + positive + "]"
					role := "dms"
					if mode == "mpim-catalog" {
						role = "mpims"
					}
					files[role+".json"] = `[{"id":"CSAME","name":"room"}]`
				} else {
					rows := []string{positive, negative}
					if reverse {
						rows[0], rows[1] = rows[1], rows[0]
					}
					files["channels.json"] = "[" + strings.Join(rows, ",") + "]"
				}
				ex, err := Open(writeFixtureZIP(t, files))
				require.NoError(t, err)
				defer ex.Close()
				plan, err := ex.Prepare("T1", admission.Exclude)
				require.NoError(t, err)
				require.Empty(t, plan.Channels())
				require.Equal(t, 1, plan.OmittedDM())
				for _, policy := range []admission.DMPolicy{admission.Default, admission.Include} {
					plan, err := ex.Prepare("T1", policy)
					require.NoError(t, err)
					require.NotEmpty(t, plan.Channels())
					err = plan.Scan(context.Background(), func(ChannelInfo, MessageEnvelope) error { return nil })
					require.Error(t, err)
					require.NotContains(t, err.Error(), "excluded-invalid-json-canary")
				}
			})
		}
	}
}

func TestPreparedExportIdentityBeforeTimestampAndProjection(t *testing.T) {
	for _, policy := range []admission.DMPolicy{admission.Default, admission.Include, admission.Exclude} {
		for _, wrapper := range []string{"top", "message", "previous_message", "root", "previous", "context_team_id", "catalog-latest"} {
			t.Run(string(rune('0'+policy))+"/"+wrapper, func(t *testing.T) {
				raw := map[string]any{"channel": "COTHER", "text": "identity-canary"} // deliberately no timestamp
				if wrapper == "context_team_id" {
					raw = map[string]any{"context_team_id": "TOTHER"}
				}
				if wrapper != "top" && wrapper != "context_team_id" && wrapper != "catalog-latest" {
					raw = map[string]any{wrapper: raw}
				}
				catalog := map[string]any{"id": "C1", "name": "room"}
				if wrapper == "catalog-latest" {
					catalog["latest"] = raw
					raw = map[string]any{"ts": "1"}
				}
				cat, _ := json.Marshal([]any{catalog})
				body, _ := json.Marshal([]any{raw})
				ex, err := Open(writeFixtureZIP(t, map[string]string{"channels.json": string(cat), "room/1.json": string(body)}))
				require.NoError(t, err)
				defer ex.Close()
				plan, err := ex.Prepare("T1", policy)
				if err == nil {
					err = plan.Scan(context.Background(), func(ChannelInfo, MessageEnvelope) error { return nil })
				}
				require.ErrorContains(t, err, "identity")
				require.NotContains(t, err.Error(), "COTHER")
				require.NotContains(t, err.Error(), "identity-canary")
			})
		}
	}
	ex, err := Open(writeFixtureZIP(t, map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`, "room/1.json": `[{"channel":"C1","context_team_id":"T1","team":"TEXTERNAL","user_team":"TEXTERNAL","latest":{"channel":"COTHER"},"files":[{"shares":{"COTHER":[]}}],"attachments":[{"channel":"COTHER"}],"previous":{"channel":"C1"},"ts":"1"}]`}))
	require.NoError(t, err)
	defer ex.Close()
	plan, err := ex.Prepare("T1", admission.Exclude)
	require.NoError(t, err)
	require.NoError(t, plan.Scan(context.Background(), func(ChannelInfo, MessageEnvelope) error { return nil }))
}

func TestPreparedExportLocatorOwnership(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("directory identity is platform-qualified")
	}
	for _, mode := range []string{"same-owner", "directory-alias", "hardlink", "unused-fallback", "logical-name-id"} {
		t.Run(mode, func(t *testing.T) {
			files := map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`, "room/1.json": `[{"ts":"1"}]`}
			root := writeFixtureDir(t, files)
			if mode == "same-owner" {
				require.NoError(t, os.Symlink("room", filepath.Join(root, "C1")))
			} else {
				name := "direct"
				if mode == "logical-name-id" {
					name = "C1"
				}
				require.NoError(t, os.WriteFile(filepath.Join(root, "dms.json"), []byte(`[{"id":"D1","name":"`+name+`"}]`), 0600))
				switch mode {
				case "directory-alias":
					require.NoError(t, os.Symlink("room", filepath.Join(root, "direct")))
				case "hardlink":
					require.NoError(t, os.Mkdir(filepath.Join(root, "direct"), 0700))
					require.NoError(t, os.Link(filepath.Join(root, "room", "1.json"), filepath.Join(root, "direct", "1.json")))
				case "unused-fallback":
					require.NoError(t, os.Mkdir(filepath.Join(root, "direct"), 0700))
					require.NoError(t, os.WriteFile(filepath.Join(root, "direct", "1.json"), []byte("excluded"), 0600))
					require.NoError(t, os.Symlink("direct", filepath.Join(root, "C1")))
				}
			}
			ex, err := Open(root)
			require.NoError(t, err)
			defer ex.Close()
			plan, err := ex.Prepare("T1", admission.Exclude)
			if mode != "same-owner" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NoError(t, plan.Scan(context.Background(), func(ChannelInfo, MessageEnvelope) error { return nil }))
		})
	}
}

func TestPreparedExportFrozenFilesAndFallback(t *testing.T) {
	for _, mode := range []string{"blank-ts-name", "empty-name", "same-inode", "replace", "missing", "additions", "unused-fallback"} {
		t.Run(mode, func(t *testing.T) {
			nameBody := `[{"ts":"1","text":"name"}]`
			if mode == "blank-ts-name" {
				nameBody = `[{"text":"name"}]`
			}
			if mode == "empty-name" {
				nameBody = `[]`
			}
			idBody := `[{"ts":"2","text":"id"}]`
			if mode == "unused-fallback" {
				idBody = "invalid unused fallback"
			}
			root := writeFixtureDir(t, map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`, "room/1.json": nameBody, "C1/1.json": idBody})
			ex, err := Open(root)
			require.NoError(t, err)
			defer ex.Close()
			plan, err := ex.Prepare("T1", admission.Default)
			require.NoError(t, err)
			require.NoError(t, plan.Scan(context.Background(), func(ChannelInfo, MessageEnvelope) error { return nil }))
			switch mode {
			case "same-inode":
				require.NoError(t, os.WriteFile(filepath.Join(root, "room", "1.json"), []byte(`[{"ts":"9","text":"changed"}]`), 0600))
			case "replace":
				replacement := filepath.Join(root, "replacement")
				require.NoError(t, os.WriteFile(replacement, []byte(nameBody), 0600))
				require.NoError(t, os.Rename(replacement, filepath.Join(root, "room", "1.json")))
			case "missing":
				require.NoError(t, os.Remove(filepath.Join(root, "room", "1.json")))
			case "additions":
				require.NoError(t, os.WriteFile(filepath.Join(root, "channels.json"), []byte("changed catalog"), 0600))
				require.NoError(t, os.WriteFile(filepath.Join(root, "room", "new.json"), []byte("new invalid file"), 0600))
			}
			messages := []MessageEnvelope{}
			var iterErr error
			for env, err := range plan.Messages(0) {
				if err != nil {
					iterErr = err
					break
				}
				messages = append(messages, env)
			}
			if mode == "same-inode" || mode == "replace" || mode == "missing" {
				require.Error(t, iterErr)
				require.Empty(t, messages)
				return
			}
			require.NoError(t, iterErr)
			require.Len(t, messages, 1)
			want := "name"
			if mode == "empty-name" {
				want = "id"
			}
			require.Equal(t, want, messages[0].Raw["text"])
		})
	}
}

// A replacement after Open must not change the already-opened file's bytes.
// This exercises the actual fs seam without sleeps or production test hooks.
type replaceAfterOpenFS struct {
	fs.FS
	target, replacement string
	calls               int
}

func (s *replaceAfterOpenFS) Open(name string) (fs.File, error) {
	f, err := s.FS.Open(name)
	if err == nil && name == "room/1.json" {
		s.calls++
		if s.calls == 3 {
			if err := os.Rename(s.replacement, s.target); err != nil {
				_ = f.Close()
				return nil, err
			}
		}
	}
	return f, err
}
func TestPreparedExportDecodesVerifiedOpenHandle(t *testing.T) {
	root := writeFixtureDir(t, map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`, "room/1.json": `[{"ts":"1","text":"original"}]`, "replacement": `[{"ts":"2","text":"replacement"}]`})
	ex, err := Open(root)
	require.NoError(t, err)
	defer ex.Close()
	ex.fs = &replaceAfterOpenFS{FS: ex.fs, target: filepath.Join(root, "room", "1.json"), replacement: filepath.Join(root, "replacement")}
	plan, err := ex.Prepare("T1", admission.Default)
	require.NoError(t, err)
	require.NoError(t, plan.Scan(context.Background(), func(ChannelInfo, MessageEnvelope) error { return nil }))
	var rows []MessageEnvelope
	for env, err := range plan.Messages(0) {
		require.NoError(t, err)
		rows = append(rows, env)
	}
	require.Len(t, rows, 1)
	require.Equal(t, "original", rows[0].Raw["text"])
}

func TestExportZIPRejectsAmbiguousEntriesAndSharedSpans(t *testing.T) {
	for _, mode := range []string{"duplicate", "normalized", "shared-payload", "insecure-path"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "insecure-path" {
				t.Setenv("GODEBUG", "zipinsecurepath=0")
			}
			path := writeFixtureZIP(t, map[string]string{"channels.json": `[{"id":"C1","name":"room"}]`, "dms.json": `[{"id":"D1","name":"direct"}]`, "room/1.json": `[{"ts":"1"}]`, "direct/1.json": `[{"ts":"1"}]`})
			blob, err := os.ReadFile(path)
			require.NoError(t, err)
			// Patch only the central directory, preserving actual local payloads.
			headers := map[string]int{}
			for offset := 0; offset+46 <= len(blob); offset++ {
				if !bytes.Equal(blob[offset:offset+4], []byte{'P', 'K', 1, 2}) {
					continue
				}
				n := int(binary.LittleEndian.Uint16(blob[offset+28:]))
				if offset+46+n > len(blob) {
					continue
				}
				headers[string(blob[offset+46:offset+46+n])] = offset
			}
			direct := headers["direct/1.json"]
			require.NotZero(t, direct)
			switch mode {
			case "shared-payload":
				copy(blob[direct+42:direct+46], blob[headers["room/1.json"]+42:headers["room/1.json"]+46])
			case "duplicate", "normalized", "insecure-path":
				// Both replacement names are the same length as direct/1.json.
				replacement := "room/../1.json"
				if mode == "duplicate" {
					replacement = "room/1.json"
					blob = replaceZIPCentralName(t, blob, direct, replacement)
				}
				if mode == "normalized" {
					blob = replaceZIPCentralName(t, blob, direct, "room/./1.json")
				}
				if mode == "insecure-path" {
					blob = replaceZIPCentralName(t, blob, direct, "../room/1.json")
				}
			}
			require.NoError(t, os.WriteFile(path, blob, 0600))
			ex, err := Open(path)
			if err == nil {
				defer ex.Close()
				_, err = ex.Prepare("T1", admission.Exclude)
			}
			require.Error(t, err)
		})
	}
}

func replaceZIPCentralName(t *testing.T, blob []byte, offset int, name string) []byte {
	t.Helper()
	old := int(binary.LittleEndian.Uint16(blob[offset+28:]))
	delta := len(name) - old
	out := append([]byte{}, blob[:offset+46]...)
	out = append(out, []byte(name)...)
	out = append(out, blob[offset+46+old:]...)
	binary.LittleEndian.PutUint16(out[offset+28:], uint16(len(name)))
	end := bytes.LastIndex(out, []byte{'P', 'K', 5, 6})
	require.GreaterOrEqual(t, end, 0)
	size := binary.LittleEndian.Uint32(out[end+12:])
	binary.LittleEndian.PutUint32(out[end+12:], uint32(int(size)+delta))
	return out
}

func TestStrictExportEmptyBodiesKeepIDFallback(t *testing.T) {
	for _, body := range []string{"", " \n\t", "[]"} {
		ex, err := Open(writeFixtureZIP(t, map[string]string{
			"channels.json": `[{"id":"C1","name":"room"}]`,
			"room/1.json":   body,
			"C1/1.json":     `[{"ts":"1","text":"fallback"}]`,
		}))
		require.NoError(t, err)
		plan, err := ex.Prepare("T1", admission.Exclude)
		require.NoError(t, err)
		require.NoError(t, plan.Scan(context.Background(), func(ChannelInfo, MessageEnvelope) error { return nil }))
		rows := []MessageEnvelope{}
		for row, err := range plan.Messages(0) {
			require.NoError(t, err)
			rows = append(rows, row)
		}
		require.Len(t, rows, 1)
		require.Equal(t, "fallback", rows[0].Raw["text"])
		require.NoError(t, ex.Close())
	}
}
