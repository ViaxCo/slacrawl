package share

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

const selectionPublicRaw = `{"id":"C1","is_channel":true,"is_private":false,"is_group":false,"is_im":false,"is_mpim":false,"context_team_id":"T1","private_payload":"selection-raw-canary"}`

func selectionFixture(t *testing.T) (*store.Store, string, ExportSelection) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "archive.db")
	s := seedStore(t, path)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	ctx := context.Background()
	_, err := s.DB().ExecContext(ctx, `update channels set name = 'selection-channel-name-canary', raw_json = ? where id = 'C1'`, selectionPublicRaw)
	require.NoError(t, err)
	_, err = s.DB().ExecContext(ctx, `update workspaces set name = 'selection-workspace-name-canary' where id = 'T1'; update users set display_name = 'selection-profile-canary'`)
	require.NoError(t, err)
	for _, message := range []store.Message{
		{TS: "123.456", ThreadTS: "123.456", Text: "selected root", ReplyCount: 1, EditedTS: "125.000"},
		{TS: "124.000", ThreadTS: "123.456", Text: "selection-replaced-canary"},
		{TS: "126.000", Text: "selection-empty-replacement-canary"},
		{TS: "127.000", Text: "selection-unselected-canary"},
	} {
		message.ChannelID, message.WorkspaceID, message.UserID = "C1", "T1", "U1"
		message.SourceRank, message.SourceName = 2, "api-bot"
		message.RawJSON = `{"text":"selection-raw-canary"}`
		message.NormalizedText = "selection-normalized-canary"
		message.UpdatedAt = time.Unix(1710000000, 0).UTC()
		require.NoError(t, s.UpsertMessage(ctx, message, nil))
	}
	return s, path, ExportSelection{
		WorkspaceID: "T1", Channels: []ExportChannelSelection{{ChannelID: "C1"}},
		Messages: []ExportMessageSelection{
			{ChannelID: "C1", TS: "126.000", Text: ExportTextChoice{Mode: ExportTextReplace, Replacement: new("")}},
			{ChannelID: "C1", TS: "123.456", Text: ExportTextChoice{Mode: ExportTextKeep}},
			{ChannelID: "C1", TS: "124.000", Text: ExportTextChoice{Mode: ExportTextReplace, Replacement: new("reviewed reply")}},
		},
	}
}

func TestExportSelectionRoundTrip(t *testing.T) {
	s, path, selection := selectionFixture(t)
	ctx := context.Background()
	before := historyArchiveState(t, s)
	plan, err := PrepareExportSelection(ctx, path, selection)
	require.NoError(t, err)
	require.Equal(t, ExportSelectionVersion, plan.Version)
	require.Equal(t, "T1", plan.WorkspaceLabel)
	require.Equal(t, "C1", plan.Channels[0].Label)
	require.Len(t, plan.WorkspaceSHA256, 64)
	body, err := json.Marshal(plan)
	require.NoError(t, err)
	var decoded ExportSelectionPlan
	require.NoError(t, json.Unmarshal(body, &decoded))
	require.Equal(t, plan, decoded)
	slices.Reverse(selection.Messages)
	other, err := PrepareExportSelection(ctx, path, selection)
	require.NoError(t, err)
	require.Equal(t, plan, other)
	*selection.Messages[0].Text.Replacement = "caller mutation"
	selection.Channels[0].Label = "caller mutation"
	require.Equal(t, "reviewed reply", *plan.Messages[1].Text.Replacement)
	require.Equal(t, "C1", plan.Channels[0].Label)
	projection, err := ResolveExportSelection(ctx, path, decoded)
	require.NoError(t, err)
	require.Equal(t, ExportProjection{
		WorkspaceID: "T1", WorkspaceLabel: "T1", Channels: []ExportChannelSelection{{ChannelID: "C1", Label: "C1"}},
		Messages: []ExportMessage{
			{ChannelID: "C1", TS: "123.456", UserID: new("U1"), Text: "selected root", ThreadTS: new("123.456"), EditedTS: new("125.000")},
			{ChannelID: "C1", TS: "124.000", UserID: new("U1"), Text: "reviewed reply", ThreadTS: new("123.456"), EditedTS: new("")},
			{ChannelID: "C1", TS: "126.000", UserID: new("U1"), Text: "", ThreadTS: new(""), EditedTS: new("")},
		},
	}, projection)
	output, err := json.Marshal(projection)
	require.NoError(t, err)
	for _, private := range []string{"canary", "source_sha256", "workspace_sha256", "raw_json", "normalized_text", "subtype", "source_name", "deleted_ts", "reply_count", "updated_at"} {
		require.NotContains(t, string(output), private)
	}
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(output, &fields))
	require.Len(t, fields, 4)
	var messages []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(fields["messages"], &messages))
	for _, message := range messages {
		keys := make([]string, 0, len(message))
		for key := range message {
			keys = append(keys, key)
		}
		require.ElementsMatch(t, []string{"channel_id", "ts", "user_id", "text", "thread_ts", "edited_ts"}, keys)
	}
	// Labels and replacement choices are editable private input, not authority.
	decoded.WorkspaceLabel, decoded.Channels[0].Label = "Workspace", "Channel"
	decoded.Messages[1].Text.Replacement = new("second review")
	changed, err := ResolveExportSelection(ctx, path, decoded)
	require.NoError(t, err)
	require.Equal(t, "Workspace", changed.WorkspaceLabel)
	require.Equal(t, "Channel", changed.Channels[0].Label)
	require.Equal(t, "second review", changed.Messages[1].Text)
	decoded.Channels[0].Label = "later caller mutation"
	*decoded.Messages[1].Text.Replacement = "later caller mutation"
	require.Equal(t, "Channel", changed.Channels[0].Label)
	require.Equal(t, "second review", changed.Messages[1].Text)
	require.Equal(t, before, historyArchiveState(t, s))
}

func TestExportSelectionBindsStoredScalars(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"workspace-name", `update workspaces set name = 'changed'`},
		{"workspace-domain-null", `update workspaces set domain = null`},
		{"workspace-enterprise-null", `update workspaces set enterprise_id = null`},
		{"workspace-raw", `update workspaces set raw_json = '{"changed":true}'`},
		{"workspace-updated", `update workspaces set updated_at = 'changed'`},
		{"channel-name", `update channels set name = 'changed'`},
		{"channel-kind", `update channels set kind = 'public'`},
		{"channel-topic-null", `update channels set topic = null`},
		{"channel-purpose-null", `update channels set purpose = null`},
		{"channel-archived", `update channels set is_archived = 1`},
		{"channel-shared", `update channels set is_shared = 1`},
		{"channel-general", `update channels set is_general = 1`},
		{"channel-updated", `update channels set updated_at = 'changed'`},
		{"channel-raw", `update channels set raw_json = raw_json || ' '`},
		{"message-user-null", `update messages set user_id = null`},
		{"message-subtype-null", `update messages set subtype = null`},
		{"message-client-null", `update messages set client_msg_id = null`},
		{"message-thread-null", `update messages set thread_ts = null where ts = '126.000'`},
		{"message-parent-null", `update messages set parent_user_id = null`},
		{"message-text", `update messages set text = 'changed'`},
		{"message-reply-count", `update messages set reply_count = 7`},
		{"message-latest-null", `update messages set latest_reply = null`},
		{"message-edited-null", `update messages set edited_ts = null`},
		{"message-deleted-null", `update messages set deleted_ts = null`},
		{"message-source-rank", `update messages set source_rank = 9`},
		{"message-source-name", `update messages set source_name = 'provider-fixture'`},
		{"message-updated", `update messages set updated_at = 'changed'`},
		{"message-raw", `update messages set raw_json = '{"changed":true}'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, path, selection := selectionFixture(t)
			plan, err := PrepareExportSelection(context.Background(), path, selection)
			require.NoError(t, err)
			_, err = s.DB().Exec(tc.query)
			require.NoError(t, err)
			before := historyArchiveState(t, s)
			projection, err := ResolveExportSelection(context.Background(), path, plan)
			require.ErrorContains(t, err, "binding changed")
			require.Empty(t, projection)
			require.Equal(t, before, historyArchiveState(t, s))
		})
	}
}

func TestExportSelectionRejectsScalarChangeWithRetainedRaw(t *testing.T) {
	s, path, selection := selectionFixture(t)
	ctx := context.Background()
	plan, err := PrepareExportSelection(ctx, path, selection)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessage(ctx, store.Message{
		ChannelID: "C1", TS: "123.456", WorkspaceID: "T1", UserID: "U1", Text: "new scalar text", NormalizedText: "new scalar text",
		ThreadTS: "123.456", SourceRank: 9, SourceName: "provider-fixture", RawJSON: `{"new":"lower priority"}`, UpdatedAt: time.Unix(1710000001, 0),
	}, nil))
	var raw, text string
	require.NoError(t, s.DB().QueryRow(`select raw_json, text from messages where channel_id = 'C1' and ts = '123.456'`).Scan(&raw, &text))
	require.Equal(t, `{"text":"selection-raw-canary"}`, raw)
	require.Equal(t, "new scalar text", text)
	projection, err := ResolveExportSelection(ctx, path, plan)
	require.ErrorContains(t, err, "binding changed")
	require.Empty(t, projection)
}

func TestExportSelectionIgnoresUnselectedAndDerivedRows(t *testing.T) {
	s, path, selection := selectionFixture(t)
	ctx := context.Background()
	plan, err := PrepareExportSelection(ctx, path, selection)
	require.NoError(t, err)
	_, err = s.DB().Exec(`update messages set normalized_text = 'derived change'; update messages set text = 'unselected change' where ts = '127.000'; update users set display_name = 'private profile change'; update message_event_heads set payload_json = '{"private":"event change"}'; update sync_state set value = 'progress change'`)
	require.NoError(t, err)
	projection, err := ResolveExportSelection(ctx, path, plan)
	require.NoError(t, err)
	require.Len(t, projection.Messages, 3)
	require.Equal(t, "selected root", projection.Messages[0].Text)
}

func TestExportSelectionPreservesNullAndEmpty(t *testing.T) {
	s, path, selection := selectionFixture(t)
	_, err := s.DB().Exec(`update messages set user_id = null, thread_ts = null, edited_ts = null where ts = '126.000'`)
	require.NoError(t, err)
	plan, err := PrepareExportSelection(context.Background(), path, selection)
	require.NoError(t, err)
	projection, err := ResolveExportSelection(context.Background(), path, plan)
	require.NoError(t, err)
	require.Nil(t, projection.Messages[2].UserID)
	require.Nil(t, projection.Messages[2].ThreadTS)
	require.Nil(t, projection.Messages[2].EditedTS)
	require.Equal(t, new(""), projection.Messages[1].EditedTS)
	_, err = s.DB().Exec(`update messages set thread_ts = '' where ts = '126.000'`)
	require.NoError(t, err)
	_, err = ResolveExportSelection(context.Background(), path, plan)
	require.ErrorContains(t, err, "binding changed")
}

func TestExportSelectionReplacesInvalidSourceText(t *testing.T) {
	s, path, selection := selectionFixture(t)
	_, err := s.DB().Exec(`update messages set text = cast(x'ff' as text) where ts = '123.456'`)
	require.NoError(t, err)
	_, err = PrepareExportSelection(context.Background(), path, selection)
	require.ErrorContains(t, err, "invalid text encoding")
	selection.Messages[1].Text = ExportTextChoice{Mode: ExportTextReplace, Replacement: new("reviewed replacement")}
	plan, err := PrepareExportSelection(context.Background(), path, selection)
	require.NoError(t, err)
	projection, err := ResolveExportSelection(context.Background(), path, plan)
	require.NoError(t, err)
	require.Equal(t, "reviewed replacement", projection.Messages[0].Text)
	_, err = s.DB().Exec(`update messages set text = cast(x'fe' as text) where ts = '123.456'`)
	require.NoError(t, err)
	_, err = ResolveExportSelection(context.Background(), path, plan)
	require.ErrorContains(t, err, "binding changed")
}

func TestExportSelectionCurrentChannelEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, kind, raw string
		private, valid  bool
	}{
		{"api", "public_channel", selectionPublicRaw, false, true},
		{"import", "public", selectionPublicRaw, false, true},
		{"desktop", "desktop_channel", strings.ReplaceAll(selectionPublicRaw, `"context_team_id":"T1"`, `"context_team_id":""`), false, true},
		{"case", "public", strings.ReplaceAll(selectionPublicRaw, "is_channel", "IS_CHANNEL"), false, true},
		{"stored-private", "private_channel", selectionPublicRaw, false, false},
		{"stored-dm", "im", selectionPublicRaw, false, false},
		{"stored-unknown", "provider_channel", selectionPublicRaw, false, false},
		{"stored-private-flag", "public", selectionPublicRaw, true, false},
		{"mcp", "public", `{"id":"C1","name":"selection-dm-name-canary","kind":"public","is_private":false}`, false, false},
		{"missing-private", "public", strings.ReplaceAll(selectionPublicRaw, `"is_private":false,`, ""), false, false},
		{"missing-group", "public", strings.ReplaceAll(selectionPublicRaw, `"is_group":false,`, ""), false, false},
		{"missing-im", "public", strings.ReplaceAll(selectionPublicRaw, `"is_im":false,`, ""), false, false},
		{"missing-mpim", "public", strings.ReplaceAll(selectionPublicRaw, `"is_mpim":false,`, ""), false, false},
		{"no-channel", "public", strings.ReplaceAll(selectionPublicRaw, `"is_channel":true`, `"is_channel":false`), false, false},
		{"native-private", "public", strings.ReplaceAll(selectionPublicRaw, `"is_private":false`, `"is_private":true`), false, false},
		{"native-group", "public", strings.ReplaceAll(selectionPublicRaw, `"is_group":false`, `"is_group":true`), false, false},
		{"native-im", "public", strings.ReplaceAll(selectionPublicRaw, `"is_im":false`, `"is_im":true`), false, false},
		{"native-mpim", "public", strings.ReplaceAll(selectionPublicRaw, `"is_mpim":false`, `"is_mpim":true`), false, false},
		{"boolean-string", "public", strings.ReplaceAll(selectionPublicRaw, `"is_private":false`, `"is_private":"false"`), false, false},
		{"boolean-null", "public", strings.ReplaceAll(selectionPublicRaw, `"is_im":false`, `"is_im":null`), false, false},
		{"duplicate-type", "public", strings.ReplaceAll(selectionPublicRaw, `"is_im":false`, `"is_im":false,"IS_IM":false`), false, false},
		{"escaped-veto", "public", strings.ReplaceAll(selectionPublicRaw, `"is_im":false`, `"is_im":false,"is_\u0069m":true`), false, false},
		{"folded-veto", "public", strings.ReplaceAll(selectionPublicRaw, `"is_im":false`, `"is_im":false,"iſ_im":true`), false, false},
		{"identity", "public", strings.ReplaceAll(selectionPublicRaw, `"id":"C1"`, `"id":"selection-dm-name-canary"`), false, false},
		{"duplicate-identity", "public", strings.ReplaceAll(selectionPublicRaw, `"id":"C1"`, `"id":"C1","ID":"C1"`), false, false},
		{"alternate-identity", "public", strings.ReplaceAll(selectionPublicRaw, `"id":"C1"`, `"id":"C1","channel_id":"COTHER"`), false, false},
		{"workspace", "public", strings.ReplaceAll(selectionPublicRaw, `"context_team_id":"T1"`, `"context_team_id":"TOTHER"`), false, false},
		{"identity-null", "public", strings.ReplaceAll(selectionPublicRaw, `"context_team_id":"T1"`, `"context_team_id":null`), false, false},
		{"array", "public", "[" + selectionPublicRaw + "]", false, false},
		{"trailing", "public", selectionPublicRaw + " {}", false, false},
		{"malformed", "public", selectionPublicRaw[:len(selectionPublicRaw)-1], false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, path, selection := selectionFixture(t)
			plan, err := PrepareExportSelection(context.Background(), path, selection)
			require.NoError(t, err)
			_, err = s.DB().Exec(`update channels set kind = ?, is_private = ?, raw_json = ? where id = 'C1'`, tc.kind, tc.private, tc.raw)
			require.NoError(t, err)
			before := historyArchiveState(t, s)
			_, err = PrepareExportSelection(context.Background(), path, selection)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "current public-channel evidence")
				require.NotContains(t, err.Error(), "canary")
				projection, err := ResolveExportSelection(context.Background(), path, plan)
				require.Error(t, err)
				require.NotContains(t, err.Error(), "canary")
				require.Empty(t, projection)
			}
			require.Equal(t, before, historyArchiveState(t, s))
		})
	}
}

func TestExportSelectionRejectsUnavailableMessages(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"subtype-draft", `update messages set subtype = 'desktop_draft' where ts = '124.000'`},
		{"generic-draft", `update messages set subtype = 'draft' where ts = '124.000'`},
		{"draft-source", `update messages set source_name = 'desktop-draft' where ts = '124.000'`},
		{"deleted-ts", `update messages set deleted_ts = '128.000' where ts = '124.000'`},
		{"deleted-subtype", `update messages set subtype = 'message_deleted' where ts = '124.000'`},
		{"foreign-channel", `update channels set workspace_id = 'TOTHER' where id = 'C1'`},
		{"foreign-message", `update messages set workspace_id = 'TOTHER' where ts = '124.000'`},
		{"missing-workspace", `delete from workspaces where id = 'T1'`},
		{"missing-channel", `delete from channels where id = 'C1'`},
		{"missing-message", `delete from messages where ts = '124.000'; delete from message_fts where message_key = 'C1|124.000'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, path, selection := selectionFixture(t)
			plan, err := PrepareExportSelection(context.Background(), path, selection)
			require.NoError(t, err)
			_, err = s.DB().Exec(tc.query)
			require.NoError(t, err)
			before := historyArchiveState(t, s)
			_, err = PrepareExportSelection(context.Background(), path, selection)
			require.Error(t, err)
			projection, err := ResolveExportSelection(context.Background(), path, plan)
			require.Error(t, err)
			require.Empty(t, projection)
			require.Equal(t, before, historyArchiveState(t, s))
		})
	}
	t.Run("draft-key", func(t *testing.T) {
		s, path, selection := selectionFixture(t)
		require.NoError(t, s.UpsertMessage(context.Background(), store.Message{ChannelID: "C1", TS: "draft:1710000000", WorkspaceID: "T1", Text: "draft-canary", NormalizedText: "draft-canary", RawJSON: "{}", SourceRank: 4, SourceName: "api-bot", UpdatedAt: time.Unix(1710000000, 0)}, nil))
		selection.Messages = []ExportMessageSelection{{ChannelID: "C1", TS: "draft:1710000000", Text: ExportTextChoice{Mode: ExportTextKeep}}}
		_, err := PrepareExportSelection(context.Background(), path, selection)
		require.ErrorContains(t, err, "draft or deleted")
		require.NotContains(t, err.Error(), "canary")
	})
}

func TestExportSelectionRequiresSelectedRoot(t *testing.T) {
	for _, mode := range []string{"missing", "chain", "cycle", "other-channel", "deleted-parent", "self", "empty"} {
		t.Run(mode, func(t *testing.T) {
			s, path, selection := selectionFixture(t)
			ctx := context.Background()
			switch mode {
			case "missing":
				selection.Messages = selection.Messages[2:]
			case "chain":
				_, err := s.DB().Exec(`update messages set thread_ts = '126.000' where ts = '123.456'`)
				require.NoError(t, err)
			case "cycle":
				_, err := s.DB().Exec(`update messages set thread_ts = '124.000' where ts = '123.456'`)
				require.NoError(t, err)
			case "other-channel":
				require.NoError(t, s.UpsertChannel(ctx, store.Channel{ID: "C2", WorkspaceID: "T1", Name: "other", Kind: "public", RawJSON: strings.ReplaceAll(selectionPublicRaw, "C1", "C2"), UpdatedAt: time.Unix(1710000000, 0)}))
				require.NoError(t, s.UpsertMessage(ctx, store.Message{ChannelID: "C2", TS: "129.000", WorkspaceID: "T1", RawJSON: "{}", SourceName: "api-bot", SourceRank: 2, UpdatedAt: time.Unix(1710000000, 0)}, nil))
				_, err := s.DB().Exec(`update messages set thread_ts = '129.000' where ts = '124.000'`)
				require.NoError(t, err)
				selection.Channels = append(selection.Channels, ExportChannelSelection{ChannelID: "C2"})
				selection.Messages = append(selection.Messages, ExportMessageSelection{ChannelID: "C2", TS: "129.000", Text: ExportTextChoice{Mode: ExportTextKeep}})
			case "deleted-parent":
				_, err := s.DB().Exec(`update messages set deleted_ts = '130.000' where ts = '123.456'`)
				require.NoError(t, err)
			case "empty":
				_, err := s.DB().Exec(`update messages set thread_ts = '' where ts = '123.456'`)
				require.NoError(t, err)
			}
			plan, err := PrepareExportSelection(ctx, path, selection)
			if mode == "empty" || mode == "self" {
				require.NoError(t, err)
				projection, err := ResolveExportSelection(ctx, path, plan)
				require.NoError(t, err)
				require.Len(t, projection.Messages, 3)
			} else {
				require.Error(t, err)
				require.Empty(t, plan)
			}
		})
	}
}

func TestExportSelectionInputAndDiagnostics(t *testing.T) {
	for _, mode := range []string{"workspace", "channels", "messages", "duplicate-channel", "duplicate-message", "unselected-channel", "text-choice", "keep-with-replacement", "replace-missing", "version", "binding", "missing-binding"} {
		t.Run(mode, func(t *testing.T) {
			_, path, selection := selectionFixture(t)
			selection.WorkspaceLabel, selection.Channels[0].Label = "label-canary", "dm-name-canary"
			selection.Messages[0].Text.Replacement = new("replacement-canary")
			plan, err := PrepareExportSelection(context.Background(), path, selection)
			require.NoError(t, err)
			switch mode {
			case "workspace":
				selection.WorkspaceID = ""
			case "channels":
				selection.Channels = nil
			case "messages":
				selection.Messages = nil
			case "duplicate-channel":
				selection.Channels = append(selection.Channels, selection.Channels[0])
			case "duplicate-message":
				selection.Messages = append(selection.Messages, selection.Messages[0])
			case "unselected-channel":
				selection.Messages[0].ChannelID = "dm-id-canary"
			case "text-choice":
				selection.Messages[0].Text.Mode = "mode-canary"
			case "keep-with-replacement":
				selection.Messages[0].Text.Mode = ExportTextKeep
			case "replace-missing":
				selection.Messages[0].Text.Replacement = nil
			case "version":
				plan.Version++
			case "binding":
				plan.Messages[0].SourceSHA256 = "binding-canary"
			case "missing-binding":
				plan.Channels[0].SourceSHA256 = ""
			}
			if mode == "version" || mode == "binding" || mode == "missing-binding" {
				projection, resolveErr := ResolveExportSelection(context.Background(), path, plan)
				err = resolveErr
				require.Empty(t, projection)
			} else {
				_, err = PrepareExportSelection(context.Background(), path, selection)
			}
			require.Error(t, err)
			require.NotContains(t, err.Error(), "canary")
			require.NotContains(t, err.Error(), path)
		})
	}
}

func TestExportSelectionFileOnlyRead(t *testing.T) {
	_, _, selection := selectionFixture(t)
	for _, mode := range []string{"missing", "memory", "uri", "directory", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			missing := filepath.Join(dir, "absent.db")
			path := missing
			switch mode {
			case "memory":
				path = ":memory:"
			case "uri":
				path = "file:" + missing + "?mode=rwc"
			case "directory":
				path = dir
			case "invalid":
				path = filepath.Join(dir, "invalid.db")
				require.NoError(t, os.WriteFile(path, []byte("archive-content-canary"), 0o600))
			}
			_, err := PrepareExportSelection(context.Background(), path, selection)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "canary")
			require.NotContains(t, err.Error(), path)
			require.NoFileExists(t, missing)
		})
	}
}

func TestExportSelectionKeepsArchiveFileUnchanged(t *testing.T) {
	s, path, selection := selectionFixture(t)
	require.NoError(t, s.Close())
	require.NoError(t, os.Chmod(path, 0o400))
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	plan, err := PrepareExportSelection(context.Background(), path, selection)
	require.NoError(t, err)
	_, err = ResolveExportSelection(context.Background(), path, plan)
	require.NoError(t, err)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o400), info.Mode().Perm())
}

func TestExportSelectionUsesOneSnapshot(t *testing.T) {
	s, path, selection := selectionFixture(t)
	ctx := context.Background()
	plan, err := PrepareExportSelection(ctx, path, selection)
	require.NoError(t, err)
	reader, err := store.OpenReadOnly(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	tx, err := reader.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var id string
	require.NoError(t, tx.QueryRowContext(ctx, `select id from workspaces where id = 'T1'`).Scan(&id))
	_, err = s.DB().ExecContext(ctx, `update messages set text = 'changed after snapshot' where ts = '123.456'`)
	require.NoError(t, err)
	selection, err = canonicalExportSelection(selection)
	require.NoError(t, err)
	bound, projection, err := selectExportRows(ctx, tx, selection)
	require.NoError(t, err)
	require.Equal(t, plan, bound)
	require.Equal(t, "selected root", projection.Messages[0].Text)
	require.NoError(t, tx.Commit())
	_, err = ResolveExportSelection(ctx, path, plan)
	require.ErrorContains(t, err, "binding changed")
}
