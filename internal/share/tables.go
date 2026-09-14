package share

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func exportTable(ctx context.Context, db *sql.DB, dataDir, table string) (TableManifest, error) {
	rows, err := db.QueryContext(ctx, "select * from "+quoteIdent(table)) //nolint:gosec // Table names are emitted through quoteIdent from export metadata.
	if err != nil {
		return TableManifest{}, fmt.Errorf("query %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return TableManifest{}, fmt.Errorf("columns %s: %w", table, err)
	}
	tableDir := filepath.Join(dataDir, table)
	if err := os.MkdirAll(tableDir, 0o750); err != nil {
		return TableManifest{}, fmt.Errorf("mkdir %s: %w", table, err)
	}
	writer := tableShardWriter{dataDir: dataDir, table: table}
	if err := writer.open(); err != nil {
		return TableManifest{}, err
	}
	defer func() { _ = writer.close() }()

	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	count := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return TableManifest{}, fmt.Errorf("scan %s: %w", table, err)
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			row[column] = exportValue(values[i])
		}
		if isLocalSyncProgress(table, row) {
			continue
		}
		body, err := json.Marshal(row)
		if err != nil {
			return TableManifest{}, fmt.Errorf("marshal %s row: %w", table, err)
		}
		if err := writer.rotateIfNeeded(); err != nil {
			return TableManifest{}, err
		}
		if _, err := writer.Write(body); err != nil {
			return TableManifest{}, fmt.Errorf("write %s row: %w", table, err)
		}
		if _, err := writer.Write([]byte{'\n'}); err != nil {
			return TableManifest{}, fmt.Errorf("write %s newline: %w", table, err)
		}
		count++
		if err := writer.finishRow(); err != nil {
			return TableManifest{}, err
		}
	}
	if err := rows.Err(); err != nil {
		return TableManifest{}, fmt.Errorf("iterate %s: %w", table, err)
	}
	if err := writer.close(); err != nil {
		return TableManifest{}, err
	}
	return TableManifest{Name: table, Files: writer.files, Columns: columns, Rows: count}, nil
}

func tableManifestFiles(table TableManifest) []string {
	if len(table.Files) > 0 {
		return table.Files
	}
	if strings.TrimSpace(table.File) != "" {
		return []string{table.File}
	}
	return nil
}

type mergeImportState struct {
	rejectedMessages          map[string]struct{}
	retentionRejectedMessages map[string]struct{}
	importedFiles             map[string]struct{}
}

func importTable(ctx context.Context, tx *sql.Tx, repoPath string, table TableManifest, restore, includeMedia bool, state *mergeImportState) (int, error) {
	files := tableManifestFiles(table)
	columns := append([]string{}, table.Columns...)
	legacyColumns := map[string][]string{
		"message_files":    {"deleted_at", "deletion_source", "deletion_reason"},
		"message_events":   {"event_key"},
		"message_mentions": {"deleted_at", "deletion_source", "deletion_reason", "updated_at"},
	}[table.Name]
	for _, column := range legacyColumns {
		if !containsString(columns, column) {
			columns = append(columns, column)
		}
	}
	if !restore && table.Name == "message_events" {
		columns = withoutString(columns, "id")
	}
	statement := insertSQL(table.Name, columns)
	if table.Name == "message_events" {
		statement += " on conflict(event_key) do nothing"
	} else if !restore {
		statement = mergeSQL(table.Name, columns)
	}
	var stmt *sql.Stmt
	var err error
	if statement != "" {
		stmt, err = tx.PrepareContext(ctx, statement)
	}
	if err != nil {
		return 0, fmt.Errorf("prepare import %s: %w", table.Name, err)
	}
	if stmt != nil {
		defer func() { _ = stmt.Close() }()
	}
	rows := 0
	for _, rel := range files {
		count, err := importTableFile(ctx, tx, stmt, columns, repoPath, table, rel, restore, includeMedia, state)
		if err != nil {
			return 0, err
		}
		rows += count
	}
	return rows, nil
}

func importTableFile(ctx context.Context, tx *sql.Tx, stmt *sql.Stmt, columns []string, repoPath string, table TableManifest, rel string, restore, includeMedia bool, state *mergeImportState) (int, error) {
	path, err := resolveManifestTableFile(repoPath, table.Name, rel)
	if err != nil {
		return 0, fmt.Errorf("manifest table %s file %q: %w", table.Name, rel, err)
	}
	file, err := os.Open(path) //nolint:gosec // Import reads files from the configured backup repo.
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", rel, err)
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return 0, fmt.Errorf("read gzip %s: %w", rel, err)
	}
	defer func() { _ = gz.Close() }()
	dec := json.NewDecoder(gz)
	dec.UseNumber()
	rows := 0
	for {
		row := map[string]any{}
		err := dec.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("decode %s: %w", rel, err)
		}
		if isLocalSyncProgress(table.Name, row) {
			// Discard foreign progress without bypassing the required-field and
			// SQL scalar checks that insertion previously enforced.
			for _, column := range []string{"source_name", "entity_type", "entity_id", "value", "updated_at"} {
				value := importValue(row[column])
				if value == nil || !driver.IsValue(value) {
					return 0, fmt.Errorf("invalid sync_state history checkpoint field %s", column)
				}
			}
			rows++
			continue
		}
		if table.Name == "message_files" && !includeMedia {
			row["media_path"] = nil
			row["content_sha256"] = nil
			row["content_size"] = json.Number("0")
			row["fetched_at"] = nil
			row["fetch_status"] = ""
			row["fetch_error"] = ""
		}
		if (table.Name == "message_files" || table.Name == "message_mentions") && !containsString(table.Columns, "deleted_at") {
			if err := synthesizeLegacySubordinateTombstone(ctx, tx, row); err != nil {
				return 0, err
			}
		}
		if table.Name == "message_mentions" && snapshotStringValue(row["updated_at"]) == "" {
			var parentUpdated string
			err := tx.QueryRowContext(ctx, `select updated_at from messages where channel_id = ? and ts = ?`, row["channel_id"], row["ts"]).Scan(&parentUpdated)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return 0, fmt.Errorf("read legacy mention parent version: %w", err)
			}
			row["updated_at"] = parentUpdated
		}
		if table.Name == "message_events" && snapshotStringValue(row["event_key"]) == "" {
			row["event_key"] = legacyMessageEventKey(row)
		}
		if !restore {
			apply, err := shouldMergeSnapshotRow(ctx, tx, table.Name, row, state)
			if err != nil {
				return 0, err
			}
			if !apply {
				rows++
				continue
			}
		}
		values := make([]any, len(columns))
		for i, column := range columns {
			values[i] = importValue(row[column])
		}
		if stmt != nil {
			if _, err := stmt.ExecContext(ctx, values...); err != nil {
				return 0, fmt.Errorf("insert %s: %w", table.Name, err)
			}
		}
		if !restore && table.Name == "message_files" {
			state.importedFiles[fileMediaKey(snapshotStringValue(row["channel_id"]), snapshotStringValue(row["ts"]), snapshotStringValue(row["file_id"]))] = struct{}{}
		}
		rows++
	}
	return rows, nil
}

func isLocalSyncProgress(table string, row map[string]any) bool {
	if table != "sync_state" {
		return false
	}
	return (row["entity_type"] == "history_coverage_v1" && (row["source_name"] == "api-bot" || row["source_name"] == "api-user")) ||
		(row["entity_type"] == "thread_pending_v1" && (row["source_name"] == "api-user" || row["source_name"] == "mcp")) ||
		(row["entity_type"] == "history_work_v1" && row["source_name"] == "mcp")
}

func synthesizeLegacySubordinateTombstone(ctx context.Context, tx *sql.Tx, row map[string]any) error {
	var parentUpdated, parentSource string
	var parentDeleted bool
	err := tx.QueryRowContext(ctx, `
select updated_at, source_name, trim(coalesce(deleted_ts, '')) <> ''
from messages
where channel_id = ? and ts = ?
`, importValue(row["channel_id"]), importValue(row["ts"])).Scan(&parentUpdated, &parentSource, &parentDeleted)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read legacy subordinate parent tombstone: %w", err)
	}
	if !parentDeleted {
		return nil
	}
	row["deleted_at"] = parentUpdated
	row["deletion_source"] = parentSource
	row["deletion_reason"] = "parent_message_deleted"
	row["updated_at"] = parentUpdated
	return nil
}

func legacyMessageEventKey(row map[string]any) string {
	parts := []string{
		snapshotStringValue(row["id"]),
		snapshotStringValue(row["channel_id"]),
		snapshotStringValue(row["ts"]),
		snapshotStringValue(row["event_type"]),
		snapshotStringValue(row["source_name"]),
		snapshotStringValue(row["payload_json"]),
		snapshotStringValue(row["created_at"]),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "legacy-" + hex.EncodeToString(sum[:])
}

func resolveManifestTableFile(repoPath string, tableName string, rel string) (string, error) {
	rel = strings.TrimSpace(rel)
	if rel == "" {
		return "", errors.New("empty path")
	}
	if filepath.IsAbs(rel) || filepath.IsAbs(filepath.FromSlash(rel)) {
		return "", errors.New("absolute path")
	}
	cleanRel := filepath.Clean(filepath.FromSlash(rel))
	if cleanRel == "." || cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes share repo")
	}
	expectedPrefix := filepath.Join("tables", tableName) + string(filepath.Separator)
	if !strings.HasPrefix(cleanRel, expectedPrefix) {
		return "", fmt.Errorf("path is outside tables/%s", tableName)
	}
	basePath := filepath.Join(repoPath, "tables", tableName)
	filePath := filepath.Join(repoPath, cleanRel)
	repoEval, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		return "", fmt.Errorf("resolve share repo: %w", err)
	}
	baseEval, err := filepath.EvalSymlinks(basePath)
	if err != nil {
		return "", fmt.Errorf("resolve table dir: %w", err)
	}
	if !pathWithin(repoEval, baseEval) {
		return "", errors.New("path escapes share repo")
	}
	fileEval, err := filepath.EvalSymlinks(filePath)
	if err != nil {
		return "", fmt.Errorf("resolve file: %w", err)
	}
	if !pathWithin(baseEval, fileEval) {
		return "", errors.New("path escapes table directory")
	}
	info, err := os.Stat(fileEval)
	if err != nil {
		return "", err
	}
	if info.IsDir() {
		return "", errors.New("path is a directory")
	}
	return fileEval, nil
}

func pathWithin(root string, target string) bool {
	relative, err := filepath.Rel(root, target)
	if err != nil {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}

func tableColumns(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "select * from "+quoteIdent(table)+" limit 0") //nolint:gosec // Table names are validated against SnapshotTables before querying.
	if err != nil {
		return nil, fmt.Errorf("read %s columns: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read %s columns: %w", table, err)
	}
	return columns, nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func compatibleTableColumns(table string, imported, current []string) bool {
	if sameStrings(imported, current) {
		return true
	}
	optional := map[string]map[string]struct{}{
		"message_files": {
			"deleted_at": {}, "deletion_source": {}, "deletion_reason": {},
		},
		"message_mentions": {
			"deleted_at": {}, "deletion_source": {}, "deletion_reason": {}, "updated_at": {},
		},
		"message_events": {"event_key": {}},
	}[table]
	if len(optional) == 0 {
		return false
	}
	currentSet := make(map[string]struct{}, len(current))
	for _, column := range current {
		currentSet[column] = struct{}{}
	}
	importedSet := make(map[string]struct{}, len(imported))
	for _, column := range imported {
		if _, ok := currentSet[column]; !ok {
			return false
		}
		if _, duplicate := importedSet[column]; duplicate {
			return false
		}
		importedSet[column] = struct{}{}
	}
	for _, column := range current {
		if _, ok := importedSet[column]; ok {
			continue
		}
		if _, ok := optional[column]; !ok {
			return false
		}
	}
	return true
}

type tableShardWriter struct {
	dataDir     string
	table       string
	nextShard   int
	rowsInShard int
	files       []string
	file        *os.File
	counter     *countingWriter
	gz          *gzip.Writer
}

func (w *tableShardWriter) open() error {
	rel := filepath.ToSlash(filepath.Join("tables", w.table, fmt.Sprintf("%06d.jsonl.gz", w.nextShard)))
	path := filepath.Join(w.dataDir, w.table, fmt.Sprintf("%06d.jsonl.gz", w.nextShard))
	file, err := os.Create(path) //nolint:gosec // Export creates files below the configured backup repo.
	if err != nil {
		return fmt.Errorf("create %s: %w", rel, err)
	}
	w.nextShard++
	w.rowsInShard = 0
	w.files = append(w.files, rel)
	w.file = file
	w.counter = &countingWriter{w: file}
	w.gz = gzip.NewWriter(w.counter)
	return nil
}

func (w *tableShardWriter) Write(p []byte) (int, error) {
	return w.gz.Write(p)
}

func (w *tableShardWriter) rotateIfNeeded() error {
	if maxShardBytes <= 0 || w.rowsInShard == 0 || w.counter.n < maxShardBytes {
		return nil
	}
	if err := w.close(); err != nil {
		return err
	}
	return w.open()
}

func (w *tableShardWriter) finishRow() error {
	w.rowsInShard++
	if maxShardBytes > 1024*1024 && w.rowsInShard%shardFlushRows != 0 {
		return nil
	}
	if err := w.gz.Flush(); err != nil {
		return fmt.Errorf("flush %s shard: %w", w.table, err)
	}
	return nil
}

func (w *tableShardWriter) close() error {
	var closeErr error
	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			closeErr = err
		}
		w.gz = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		w.file = nil
	}
	if closeErr != nil {
		return fmt.Errorf("close %s shard: %w", w.table, closeErr)
	}
	return nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func exportValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

func importValue(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v.String(), 64); err == nil {
			return f
		}
		return v.String()
	default:
		return v
	}
}

func insertSQL(table string, columns []string) string {
	quoted := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quoteIdent(column)
		placeholders[i] = "?"
	}
	return "insert into " + quoteIdent(table) + "(" + strings.Join(quoted, ",") + ") values(" + strings.Join(placeholders, ",") + ")"
}

func mergeSQL(table string, columns []string) string {
	if table == "embedding_jobs" {
		return ""
	}
	if table == "message_events" {
		return insertSQL(table, columns) + " on conflict(event_key) do nothing"
	}
	if table == "sync_state" {
		return insertSQL(table, columns) + " on conflict do nothing"
	}
	primaryKeys := map[string]map[string]struct{}{
		"workspaces":       {"id": {}},
		"channels":         {"id": {}},
		"users":            {"id": {}},
		"messages":         {"channel_id": {}, "ts": {}},
		"message_files":    {"channel_id": {}, "ts": {}, "file_id": {}},
		"message_mentions": {"channel_id": {}, "ts": {}, "mention_type": {}, "target_id": {}},
	}
	keys, ok := primaryKeys[table]
	if !ok {
		return insertSQL(table, columns) + " on conflict do nothing"
	}
	updates := make([]string, 0, len(columns))
	for _, column := range columns {
		if _, key := keys[column]; key {
			continue
		}
		quoted := quoteIdent(column)
		updates = append(updates, quoted+"=excluded."+quoted)
	}
	if len(updates) == 0 {
		return insertSQL(table, columns) + " on conflict do nothing"
	}
	sort.Strings(updates)
	return insertSQL(table, columns) + " on conflict do update set " + strings.Join(updates, ",")
}
