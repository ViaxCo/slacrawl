package store

import (
	"context"
	"errors"
	"strings"
)

func (s *Store) QueryReadOnly(ctx context.Context, query string) ([]map[string]any, error) {
	if err := validateReadOnlyQuery(query); err != nil {
		return nil, err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "pragma query_only = on"); err != nil {
		return nil, err
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "pragma query_only = off")
	}()
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	seenColumns := make(map[string]bool, len(cols))
	for _, column := range cols {
		if seenColumns[column] {
			return nil, errors.New("SQL result contains duplicate column names; use unique AS aliases")
		}
		seenColumns[column] = true
	}
	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, col := range cols {
			row[col] = stringifyDBValue(values[i])
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func validateReadOnlyQuery(query string) error {
	trimmed := stripSQLLeadingComments(query)
	if !startsWithSQLKeyword(trimmed, "select") && !startsWithSQLKeyword(trimmed, "with") {
		return errors.New("only read-only select statements are allowed")
	}
	if hasAdditionalSQLStatement(trimmed) {
		return errors.New("only a single read-only select statement is allowed")
	}
	return nil
}

func startsWithSQLKeyword(query, keyword string) bool {
	if len(query) < len(keyword) {
		return false
	}
	if !strings.EqualFold(query[:len(keyword)], keyword) {
		return false
	}
	return len(query) == len(keyword) || !isSQLIdentChar(query[len(keyword)])
}

func hasAdditionalSQLStatement(query string) bool {
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '\'', '"', '`':
			i = scanSQLQuoted(query, i, query[i])
		case '[':
			end := strings.IndexByte(query[i+1:], ']')
			if end < 0 {
				return false // SQLite will reject the unterminated identifier.
			}
			i += end + 1
		case '$', ':', '@':
			i = scanSQLParameter(query, i)
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				i = scanSQLLineComment(query, i+2)
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				i = scanSQLBlockComment(query, i+2)
			}
		case ';':
			return strings.TrimSpace(stripSQLLeadingComments(query[i+1:])) != ""
		default:
			if isSQLIdentChar(query[i]) {
				for i+1 < len(query) && isSQLIdentChar(query[i+1]) {
					i++
				}
			}
		}
	}
	return false
}

// SQLite's named parameters can include :: and an opaque parenthesized suffix.
// Quotes, comment markers and semicolons within that suffix are not SQL tokens.
func scanSQLParameter(query string, start int) int {
	seenName := false
	for i := start + 1; i < len(query); {
		switch {
		case isSQLIdentChar(query[i]):
			seenName = true
			i++
		case query[i] == ':' && i+1 < len(query) && query[i+1] == ':':
			i += 2
		case query[i] == '(' && seenName:
			for i++; i < len(query) && query[i] != ')' && query[i] != 0 && !isSQLSpace(query[i]); i++ {
			}
			if i < len(query) && query[i] == ')' {
				i++
			}
			return i - 1
		default:
			return i - 1
		}
	}
	return len(query) - 1
}

func scanSQLQuoted(query string, start int, quote byte) int {
	for i := start + 1; i < len(query); i++ {
		if query[i] != quote {
			continue
		}
		if i+1 < len(query) && query[i+1] == quote {
			i++
			continue
		}
		return i
	}
	return len(query) - 1
}

func scanSQLLineComment(query string, start int) int {
	for i := start; i < len(query); i++ {
		// SQLite line comments end at LF; CR remains inside the comment.
		if query[i] == '\n' {
			return i
		}
	}
	return len(query) - 1
}

func scanSQLBlockComment(query string, start int) int {
	for i := start; i+1 < len(query); i++ {
		if query[i] == '*' && query[i+1] == '/' {
			return i + 1
		}
	}
	return len(query) - 1
}

func stripSQLLeadingComments(query string) string {
	for {
		query = strings.TrimSpace(query)
		switch {
		case strings.HasPrefix(query, "--"):
			end := strings.IndexByte(query, '\n')
			if end < 0 {
				return ""
			}
			query = query[end+1:]
		case strings.HasPrefix(query, "/*"):
			end := strings.Index(query[2:], "*/")
			if end < 0 {
				return ""
			}
			query = query[end+4:]
		default:
			return query
		}
	}
}

func isSQLIdentChar(c byte) bool {
	return c == '_' || c == '$' || c >= 0x80 || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isSQLSpace(c byte) bool {
	return c == ' ' || c >= '\t' && c <= '\r'
}

func stringifyDBValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	default:
		return typed
	}
}
