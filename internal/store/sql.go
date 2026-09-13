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
	trimmed := strings.TrimSpace(query)
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
		case '\'':
			i = scanSQLQuoted(query, i, '\'')
		case '"':
			i = scanSQLQuoted(query, i, '"')
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
		}
	}
	return false
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
		if query[i] == '\n' || query[i] == '\r' {
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
			end := strings.IndexAny(query, "\r\n")
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
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func stringifyDBValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	default:
		return typed
	}
}
