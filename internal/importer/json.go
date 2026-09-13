package importer

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
)

// Check before projection: encoding/json otherwise discards earlier duplicate
// evidence. Catalog struct fields also accept case-insensitive spellings.
func validateUniqueJSONKeys(blob []byte, catalog bool) error {
	invalid := errors.New("invalid or ambiguous export JSON object keys; include_dms=false requires unique keys")
	if !json.Valid(blob) {
		return invalid
	}
	decoder := json.NewDecoder(bytes.NewReader(blob))
	decoder.UseNumber()
	var value func(int) error
	value = func(depth int) error {
		token, err := decoder.Token()
		if err != nil {
			return invalid
		}
		delim, container := token.(json.Delim)
		if !container {
			return nil
		}
		keys := map[string]bool{}
		for decoder.More() {
			if delim == '{' {
				token, err := decoder.Token()
				key, ok := token.(string)
				if err != nil || !ok {
					return invalid
				}
				if catalog && depth == 0 {
					key = canonicalCatalogKey(key)
				}
				if keys[key] {
					return invalid
				}
				keys[key] = true
			}
			if err := value(depth + 1); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return invalid
		}
		return nil
	}
	return value(0)
}

func canonicalCatalogKey(key string) string {
	for _, field := range []string{"id", "name", "is_private", "is_channel", "is_group", "is_im", "is_mpim"} {
		if strings.EqualFold(key, field) {
			return field
		}
	}
	return key
}
