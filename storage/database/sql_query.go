package database

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
)

// GenerateUpdateQueryById builds an UPDATE ... SET ... WHERE id=$N query with positional args.
// Accepts either map[string]any or struct (optionally pointer) with exported fields.
// For structs, it uses `db` tag if present or falls back to snake_case field name.
// Pointer fields are skipped when nil.
func GenerateUpdateQueryById(table string, id any, values any) (string, []any, error) {
	updates, err := buildUpdateMap(values)
	if err != nil {
		return "", nil, err
	}

	if len(updates) == 0 {
		return "", nil, fmt.Errorf("no fields to update")
	}

	setParts := make([]string, 0, len(updates))
	args := make([]any, 0, len(updates)+1)
	i := 1

	for k, v := range updates {
		col := pgx.Identifier{k}.Sanitize()
		setParts = append(setParts, fmt.Sprintf(`%s = $%d`, col, i))
		args = append(args, v)
		i++
	}

	args = append(args, id)

	query := fmt.Sprintf(`UPDATE %s SET %s WHERE "id" = $%d`,
		pgx.Identifier{table}.Sanitize(),
		strings.Join(setParts, ", "),
		i,
	)

	return query, args, nil
}

func buildUpdateMap(values any) (map[string]any, error) {
	if values == nil {
		return nil, fmt.Errorf("no fields to update")
	}

	if m, ok := values.(map[string]any); ok {
		return m, nil
	}

	val := reflect.ValueOf(values)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("no fields to update")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("unsupported update type %T", values)
	}

	typ := val.Type()
	result := make(map[string]any, typ.NumField())

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" { // unexported
			continue
		}

		key := field.Tag.Get("db")
		if key == "" {
			key = toSnake(field.Name)
		}

		fv := val.Field(i)
		if fv.Kind() == reflect.Ptr {
			if fv.IsNil() {
				continue
			}
			result[key] = fv.Elem().Interface()
			continue
		}

		result[key] = fv.Interface()
	}

	return result, nil
}

func toSnake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
