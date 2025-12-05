package database

import (
	"testing"
)

type testUpdateStruct struct {
	ID    int64   `db:"id"`
	Name  string  `db:"name"`
	Count *int    `db:"cnt"`
	Note  *string // no db tag, should be snake_case "note"
	skip  string  // unexported, must be ignored
}

func TestGenerateUpdateQueryById_FromStruct(t *testing.T) {
	cnt := 10
	note := "hello"
	u := testUpdateStruct{
		ID:    1,
		Name:  "foo",
		Count: &cnt,
		Note:  &note,
	}

	q, args, err := GenerateUpdateQueryById("go_saga_in_task", int64(42), u)
	if err != nil {
		t.Fatalf("GenerateUpdateQueryById() error = %v", err)
	}

	if q == "" {
		t.Fatalf("GenerateUpdateQueryById() returned empty query")
	}
	if len(args) != 5 {
		t.Fatalf("GenerateUpdateQueryById() args len = %d, want 5", len(args))
	}
	if args[len(args)-1] != int64(42) {
		t.Fatalf("GenerateUpdateQueryById() last arg = %v, want 42", args[len(args)-1])
	}
}

func TestGenerateUpdateQueryById_FromMap(t *testing.T) {
	values := map[string]any{
		"name": "bar",
		"cnt":  5,
	}

	q, args, err := GenerateUpdateQueryById("go_saga_in_task", int64(7), values)
	if err != nil {
		t.Fatalf("GenerateUpdateQueryById() error = %v", err)
	}
	if q == "" {
		t.Fatalf("GenerateUpdateQueryById() returned empty query")
	}
	if len(args) != 3 {
		t.Fatalf("GenerateUpdateQueryById() args len = %d, want 3", len(args))
	}
	if args[len(args)-1] != int64(7) {
		t.Fatalf("GenerateUpdateQueryById() last arg = %v, want 7", args[len(args)-1])
	}
}

func TestGenerateUpdateQueryById_NoFields(t *testing.T) {
	_, _, err := GenerateUpdateQueryById("t", 1, struct{}{})
	if err == nil {
		t.Fatalf("GenerateUpdateQueryById() error = nil, want non-nil")
	}
}

func TestGenerateUpdateQueryById_UnsupportedType(t *testing.T) {
	_, _, err := GenerateUpdateQueryById("t", 1, 123)
	if err == nil {
		t.Fatalf("GenerateUpdateQueryById() error = nil, want non-nil")
	}
}

func TestToSnake(t *testing.T) {
	tests := map[string]string{
		"Name":        "name",
		"HTTPServer":  "h_t_t_p_server",
		"SimpleField": "simple_field",
		"x":           "x",
		"":            "",
	}

	for in, want := range tests {
		if got := toSnake(in); got != want {
			t.Errorf("toSnake(%q) = %q, want %q", in, got, want)
		}
	}
}
