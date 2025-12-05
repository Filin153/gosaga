package gosaga

import (
	"encoding/json"
	"testing"

	"github.com/Filin153/gosaga/domain"
)

func TestUnmarshal_Success(t *testing.T) {
	task := &domain.SagaTask{
		Data: mustJSON(t, domain.SagaMsg{
			Key:   "k",
			Value: "v",
			Topic: "t",
		}),
	}

	got, err := Unmarshal(task)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if got.Key != "k" || got.Topic != "t" {
		t.Fatalf("Unmarshal() = %+v, want key=k topic=t", got)
	}
	if got.Value != "v" {
		t.Fatalf("Unmarshal().Value = %v, want %v", got.Value, "v")
	}
}

func TestUnmarshal_InvalidJSON(t *testing.T) {
	task := &domain.SagaTask{
		Data: []byte(`{invalid json`),
	}

	if _, err := Unmarshal(task); err == nil {
		t.Fatalf("Unmarshal() error = nil, want non-nil")
	}
}

func TestGenerateIdempotencyKey(t *testing.T) {
	var s Saga

	key1, err := s.generateIdempotencyKey()
	if err != nil {
		t.Fatalf("generateIdempotencyKey() error = %v", err)
	}
	if len(key1) == 0 {
		t.Fatalf("generateIdempotencyKey() returned empty key")
	}

	key2, err := s.generateIdempotencyKey()
	if err != nil {
		t.Fatalf("generateIdempotencyKey() second call error = %v", err)
	}
	if key1 == key2 {
		t.Fatalf("generateIdempotencyKey() produced equal keys on two calls")
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal error: %v", err)
	}
	return b
}
