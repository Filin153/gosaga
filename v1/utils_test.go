package gosaga

import (
	"encoding/hex"
	"errors"
	"sync"
	"testing"

	"github.com/Filin153/gosaga/domain"
	"github.com/stretchr/testify/require"
)

func TestUnmarshal(t *testing.T) {
	data, _ := domain.EncodeSagaMsg(&domain.SagaMsg{Key: "k", Value: []byte(`{"a":1}`), Topic: "t"})
	task := domain.SagaTask{Data: data}
	msg, err := Unmarshal(&task)
	require.NoError(t, err)
	require.Equal(t, "k", msg.Key)
	require.Equal(t, "t", msg.Topic)
}

func TestUnmarshalError(t *testing.T) {
	task := domain.SagaTask{Data: []byte{0x02}}
	_, err := Unmarshal(&task)
	require.Error(t, err)
}

func TestGenerateIdempotencyKey(t *testing.T) {
	s := &Saga{}
	key, err := s.generateIdempotencyKey()
	require.NoError(t, err)
	require.Len(t, key, 64)
	_, err = hex.DecodeString(key)
	require.NoError(t, err)
}

func TestGenerateIdempotencyKeyError(t *testing.T) {
	oldRand := randReader
	randReader = func(b []byte) (int, error) { return 0, errors.New("boom") }
	defer func() { randReader = oldRand }()

	s := &Saga{}
	_, err := s.generateIdempotencyKey()
	require.EqualError(t, err, "boom")
}

func TestGenerateIdempotencyKeyParallel5000(t *testing.T) {
	s := &Saga{}
	const count = 5000
	keys := make([]string, count)

	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(idx int) {
			defer wg.Done()
			key, err := s.generateIdempotencyKey()
			require.NoError(t, err)
			keys[idx] = key
		}(i)
	}
	wg.Wait()

	seen := make(map[string]struct{}, count)
	for _, k := range keys {
		if _, ok := seen[k]; ok {
			t.Fatalf("duplicate key: %s", k)
		}
		seen[k] = struct{}{}
		require.Len(t, k, 64)
	}
}
