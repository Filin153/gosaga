package gosaga

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"github.com/Filin153/gosaga/domain"
	"log/slog"
)

func Unmarshal(task *domain.SagaTask) (*domain.SagaMsg, error) {
	var msgData domain.SagaMsg
	err := json.Unmarshal(task.Data, &msgData)
	if err != nil {
		slog.Error("json.Unmarshal", "error", err.Error())
		return nil, err
	}

	return &msgData, nil
}

// generateIdempotencyKey produces a random hex string used to deduplicate tasks.
func (k *Saga) generateIdempotencyKey() (string, error) {
	bytes := make([]byte, 32)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	str := hex.EncodeToString(bytes)
	return str, nil
}
