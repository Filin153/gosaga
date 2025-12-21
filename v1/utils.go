package gosaga

import (
	"encoding/hex"
	"log/slog"

	"github.com/Filin153/gosaga/domain"
)

func Unmarshal(task *domain.SagaTask) (*domain.SagaMsg, error) {
	msgData, err := domain.DecodeSagaMsg(task.Data)
	if err != nil {
		slog.Error("DecodeSagaMsg", "error", err.Error())
		return nil, err
	}

	return msgData, nil
}

// generateIdempotencyKey produces a random hex string used to deduplicate tasks.
func (k *Saga) generateIdempotencyKey() (string, error) {
	bytes := make([]byte, 32)
	_, err := randReader(bytes)
	if err != nil {
		return "", err
	}
	str := hex.EncodeToString(bytes)
	return str, nil
}
