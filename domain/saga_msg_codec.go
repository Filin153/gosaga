package domain

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const sagaMsgVersion byte = 1

var errInvalidSagaMsg = errors.New("invalid saga msg encoding")

// EncodeSagaMsg packs SagaMsg into a binary format for storage and transport.
func EncodeSagaMsg(msg *SagaMsg) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("nil saga msg")
	}

	key := []byte(msg.Key)
	topic := []byte(msg.Topic)
	value := msg.Value
	if value == nil {
		value = []byte{}
	}

	const maxFieldLen = int(^uint32(0))
	if len(key) > maxFieldLen || len(topic) > maxFieldLen || len(value) > maxFieldLen {
		return nil, fmt.Errorf("saga msg field too large")
	}

	total := 1 + 4 + len(key) + 4 + len(topic) + 4 + len(value)
	buf := make([]byte, 0, total)
	buf = append(buf, sagaMsgVersion)
	buf = appendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	buf = appendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	buf = appendUint32(buf, uint32(len(value)))
	buf = append(buf, value...)
	return buf, nil
}

// DecodeSagaMsg unpacks SagaMsg from binary format.
func DecodeSagaMsg(data []byte) (*SagaMsg, error) {
	if len(data) == 0 {
		return nil, errors.New("empty saga msg data")
	}
	if data[0] != sagaMsgVersion {
		return nil, errInvalidSagaMsg
	}

	idx := 1
	key, err := readField(data, &idx)
	if err != nil {
		return nil, err
	}
	topic, err := readField(data, &idx)
	if err != nil {
		return nil, err
	}
	value, err := readField(data, &idx)
	if err != nil {
		return nil, err
	}
	if idx != len(data) {
		return nil, errInvalidSagaMsg
	}

	return &SagaMsg{
		Key:   string(key),
		Topic: string(topic),
		Value: value,
	}, nil
}

func appendUint32(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}

func readField(data []byte, idx *int) ([]byte, error) {
	if *idx+4 > len(data) {
		return nil, errInvalidSagaMsg
	}
	length := binary.BigEndian.Uint32(data[*idx : *idx+4])
	*idx += 4
	if *idx+int(length) > len(data) {
		return nil, errInvalidSagaMsg
	}
	field := data[*idx : *idx+int(length)]
	*idx += int(length)
	return field, nil
}
