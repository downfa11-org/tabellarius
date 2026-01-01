package util

import (
	"encoding/json"
	"os"
)

func SaveJSON[T any](path string, v T) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

func LoadJSON[T any](path string) (T, bool) {
	var zero T

	b, err := os.ReadFile(path)
	if err != nil {
		return zero, false
	}

	if err := json.Unmarshal(b, &zero); err != nil {
		return zero, false
	}

	return zero, true
}
