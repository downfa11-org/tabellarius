package util

import "time"

func GenerateID() uint32 {
	return uint32(time.Now().UnixNano() % 0xFFFFFFFF)
}
