package model

import "fmt"

type Offset interface {
	Compare(other Offset) int
	String() string
}

type MySQLOffset struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
}

func (o MySQLOffset) Compare(other Offset) int {
	o2, ok := other.(MySQLOffset)
	if !ok {
		panic("incompatible offset type")
	}

	if o.File != o2.File {
		if o.File < o2.File {
			return -1
		}
		return 1
	}

	switch {
	case o.Pos < o2.Pos:
		return -1
	case o.Pos > o2.Pos:
		return 1
	default:
		return 0
	}
}

func (o MySQLOffset) String() string {
	return o.File + ":" + fmt.Sprint(o.Pos)
}
