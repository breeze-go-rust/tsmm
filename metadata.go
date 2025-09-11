package go_tsmm

import "github.com/breeze-go-rust/go-tsmm/internal/common"

type MetaData struct {
	magic     uint32
	version   uint32
	pgid      common.Pgid
	overflow  uint32
	seq       uint64
	stdCommit uint64
}
