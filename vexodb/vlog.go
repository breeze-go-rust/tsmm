package vexodb

type ValueLog struct{}

func (vlog *ValueLog) Update(data []byte, fid uint64, index uint64, seq uint64) (uint64, uint64) {
	return 0, 0
}

func (vlog *ValueLog) Del(index uint64, fid uint64) {
	return
}
