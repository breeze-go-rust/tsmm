package go_tsmm

import (
	"fmt"
	"github.com/breeze-go-rust/tsmm/file"
	"github.com/breeze-go-rust/tsmm/internal/common"
	"path/filepath"
	"unsafe"
)

type MetaMgr struct {
	mFile              []*file.File
	noSync             bool
	activateVersionNum int
}

func NewMetaMgr(metaFilePath string, activateVersionNum int, noSync bool) (*MetaMgr, error) {
	files := make([]*file.File, activateVersionNum)
	for i := 0; i < activateVersionNum; i++ {
		metaPath := filepath.Join(metaFilePath, fmt.Sprintf("%d.meta", i))
		mFile, err := file.OpenFile(metaPath, file.NewFLocker())
		if err != nil {
			return nil, fmt.Errorf("error opening page file %s: %w", metaFilePath, err)
		}
		files[i] = mFile
	}
	metaMgr := &MetaMgr{mFile: files, activateVersionNum: activateVersionNum, noSync: noSync}
	return metaMgr, nil
}

// Write 对于持久化的 meta 进行写入, 树上的活跃版本 不在这里写入
// v=10
// 0,1,2,3,4,5,6,7,8,9
// 10,12,13,14,15,16,17,18,19
func (mm *MetaMgr) Write(meta *common.Meta) error {
	metaFileIndex := int(meta.Pgid()) % len(mm.mFile)
	metaFilePosition := int(meta.Pgid()) / len(mm.mFile)
	data := meta.Encode()
	at, err := mm.mFile[metaFileIndex].WriteAt(int64(metaFilePosition), data)
	if err != nil {
		return fmt.Errorf("error writing meta file: %w", err)
	}
	if at != len(data) {
		return fmt.Errorf("incorrect position written size")
	}
	return Sync(mm.noSync, mm.mFile[metaFileIndex].Sync)
}

func (mm *MetaMgr) ReadMeta(metaID uint64, data []byte) (*common.Meta, error) {
	metaFileIndex := int(metaID) % len(mm.mFile)
	metaFilePosition := int(metaID) / len(mm.mFile)
	// 实际的 Offset
	offset := int(metaFilePosition) * common.MetaSize

	at, err := mm.mFile[metaFileIndex].ReadAt(int64(offset), data)
	if err != nil {
		return nil, fmt.Errorf("error reading meta file: %w", err)
	}
	if at != len(data) {
		return nil, fmt.Errorf("incorrect position read size")
	}
	meta := (*common.Meta)(unsafe.Pointer(&data[0]))
	return meta, nil
}
