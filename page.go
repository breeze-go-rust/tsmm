package go_tsmm

import (
	"fmt"
	"github.com/breeze-go-rust/tsmm/file"
	"github.com/breeze-go-rust/tsmm/internal/common"
	"io"
	"unsafe"
)

type PageMgr struct {
	pFile        *file.File
	noSync       bool
	pageSize     uint64
	pageFilePath string
}

func NewPageMgr(pageFilePath string, noSync bool) (*PageMgr, error) {
	pFile, err := file.OpenFile(pageFilePath, file.NewFLocker())
	if err != nil {
		return nil, fmt.Errorf("error opening page file %s: %w", pageFilePath, err)
	}
	return &PageMgr{pFile: pFile, pageFilePath: pageFilePath, noSync: noSync}, nil
}

func (pm *PageMgr) Write(page *common.Page) error {
	// 计算索引位
	offset := uint64(page.Id()) * pm.pageSize
	bufSize := (uint64(page.Id()) + uint64(page.Overflow())) * pm.pageSize
	data := common.UnsafeByteSlice(unsafe.Pointer(page), 0, 0, 0)
	n, err := pm.pFile.WriteAt(int64(offset), data)
	if err != nil {
		return fmt.Errorf("error writing to page file %s: %w", pm.pageFilePath, err)
	}
	if uint64(n) != bufSize {
		return fmt.Errorf("error writing to page file %s: %w", pm.pageFilePath, io.ErrShortWrite)
	}
	return Sync(!pm.noSync, pm.pFile.Sync)
}

func (pm *PageMgr) ReadAt(pid common.Pgid, overflow uint32) (*common.Page, error) {
	offset := uint64(pid) * pm.pageSize
	bufSize := (uint64(pid) + uint64(overflow)) * pm.pageSize
	buf := make([]byte, bufSize)
	n, err := pm.pFile.ReadAt(int64(offset), buf)
	if err != nil {
		return nil, fmt.Errorf("error reading from page file %s: %w", pm.pageFilePath, err)
	}
	if n != int(bufSize) {
		return nil, fmt.Errorf("error reading from page file %s: %w", pm.pageFilePath, io.ErrShortWrite)
	}
	return (*common.Page)(unsafe.Pointer(&buf[0])), nil
}

func Sync(condition bool, f func() error) error {
	if !condition {
		return f()
	}
	return nil
}
