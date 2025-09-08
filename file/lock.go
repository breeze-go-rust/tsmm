package file

import (
	"os"
	"syscall"
)

// FLock 接口定义了文件锁的基本操作
type FLock interface {
	Lock(file *os.File) error   // 获取排他锁
	Unlock(file *os.File) error // 释放锁
}

// FLocker 实现了 Lock 接口，基于系统的 flock 实现
type FLocker struct{}

// NewFLocker 创建一个新的 FLocker 实例
func NewFLocker() *FLocker {
	return &FLocker{}
}

// Lock 获取排他锁（阻塞模式）
// 若文件已被锁定，当前调用会阻塞直到锁被释放
func (f *FLocker) Lock(file *os.File) error {
	// syscall.Flock 参数说明：
	// 1. 文件描述符（需转换为 int）
	// 2. 操作类型：LOCK_EX（排他锁）| LOCK_SH（共享锁），可配合 LOCK_NB（非阻塞）
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
}

// Unlock 释放已获取的锁
func (f *FLocker) Unlock(file *os.File) error {
	// LOCK_UN 表示释放锁
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
