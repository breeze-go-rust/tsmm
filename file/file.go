package file

import (
	"fmt"
	"os"
)

type File struct {
	file  *os.File
	flock FLock
}

func OpenFile(filePath string, lock FLock) (*File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	if lock != nil {
		if err := lock.Lock(file); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("error locking file: %v", err)
		}
	}
	f := &File{
		file:  file,
		flock: lock,
	}
	return f, nil
}

func (f *File) WriteAt(offset int64, data []byte) (int, error) {
	return f.file.WriteAt(data, offset)
}
func (f *File) ReadAt(offset int64, data []byte) (int, error) {
	return f.file.ReadAt(data, offset)
}

func (f *File) Sync() error {
	return f.file.Sync()
}

func (f *File) Close() error {
	if err := f.file.Sync(); err != nil {
		return fmt.Errorf("error syncing file: %v", err)
	}
	if f.flock != nil {
		if err := f.flock.Unlock(f.file); err != nil {
			return fmt.Errorf("error unlocking file: %v", err)
		}
	}
	return f.file.Close()
}
