package util

import "bytes"

const (
	accountPrefix  = "-account"
	storagePrefix  = "-storage"
	codePrefix     = "-code"
	codeHashPrefix = "-codeHash"
	rootPrefix     = "-root"
)

const (
	subTreeNameLen = 20
)

// ParseKey parse key
// -account,<32B>
// -storage,<20B>,<32B>
func ParseKey(key []byte) (prefix []byte, subName []byte, realKey []byte) {
	if bytes.HasPrefix(key, []byte(accountPrefix)) {
		return []byte(accountPrefix), nil, key[len(accountPrefix):]
	} else if bytes.HasPrefix(key, []byte(storagePrefix)) {
		return []byte(storagePrefix), key[len(storagePrefix) : len(storagePrefix)+subTreeNameLen], key[len(storagePrefix)+subTreeNameLen:]
	} else if bytes.HasPrefix(key, []byte(codePrefix)) {

	}
	return
}

func AccountPrefix() []byte {
	return []byte(accountPrefix)
}
