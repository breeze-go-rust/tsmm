package common

import (
	"fmt"
	"os"
	"strings"
)

const EnvVerify = "TSMM_VERIFY"

type VerificationType string

const (
	EnvVerifyValueAll    VerificationType = "all"
	EnvVerifyValueAssert VerificationType = "assert"
)

func getEnvVerify() string {
	return strings.ToLower(os.Getenv(EnvVerify))
}

func IsVerificationEnabled(verification VerificationType) bool {
	env := getEnvVerify()
	return env == string(EnvVerifyValueAll) || env == strings.ToLower(string(verification))
}

// EnableVerifications sets `ENV_VERIFY` and returns a function that
// can be used to bring the original settings.
func EnableVerifications(verification VerificationType) func() {
	previousEnv := getEnvVerify()
	_ = os.Setenv(EnvVerify, string(verification))
	return func() {
		_ = os.Setenv(EnvVerify, previousEnv)
	}
}

// EnableAllVerifications enables verification and returns a function
// that can be used to bring the original settings.
func EnableAllVerifications() func() {
	return EnableVerifications(EnvVerifyValueAll)
}

// DisableVerifications unsets `ENV_VERIFY` and returns a function that
// can be used to bring the original settings.
func DisableVerifications() func() {
	previousEnv := getEnvVerify()
	_ = os.Unsetenv(EnvVerify)
	return func() {
		_ = os.Setenv(EnvVerify, previousEnv)
	}
}

// Verify performs verification if the assertions are enabled.
// In the default setup running in tests and skipped in the production code.
func Verify(f func()) {
	if IsVerificationEnabled(EnvVerifyValueAssert) {
		f()
	}
}

// Assert will panic with a given formatted message if the given condition is false.
func Assert(condition bool, msg string, v ...any) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
