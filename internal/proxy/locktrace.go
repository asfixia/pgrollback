package proxy

import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

// DefaultLockTraceCallerSkip is the usual [runtime.Caller] skip when [LockTraceRegistry.LockRWMutex]
// is called from one thin wrapper (e.g. LockRun) so the recorded site is the wrapper's caller.
const DefaultLockTraceCallerSkip = 3

// LockTraceRegistry is a process-wide singleton that records lock/unlock sites per sync
// primitive (identity = address of the Mutex or RWMutex value). Use from the debugger:
// GlobalLockTraceRegistry().RWMutexStack(&session.mu).
type LockTraceRegistry struct {
	mu sync.Mutex
	// stacks: nested acquisition order per lock address (top = latest Lock).
	stacks map[uintptr][]string
}

var globalLockTraceRegistry = &LockTraceRegistry{
	stacks: make(map[uintptr][]string),
}

// GlobalLockTraceRegistry returns the singleton lock trace registry.
func GlobalLockTraceRegistry() *LockTraceRegistry {
	return globalLockTraceRegistry
}

func rwMutexKey(mu *sync.RWMutex) uintptr {
	return uintptr(unsafe.Pointer(mu))
}

// LockRWMutex records runtime.Caller then acquires mu. Pair with [LockTraceRegistry.UnlockRWMutex].
// Optional callerSkip: if omitted, uses [DefaultLockTraceCallerSkip] (one thin wrapper, e.g. LockRun).
func (r *LockTraceRegistry) LockRWMutex(mu *sync.RWMutex, callerSkip ...int) {
	skip := DefaultLockTraceCallerSkip
	if len(callerSkip) > 0 {
		skip = callerSkip[0]
	}
	if _, file, line, ok := runtime.Caller(skip); ok {
		r.RecordRWMutexLock(mu, fmt.Sprintf("%s:%d", file, line))
	}
	mu.Lock()
}

// UnlockRWMutex releases mu then updates the trace stack (inverse of [LockTraceRegistry.LockRWMutex]).
func (r *LockTraceRegistry) UnlockRWMutex(mu *sync.RWMutex) {
	mu.Unlock()
	r.RecordRWMutexUnlock(mu)
}

// RecordRWMutexLock records a lock acquisition at caller site (before mu.Lock).
func (r *LockTraceRegistry) RecordRWMutexLock(mu *sync.RWMutex, caller string) {
	k := rwMutexKey(mu)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stacks[k] = append(r.stacks[k], caller)
}

// RecordRWMutexUnlock records an unlock (after mu.Unlock); pops the latest lock site.
func (r *LockTraceRegistry) RecordRWMutexUnlock(mu *sync.RWMutex) {
	k := rwMutexKey(mu)
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stacks[k]
	if len(s) == 0 {
		return
	}
	s = s[:len(s)-1]
	if len(s) == 0 {
		delete(r.stacks, k)
	} else {
		r.stacks[k] = s
	}
}

// RWMutexStack returns a copy of the current lock stack for mu (newest last), or nil.
func (r *LockTraceRegistry) RWMutexStack(mu *sync.RWMutex) []string {
	k := rwMutexKey(mu)
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stacks[k]
	if len(s) == 0 {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}
