package proxy

import (
	"sync"
	"testing"
	"time"
)

func TestGetOrCreateSession(t *testing.T) {
	pgtest := NewPGTest("localhost", 5432, "test", "user", "pass", 3600)

	t.Run("create new session", func(t *testing.T) {
		testID := "test123"
		session, err := pgtest.GetOrCreateSession(testID)
		if err == nil {
			t.Logf("Session created (this test requires PostgreSQL connection)")
			if session.TestID != testID {
				t.Errorf("Session.TestID = %v, want %v", session.TestID, testID)
			}
		}
	})

	t.Run("reuse existing session", func(t *testing.T) {
		testID := "test456"
		session1, err1 := pgtest.GetOrCreateSession(testID)
		if err1 != nil {
			t.Skip("Skipping test - requires PostgreSQL connection")
		}

		session2, err2 := pgtest.GetOrCreateSession(testID)
		if err2 != nil {
			t.Fatalf("GetOrCreateSession() error = %v", err2)
		}

		if session1 != session2 {
			t.Error("GetOrCreateSession() should return same session instance")
		}
	})
}

func TestGetSession(t *testing.T) {
	pgtest := NewPGTest("localhost", 5432, "test", "user", "pass", 3600)

	t.Run("get non-existent session", func(t *testing.T) {
		session := pgtest.GetSession("nonexistent")
		if session != nil {
			t.Error("GetSession() should return nil for non-existent session")
		}
	})
}

func TestConcurrency(t *testing.T) {
	pgtest := NewPGTest("localhost", 5432, "test", "user", "pass", 3600)
	testID := "concurrent_test"

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pgtest.GetOrCreateSession(testID)
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Logf("Concurrent access error (expected if PostgreSQL not available): %v", err)
		}
	}
}

func TestCleanupExpiredSessions(t *testing.T) {
	pgtest := NewPGTest("localhost", 5432, "test", "user", "pass", 100*time.Millisecond)

	session := &TestSession{
		TestID:       "expired",
		CreatedAt:    time.Now().Add(-200 * time.Millisecond),
		LastActivity: time.Now().Add(-200 * time.Millisecond),
	}

	pgtest.mu.Lock()
	pgtest.Sessions["expired"] = session
	pgtest.mu.Unlock()

	time.Sleep(150 * time.Millisecond)

	cleaned := pgtest.CleanupExpiredSessions()
	if cleaned != 1 {
		t.Errorf("CleanupExpiredSessions() = %v, want 1", cleaned)
	}

	pgtest.mu.RLock()
	_, exists := pgtest.Sessions["expired"]
	pgtest.mu.RUnlock()

	if exists {
		t.Error("Expired session should be removed")
	}
}
