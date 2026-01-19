package proxy

import (
	"testing"
)

func TestHandleBegin(t *testing.T) {
	pgtest := NewPGTest("localhost", 5432, "test", "user", "pass", 3600)
	session := &TestSession{
		TestID:         "test123",
		SavepointLevel: 0,
		Savepoints:     []string{},
	}

	query, err := pgtest.handleBegin(session)
	if err != nil {
		t.Fatalf("handleBegin() error = %v", err)
	}

	if query != "SAVEPOINT sp_1" {
		t.Errorf("handleBegin() = %v, want SAVEPOINT sp_1", query)
	}

	if session.SavepointLevel != 1 {
		t.Errorf("SavepointLevel = %v, want 1", session.SavepointLevel)
	}

	if len(session.Savepoints) != 1 {
		t.Errorf("Savepoints length = %v, want 1", len(session.Savepoints))
	}

	query2, err := pgtest.handleBegin(session)
	if err != nil {
		t.Fatalf("handleBegin() error = %v", err)
	}

	if query2 != "SAVEPOINT sp_2" {
		t.Errorf("handleBegin() = %v, want SAVEPOINT sp_2", query2)
	}

	if session.SavepointLevel != 2 {
		t.Errorf("SavepointLevel = %v, want 2", session.SavepointLevel)
	}
}

func TestHandleCommit(t *testing.T) {
	pgtest := NewPGTest("localhost", 5432, "test", "user", "pass", 3600)

	t.Run("commit savepoint when level > 0", func(t *testing.T) {
		session := &TestSession{
			TestID:         "test123",
			SavepointLevel: 2,
			Savepoints:     []string{"sp_1", "sp_2"},
		}

		query, err := pgtest.handleCommit(session)
		if err != nil {
			t.Fatalf("handleCommit() error = %v", err)
		}

		if query != "RELEASE SAVEPOINT sp_2" {
			t.Errorf("handleCommit() = %v, want RELEASE SAVEPOINT sp_2", query)
		}

		if session.SavepointLevel != 1 {
			t.Errorf("SavepointLevel = %v, want 1", session.SavepointLevel)
		}

		if len(session.Savepoints) != 1 {
			t.Errorf("Savepoints length = %v, want 1", len(session.Savepoints))
		}
	})

	t.Run("block commit when level = 0", func(t *testing.T) {
		session := &TestSession{
			TestID:         "test123",
			SavepointLevel: 0,
			Savepoints:     []string{},
		}

		query, err := pgtest.handleCommit(session)
		if err != nil {
			t.Fatalf("handleCommit() error = %v", err)
		}

		if query != "SELECT 1" {
			t.Errorf("handleCommit() = %v, want SELECT 1 (blocked)", query)
		}

		if session.SavepointLevel != 0 {
			t.Errorf("SavepointLevel = %v, want 0", session.SavepointLevel)
		}
	})
}

func TestHandleRollback(t *testing.T) {
	pgtest := NewPGTest("localhost", 5432, "test", "user", "pass", 3600)

	t.Run("rollback to savepoint when level > 0", func(t *testing.T) {
		session := &TestSession{
			TestID:         "test123",
			SavepointLevel: 2,
			Savepoints:     []string{"sp_1", "sp_2"},
		}

		query, err := pgtest.handleRollback(session)
		if err != nil {
			t.Fatalf("handleRollback() error = %v", err)
		}

		expected := "ROLLBACK TO SAVEPOINT sp_2; RELEASE SAVEPOINT sp_2"
		if query != expected {
			t.Errorf("handleRollback() = %v, want %v", query, expected)
		}

		if session.SavepointLevel != 1 {
			t.Errorf("SavepointLevel = %v, want 1", session.SavepointLevel)
		}

		if len(session.Savepoints) != 1 {
			t.Errorf("Savepoints length = %v, want 1", len(session.Savepoints))
		}
	})

	t.Run("block rollback when level = 0", func(t *testing.T) {
		session := &TestSession{
			TestID:         "test123",
			SavepointLevel: 0,
			Savepoints:     []string{},
		}

		query, err := pgtest.handleRollback(session)
		if err != nil {
			t.Fatalf("handleRollback() error = %v", err)
		}

		if query != "SELECT 1" {
			t.Errorf("handleRollback() = %v, want SELECT 1 (blocked)", query)
		}

		if session.SavepointLevel != 0 {
			t.Errorf("SavepointLevel = %v, want 0", session.SavepointLevel)
		}
	})
}

func TestHandlePGTestCommand(t *testing.T) {
	pgtest := NewPGTest("localhost", 5432, "test", "user", "pass", 3600)

	t.Run("invalid command", func(t *testing.T) {
		_, err := pgtest.handlePGTestCommand("pgtest")
		if err == nil {
			t.Error("handlePGTestCommand() should return error for invalid command")
		}
	})

	t.Run("unknown action", func(t *testing.T) {
		_, err := pgtest.handlePGTestCommand("pgtest unknown")
		if err == nil {
			t.Error("handlePGTestCommand() should return error for unknown action")
		}
	})

	t.Run("begin missing test_id", func(t *testing.T) {
		_, err := pgtest.handlePGTestCommand("pgtest begin")
		if err == nil {
			t.Error("handlePGTestCommand() should return error for missing test_id")
		}
	})

	t.Run("rollback missing test_id", func(t *testing.T) {
		_, err := pgtest.handlePGTestCommand("pgtest rollback")
		if err == nil {
			t.Error("handlePGTestCommand() should return error for missing test_id")
		}
	})

	t.Run("status missing test_id", func(t *testing.T) {
		_, err := pgtest.handlePGTestCommand("pgtest status")
		if err == nil {
			t.Error("handlePGTestCommand() should return error for missing test_id")
		}
	})
}
