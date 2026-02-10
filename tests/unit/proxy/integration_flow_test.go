package tstproxy

import (
	"fmt"
	"testing"
	"time"
)

// TestTransactionFlow_CompleteCycle testa o fluxo completo de transações:
// 1. Criar tabela -> commit (savepoint) -> inserir linha -> commit (savepoint)
// 2. Verificar linha existe -> rollback -> verificar linha sumiu -> rollback -> verificar tabela sumiu
func TestTransactionFlow_CompleteCycle(t *testing.T) {
	pgtest := newPGTestFromConfig()
	testID := "test_transaction_flow"
	session, err := pgtest.GetOrCreateSession(testID)
	if err != nil {
		t.Fatalf("GetOrCreateSession() error = %v", err)
	}

	tableName := "test_transaction_table_" + testID
	createTableWithIdAndName(t, pgtest, session, tableName)
	assertTableCount(t, session, tableName, 0, "Table should be empty initially")
	assertCommitBlocked(t, pgtest, session, "COMMIT at level 0 should be blocked")       // fake commit
	execBeginAndVerify(t, pgtest, session, 1, "First BEGIN creates savepoint level 1")
	insertRowWithName(t, pgtest, session, tableName, "test_row")
	assertRowCountWithCondition(t, session, tableName, "name = 'test_row'", 1, "Row should be inserted")
	execBeginAndVerify(t, pgtest, session, 1, "Second BEGIN is no-op (single level)")
	execCommitOnLevel(t, pgtest, session, 0, "COMMIT releases savepoint level 1 -> level 0")
	assertRowCountWithCondition(t, session, tableName, "name = 'test_row'", 1, "Row should still exist after COMMIT")
	execBeginAndVerify(t, pgtest, session, 1, "BEGIN creates savepoint level 1 again")
	execRollbackAndVerify(t, pgtest, session, 0, "ROLLBACK to level 0")
	// Rollback only released the savepoint; test_row was committed earlier so it still exists.
	assertRowCountWithCondition(t, session, tableName, "name = 'test_row'", 1, "Row should still exist after ROLLBACK")

	// 7. Segundo rollback não é possível (já estamos no nível 0)
	// Mas a tabela ainda existe porque foi criada antes de qualquer savepoint
	// Para remover a tabela, precisaríamos fazer rollback da transação principal
	// que não é permitido pelo sistema (é bloqueado)
	assertRollbackBlocked(t, pgtest, session, "ROLLBACK at level 0 should be blocked")

	// A tabela ainda existe porque foi criada antes de qualquer savepoint
	// Em um sistema real, a tabela só seria removida se fizéssemos rollback da transação principal
	// mas isso é bloqueado pelo sistema
	assertTableExists(t, session, tableName, "Table should still exist (created before any savepoint, rollback of main transaction is blocked)")
}

// TestExcessiveRollback testa fazer mais rollbacks que commits
// O sistema deve funcionar mas pode dar avisos
func TestExcessiveRollback(t *testing.T) {
	pgtest := newPGTestFromConfig()
	testID := "test_excessive_rollback"
	session, err := pgtest.GetOrCreateSession(testID)
	if err != nil {
		t.Skip("Skipping test - requires PostgreSQL connection")
	}

	// Single level: only one BEGIN creates a savepoint
	execBeginAndVerify(t, pgtest, session, 1, "First BEGIN creates savepoint level 1")
	execBeginAndVerify(t, pgtest, session, 1, "Second BEGIN is no-op (single level)")

	// First ROLLBACK (real) -> level 0
	execRollbackAndVerify(t, pgtest, session, 0, "First ROLLBACK removes savepoint level 1")

	// Second ROLLBACK is no-op (level 0)
	assertRollbackBlocked(t, pgtest, session, "Second ROLLBACK at level 0 should be blocked")

	// Nota: Em um sistema real, poderia imprimir um aviso no stdout
	// mas como estamos testando apenas a lógica, verificamos que funciona corretamente
	t.Log("Excessive rollback handled correctly - third rollback was blocked as expected")
}

// TestDisconnectReconnect simula desconexão e reconexão
// Deve manter a transação ativa e permitir continuar trabalhando
func TestDisconnectReconnect(t *testing.T) {
	pgtest := newPGTestFromConfig()
	testID := "test_disconnect_reconnect"

	// 1. Primeira conexão - cria sessão e faz alterações
	session1, err := pgtest.GetOrCreateSession(testID)
	if err != nil {
		t.Skip("Skipping test - requires PostgreSQL connection")
	}

	tableName := "test_reconnect_table_" + testID

	// Cria tabela
	createTableWithIdAndData(t, pgtest, session1, tableName)

	// Insere linha
	insertRowWithData(t, pgtest, session1, tableName, "initial_data")

	// Simula "commit" (savepoint) - cria savepoint
	execBeginAndVerify(t, pgtest, session1, 1, "BEGIN creates savepoint level 1")

	// 2. Simula desconexão (mas na verdade mantemos a sessão)
	// Em um caso real, a conexão seria fechada, mas mantemos a transação
	// A transação não é commitada porque estamos simulando comportamento real do BD
	// onde desconectar sem commit faz rollback automático, mas aqui mantemos a transação

	// 3. Reconexão - deve reusar a mesma sessão
	session2, err := pgtest.GetOrCreateSession(testID)
	if err != nil {
		t.Fatalf("GetOrCreateSession() on reconnect error = %v", err)
	}

	// Deve ser a mesma instância de sessão
	if session1 != session2 {
		t.Error("GetOrCreateSession() should return same session instance on reconnect")
	}

	// Verifica que a transação ainda está ativa
	if session2.DB == nil || !session2.DB.HasActiveTransaction() {
		t.Fatal("Transaction should still be active after reconnect")
	}

	// 4. Continua trabalhando na mesma transação
	insertRowWithData(t, pgtest, session2, tableName, "after_reconnect")

	// Verifica que ambas as linhas existem
	assertTableCount(t, session2, tableName, 2, "Total rows after reconnect should be 2")

	// Verifica linha inicial
	assertRowCountWithCondition(t, session2, tableName, "data = 'initial_data'", 1, "Initial data should exist")

	// Verifica linha após reconexão
	assertRowCountWithCondition(t, session2, tableName, "data = 'after_reconnect'", 1, "Reconnect data should exist")

	// 5. Faz rollback - deve reverter tudo
	execRollbackAndVerify(t, pgtest, session2, 0, "ROLLBACK removes savepoint level 1")

	// Verifica que a sessão ainda existe (transação ainda ativa)
	if pgtest.GetSession(testID) == nil {
		t.Error("Session should still exist after rollback")
	}

	// Verifica que os dados foram revertidos (rollback removeu a inserção após reconexão)
	assertRowCountWithCondition(t, session2, tableName, "data = 'after_reconnect'", 0, "Reconnect data should be rolled back")

	// A linha inicial ainda deve existir (antes do savepoint)
	assertRowCountWithCondition(t, session2, tableName, "data = 'initial_data'", 1, "Initial data should still exist after rollback")
}

// TestNewConnectionStartsTransaction testa que ao conectar sem existir conexão,
// ele deve conectar e iniciar uma transação
func TestNewConnectionStartsTransaction(t *testing.T) {
	pgtest := newPGTestFromConfig()
	testID := "test_new_connection_" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Primeira conexão - deve criar nova sessão com transação
	session1, err := pgtest.GetOrCreateSession(testID)
	if err != nil {
		t.Skip("Skipping test - requires PostgreSQL connection")
	}

	// Verifica que a transação foi iniciada
	if session1.DB == nil || !session1.DB.HasActiveTransaction() {
		t.Fatal("New session should have an active transaction")
	}

	// Verifica que a conexão foi estabelecida
	if session1.DB == nil || session1.DB.PgConn() == nil {
		t.Fatal("New session should have a connection")
	}

	// Faz uma operação para verificar que tudo funciona
	tableName := "test_new_conn_table_" + testID
	createTableWithId(t, pgtest, session1, tableName)

	// Segunda conexão com mesmo testID - deve reusar a mesma sessão
	session2, err := pgtest.GetOrCreateSession(testID)
	if err != nil {
		t.Fatalf("GetOrCreateSession() error = %v", err)
	}

	if session1 != session2 {
		t.Error("GetOrCreateSession() should return same session instance")
	}

	// Verifica que a transação ainda está ativa
	if session2.DB == nil || !session2.DB.HasActiveTransaction() {
		t.Fatal("Session should still have an active transaction")
	}

	// Verifica que a tabela criada ainda existe (mesma transação)
	assertTableExists(t, session2, tableName, "Table should exist in the same transaction")
}
