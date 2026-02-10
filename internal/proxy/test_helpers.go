package proxy

import (
	"context"
	"math/rand"
	"strconv"
)

// NewPGTestFromConfigForTesting cria uma instância PGTest a partir da configuração para testes
// Esta função é exportada para permitir que testes em outros packages a usem
func NewPGTestFromConfigForTesting() *PGTest {
	return newPGTestFromConfig()
}

func GetNonUsedTestID(pgtest *PGTest) string {
	r := rand.New(rand.NewSource(100000000))
	newSessionTestId := ""
	for {
		newSessionTestId = strconv.Itoa(int(r.Int31()))
		if pgtest.GetSession(newSessionTestId) == nil {
			break
		}
	}
	return newSessionTestId
}

// NewTestSessionForTesting cria uma instância TestSession para testes
// Esta função é exportada para permitir que testes em outros packages a usem
func NewTestSessionForTesting(pgtest *PGTest) *TestSession {
	session, err := pgtest.GetOrCreateSession(GetNonUsedTestID(pgtest))
	if err != nil {
		return nil
	}
	return session
}

// testSetupConnectionID is used by NewTestSessionWithLevel when applying BEGIN side effects (no real proxy connection).
const testSetupConnectionID ConnectionID = 0

// NewTestSessionWithLevel cria uma instância TestSession com nível de savepoint 1 para testes.
// Executa um BEGIN (SAVEPOINT) e aplica claim + incremento de nível.
func NewTestSessionWithLevel(pgtest *PGTest, testID string) *TestSession {
	session, err := pgtest.GetOrCreateSession(testID)
	if err != nil {
		return nil
	}
	if session.DB == nil || !session.DB.HasActiveTransaction() {
		return nil
	}
	q, err := pgtest.handleBegin(testID, testSetupConnectionID)
	if err != nil {
		return nil
	}
	if _, err := session.DB.Tx().Exec(context.Background(), q); err != nil {
		return nil
	}
	if err := session.DB.ClaimOpenTransaction(testSetupConnectionID); err != nil {
		return nil
	}
	session.DB.IncrementSavepointLevel()
	return session
}
