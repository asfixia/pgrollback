package proxy

import (
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

// NewTestSessionWithLevel cria uma instância TestSession com nível de savepoint específico para testes
// Esta função é exportada para permitir que testes em outros packages a usem
func NewTestSessionWithLevel(pgtest *PGTest, testID string, savepointQuantity int) *TestSession {
	if savepointQuantity < 0 {
		return nil
	}
	session, err := pgtest.GetOrCreateSession(testID)
	if err != nil {
		return nil
	}
	for i := 0; i < savepointQuantity; i++ {
		_, err := pgtest.handleBegin(testID)
		if err != nil {
			return nil
		}
	}
	return session
}
