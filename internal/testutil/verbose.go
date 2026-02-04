package testutil

import (
	"log"
	"os"
	"strings"
)

// TestLogger é uma interface para logging em testes
// Permite que testutil seja usado com *testing.T sem criar dependência direta
type TestLogger interface {
	Helper()
	Logf(format string, args ...interface{})
}

// IsTestVerbose verifica se estamos rodando testes em modo verbose
// Verifica se -test.v está presente nos argumentos da linha de comando
// ou se a variável de ambiente GO_TEST_VERBOSE está definida como "1"
func IsTestVerbose() bool {
	for _, arg := range os.Args {
		if strings.Contains(arg, "-test.v") || strings.Contains(arg, "test.v") {
			return true
		}
	}
	return os.Getenv("GO_TEST_VERBOSE") == "1"
}

// LogIfVerbose registra uma mensagem apenas se estivermos em modo verbose de teste
// Útil para logs de configuração e inicialização que devem aparecer apenas com -v
// Esta versão não chama t.Logf() - use LogIfVerboseWithTest se precisar de integração com testes
func LogIfVerbose(format string, args ...interface{}) {
	if IsTestVerbose() {
		log.Printf(format, args...)
	}
}

// LogIfVerboseWithTest registra uma mensagem apenas se estivermos em modo verbose de teste
// Também chama t.Logf() se um TestLogger for fornecido, garantindo que mensagens apareçam
// tanto no logger quanto no output do teste (-v)
// Útil para logs de configuração e inicialização que devem aparecer apenas com -v
func LogIfVerboseWithTest(t TestLogger, format string, args ...interface{}) {
	if !IsTestVerbose() {
		return
	}

	// Chama log.Printf sempre que estiver em modo verbose
	log.Printf(format, args...)

	// Se um TestLogger foi fornecido, também chama t.Logf para aparecer no output do teste
	if t != nil {
		t.Helper()
		t.Logf(format, args...)
	}
}
