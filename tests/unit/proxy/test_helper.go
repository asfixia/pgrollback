package tstproxy

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"pgtest/internal/proxy"
	sqlpkg "pgtest/pkg/sql"

	"github.com/jackc/pgx/v5"
)

func newPGTestFromConfig() *proxy.PGTest {
	return proxy.NewPGTestFromConfigForTesting()
}

func newTestSession(pgtest *proxy.PGTest) *proxy.TestSession {
	return proxy.NewTestSessionForTesting(pgtest)
}

func newTestSessionWithLevel(pgtest *proxy.PGTest, testID string, savepointQuantity int) *proxy.TestSession {
	return proxy.NewTestSessionWithLevel(pgtest, testID, savepointQuantity)
}

func contains(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func buildDSN(host string, port int, database, user, password, applicationName string) string {
	dsn := "host=" + host + " port=" + strconv.Itoa(port) + " database=" + database + " user=" + user
	if password != "" {
		dsn += " password=" + password
	}
	if applicationName != "" {
		dsn += " application_name=" + applicationName
	}
	return dsn
}

func getOrDefault(value time.Duration, defaultValue time.Duration) time.Duration {
	if value <= 0 {
		return defaultValue
	}
	return value
}

func coalesce(value string, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

// getConfigPath retorna o caminho do arquivo de config, resolvendo relativo à raiz do projeto
func getConfigPath() string {
	envPath := os.Getenv("PGTEST_CONFIG")
	if envPath != "" {
		if filepath.IsAbs(envPath) {
			return envPath
		}
		projectRoot := findProjectRoot()
		if projectRoot != "" {
			return filepath.Join(projectRoot, envPath)
		}
		return envPath
	}
	// Tenta config/pgtest.yaml relativo à raiz do projeto
	projectRoot := findProjectRoot()
	if projectRoot != "" {
		return filepath.Join(projectRoot, "config", "pgtest.yaml")
	}
	return "config/pgtest.yaml"
}

// findProjectRoot encontra a raiz do projeto (onde está go.mod)
func findProjectRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func isPortInUse(host string, port int) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 100*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func ensurePortIsAvailable(t *testing.T, host string, port int) {
	if isPortInUse(host, port) {
		t.Fatalf("Port %s:%d is already in use", host, port)
	}
}

// startProxyServerInBackground está deprecated - NewServer agora inicia automaticamente
// Mantido para compatibilidade, mas não é mais necessário
func startProxyServerInBackground(t *testing.T, proxyServer *proxy.Server, host string, port int, ctx context.Context) {
	// Esta função não faz mais nada, pois NewServer já inicia o servidor
	// Mantida apenas para compatibilidade com código que ainda a chama
}

func waitForProxyServerToListen(t *testing.T, host string, port int) {
	maxAttempts := 20
	for i := 0; i < maxAttempts; i++ {
		if isPortInUse(host, port) {
			return
		}
		time.Sleep(100 * time.Millisecond)
		if i == maxAttempts-1 {
			t.Fatalf("Proxy server is not listening on %s:%d", host, port)
		}
	}
}

func stopProxyServer(proxyServer *proxy.Server) {
	if proxyServer != nil {
		proxyServer.Stop()
		// Aguarda um pouco para garantir que a porta seja liberada
		time.Sleep(200 * time.Millisecond)
	}
}

// pingWithTimeout executa um ping na conexão com timeout especificado.
// Falha o teste se o ping não for bem-sucedido.
func pingWithTimeout(t *testing.T, db *sql.DB, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Fatalf("Failed to ping database connection: %v", err)
	}
}

// QueryContextLastResult executa múltiplas declarações SQL usando Exec
// e retorna apenas o último SELECT que retorna resultados, similar ao QueryContext
// mas ao invés de retornar o primeiro result set, retorna o último.
// Esta função processa todos os result sets mas retorna apenas o último que tem linhas.
func QueryContextLastResult(t *testing.T, db *sql.DB, ctx context.Context, query string) (*sql.Rows, error) {
	// Obtém a conexão pgx subjacente do database/sql.DB
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// Obtém a conexão pgx nativa através do driver
	var pgxConn *pgx.Conn
	err = conn.Raw(func(driverConn interface{}) error {
		// O driver stdlib do pgx expõe a conexão através de uma interface
		type pgxDriverConn interface {
			Conn() *pgx.Conn
		}
		if stdlibConn, ok := driverConn.(pgxDriverConn); ok {
			pgxConn = stdlibConn.Conn()
			return nil
		}
		return fmt.Errorf("unable to extract pgx connection from driver")
	})
	if err != nil {
		return nil, fmt.Errorf("failed to extract pgx connection: %w", err)
	}

	// Usa pgconn.Exec() para processar múltiplos result sets
	// Isso permite processar todos os comandos e retornar apenas o último SELECT que tem linhas
	pgConn := pgxConn.PgConn()
	mrr := pgConn.Exec(ctx, query)
	defer mrr.Close()

	// Parseia a query para encontrar os comandos SELECT usando o parser existente
	commands := sqlpkg.SplitCommands(query)

	// Identifica quais comandos são SELECTs
	var selectCommands []string
	for _, cmd := range commands {
		if sqlpkg.IsSelect(cmd) {
			selectCommands = append(selectCommands, cmd)
		}
	}

	// Processa todos os result sets para identificar qual é o último SELECT que tem linhas
	var lastSelectQuery string
	selectIndex := 0

	for mrr.NextResult() {
		rr := mrr.ResultReader()
		if rr == nil {
			continue
		}

		fieldDescs := rr.FieldDescriptions()
		if len(fieldDescs) > 0 {
			// É um SELECT - verifica se tem linhas
			hasRows := false
			for rr.NextRow() {
				hasRows = true
				break // Apenas verifica se tem pelo menos uma linha
			}

			// Se tem linhas e é um SELECT válido, armazena como último
			if hasRows && selectIndex < len(selectCommands) {
				lastSelectQuery = selectCommands[selectIndex]
			}
			selectIndex++
		}

		// Sempre fecha o ResultReader
		_, err := rr.Close()
		if err != nil {
			return nil, fmt.Errorf("error closing result reader: %w", err)
		}
	}

	// Verifica erros finais
	if err := mrr.Close(); err != nil {
		return nil, fmt.Errorf("error processing multiple command results: %w", err)
	}

	// Se encontrou um último SELECT com linhas, executa apenas ele usando QueryContext
	if lastSelectQuery != "" {
		return db.QueryContext(ctx, lastSelectQuery)
	}

	// Se não encontrou nenhum SELECT com linhas, retorna um result set vazio
	// Executa a query novamente mas usando QueryContext que retornará o primeiro result set
	// (que pode estar vazio, mas pelo menos retorna a estrutura correta)
	return db.QueryContext(ctx, query)
}

// ExecuteMultipleStatements executa múltiplas declarações SQL usando Exec
// e retorna apenas o número de linhas do último SELECT que retorna resultados.
// Esta função processa todos os result sets mas retorna apenas o último que tem linhas,
// que é o comportamento esperado quando usando Exec com múltiplas declarações.
func ExecuteMultipleStatements(t *testing.T, db *sql.DB, ctx context.Context, query string) int {
	rows, err := QueryContextLastResult(t, db, ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute multiple statements: %v", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}

	return rowCount
}
