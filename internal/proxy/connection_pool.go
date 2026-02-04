package proxy

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
)

// newConnectionForTestID cria uma nova conexão PostgreSQL para o testID.
// A conexão pertence à sessão (TestSession) que a criou; não há pool separado.
// O mesmo testID sempre usa a mesma conexão porque há apenas uma sessão por testID,
// e a sessão guarda sua DB (conn+tx) em SessionsByTestID[testID].
func newConnectionForTestID(host string, port int, database, user, password string, sessionTimeout time.Duration, testID string) (*pgx.Conn, error) {
	appName := getAppNameForTestID(testID)
	dsn := fmt.Sprintf("host=%s port=%d database=%s user=%s password=%s application_name=%s",
		host, port, database, user, password, appName)

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if sessionTimeout <= 0 {
		sessionTimeout = 300 * time.Second
	}
	config.ConnectTimeout = sessionTimeout
	dialer := &net.Dialer{
		KeepAlive: 30 * time.Second,
		Timeout:   30 * time.Second,
	}
	config.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, addr)
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := conn.Ping(pingCtx); err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("failed to verify PostgreSQL connection (ping failed): %w", err)
	}

	timeoutMs := int64(sessionTimeout / time.Millisecond)
	_, err = conn.Exec(context.Background(), fmt.Sprintf("SET idle_in_transaction_session_timeout = %d", timeoutMs))
	if err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("failed to set session timeout: %w", err)
	}

	_, err = conn.Exec(context.Background(), "SET idle_session_timeout = '0'")
	if err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("failed to set session timeout: %w", err)
	}

	_, err = conn.Exec(context.Background(), "SET statement_timeout = '0'")
	if err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("failed to set statement timeout: %w", err)
	}

	return conn, nil
}

func getAppNameForTestID(testID string) string {
	if testID == "default" {
		return "pgtest_default"
	}
	return fmt.Sprintf("pgtest-%s", testID)
}
