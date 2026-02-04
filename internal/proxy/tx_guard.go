package proxy

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgxExecer é a menor interface necessária para aplicar o guard via savepoint
// (compatível com pgx.Tx e pgx.Conn).
type pgxQueryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type guardedRows struct {
	pgx.Rows
	ctx       context.Context
	tx        pgxQueryer
	savepoint string
	closed    bool
}

func (r *guardedRows) Close() {
	if r.closed {
		return
	}
	r.closed = true

	r.Rows.Close()

	// If the query caused an error mid-iteration,
	// the transaction is aborted → rollback to savepoint
	if err := r.Rows.Err(); err != nil {
		if guardErr := rollbackToAndReleaseSavepoint(r.ctx, r.tx, r.savepoint); guardErr != nil {
			log.Printf("[PROXY] FATAL: Falha ao reverter savepoint após erro em rows: %v", guardErr)
		}
		return
	}

	// Success path → just release the savepoint
	if releaseErr := releaseSavepoint(r.ctx, r.tx, r.savepoint); releaseErr != nil {
		log.Printf("[PROXY] Aviso: Falha ao liberar savepoint de guarda: %v", releaseErr)
	}
}

func querySafeSavepoint(
	ctx context.Context,
	tx pgxQueryer,
	savepointName string,
	query string,
	args ...any,
) (pgx.Rows, error) {

	// Create guard savepoint
	if _, err := tx.Exec(ctx, "SAVEPOINT "+savepointName); err != nil {
		log.Printf("[PROXY] A Falha ao criar savepoint de guarda: %v", err)
		return nil, fmt.Errorf("falha interna de transação: %w", err)
	}

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		// Query failed → rollback guard immediately
		_ = rollbackToAndReleaseSavepoint(ctx, tx, savepointName)
		log.Printf("[PROXY] Erro na execução (revertendo guarda): %v", err)
		return nil, fmt.Errorf("falha ao executar query: %w", err)
	}

	// Wrap rows so savepoint is finalized on Close()
	return &guardedRows{
		Rows:      rows,
		ctx:       ctx,
		tx:        tx,
		savepoint: savepointName,
	}, nil
}

func execQuerySafeSavepoint(ctx context.Context, tx pgxQueryer, savepointName string, query string) (tag pgconn.CommandTag, err error) {
	// Cria um savepoint interno antes de executar o comando.
	// Se o comando falhar, fazemos rollback para este savepoint para não abortar a transação principal.
	if _, err = tx.Exec(ctx, "SAVEPOINT "+savepointName); err != nil {
		log.Printf("[PROXY] - Falha ao criar savepoint de guarda: %v", err)
		return tag, fmt.Errorf("falha interna de transação: %w", err)
	}

	// Finaliza o guard automaticamente:
	// - pânico -> rollback+release e repanica
	// - erro   -> rollback+release e retorna erro original
	// - ok     -> release
	defer func() {
		if p := recover(); p != nil {
			if guardErr := rollbackToAndReleaseSavepoint(ctx, tx, savepointName); guardErr != nil {
				log.Printf("[PROXY] FATAL: Falha ao reverter savepoint de guarda após pânico: %v", guardErr)
			}
			panic(p)
		}

		if err != nil {
			if guardErr := rollbackToAndReleaseSavepoint(ctx, tx, savepointName); guardErr != nil {
				log.Printf("[PROXY] FATAL: Falha ao reverter savepoint de guarda: %v", guardErr)
			}
			return
		}

		if releaseErr := releaseSavepoint(ctx, tx, savepointName); releaseErr != nil {
			log.Printf("[PROXY] Aviso: Falha ao liberar savepoint de guarda: %v", releaseErr)
		}
	}()

	tag, err = tx.Exec(ctx, query)
	if err != nil {
		// Retorna o erro original do comando para o cliente (a reversão ocorre no defer).
		log.Printf("[PROXY] Erro na execução (revertendo guarda): %v", err)
		return tag, fmt.Errorf("falha ao executar comando: %w", err)
	}

	return tag, nil
}

func rollbackToAndReleaseSavepoint(ctx context.Context, tx pgxQueryer, savepointName string) error {
	if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+savepointName); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+savepointName); err != nil {
		return err
	}
	return nil
}

func releaseSavepoint(ctx context.Context, tx pgxQueryer, savepointName string) error {
	_, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+savepointName)
	return err
}
