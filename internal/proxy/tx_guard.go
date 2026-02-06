package proxy

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

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
	savePoint pgx.Tx
	closed    bool
}

func (r *guardedRows) Close() {
	if r.closed {
		return
	}
	r.closed = true

	r.Rows.Close()

	if err := r.Rows.Err(); err != nil {
		if guardErr := r.savePoint.Rollback(r.ctx); guardErr != nil {
			log.Printf("[PROXY] FATAL: Falha ao reverter savepoint após erro em rows: %v", guardErr)
		}
		return
	}

	if releaseErr := r.savePoint.Commit(r.ctx); releaseErr != nil {
		log.Printf("[PROXY] Aviso: Falha ao liberar savepoint de guarda: %v", releaseErr)
	}
}

func SafeQuery(
	ctx context.Context,
	tx *realSessionDB,
	query string,
	args ...any,
) (pgx.Rows, error) {

	savePoint, err := tx.tx.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("Falha ao guardar transacao: %w, da sql: querySql: '''%s'''", err, query)
	}

	rows, err := savePoint.Query(ctx, query, args...)
	if err != nil {
		savePoint.Rollback(ctx)
		log.Printf("[PROXY] Erro na execução (revertendo guarda): %v", err)
		return nil, fmt.Errorf("falha ao executar query: %w", err)
	}
	savePoint.Commit(ctx)

	// Wrap rows so savepoint is finalized on Close()
	return &guardedRows{
		Rows:      rows,
		ctx:       ctx,
		tx:        tx,
		savePoint: savePoint,
	}, nil
}

// func execQuerySafeSavepoint(ctx context.Context, tx pgxQueryer, query string) (tag pgconn.CommandTag, err error) {
// 	savepointName := newGuardSavepointName()
// 	// Cria um savepoint interno antes de executar o comando.
// 	// Se o comando falhar, fazemos rollback para este savepoint para não abortar a transação principal.
// 	if _, err = tx.Exec(ctx, "SAVEPOINT "+savepointName); err != nil {
// 		log.Printf("[PROXY] - Falha ao criar savepoint de guarda: %v", err)
// 		return tag, fmt.Errorf("falha interna de transação: %w", err)
// 	}

// 	// Finaliza o guard automaticamente:
// 	// - pânico -> rollback+release e repanica
// 	// - erro   -> rollback+release e retorna erro original
// 	// - ok     -> release
// 	defer func() {
// 		if p := recover(); p != nil {
// 			if guardErr := rollbackToAndReleaseSavepoint(ctx, tx, savepointName); guardErr != nil {
// 				log.Printf("[PROXY] FATAL: Falha ao reverter savepoint de guarda após pânico: %v", guardErr)
// 			}
// 			panic(p)
// 		}

// 		if err != nil {
// 			if guardErr := rollbackToAndReleaseSavepoint(ctx, tx, savepointName); guardErr != nil {
// 				log.Printf("[PROXY] FATAL: Falha ao reverter savepoint de guarda: %v", guardErr)
// 			}
// 			return
// 		}

// 		if releaseErr := releaseSavepoint(ctx, tx, savepointName); releaseErr != nil {
// 			log.Printf("[PROXY] Aviso: Falha ao liberar savepoint de guarda: %v", releaseErr)
// 		}
// 	}()

// 	tag, err = tx.Exec(ctx, query)
// 	if err != nil {
// 		// Retorna o erro original do comando para o cliente (a reversão ocorre no defer).
// 		log.Printf("[PROXY] Erro na execução (revertendo guarda): %v", err)
// 		return tag, fmt.Errorf("falha ao executar comando: %w", err)
// 	}

// 	return tag, nil
// }

// func rollbackToAndReleaseSavepoint(ctx context.Context, tx pgxQueryer, savepointName string) error {
// 	if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+savepointName); err != nil {
// 		return err
// 	}
// 	if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+savepointName); err != nil {
// 		return err
// 	}
// 	return nil
// }

func releaseSavepoint(ctx context.Context, tx pgxQueryer, savepointName string) error {
	_, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+savepointName)
	return err
}

func newGuardSavepointName() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprintf("pgtest_exec_guard_%v", r.Int31())
}
