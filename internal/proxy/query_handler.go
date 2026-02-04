package proxy

import (
	"context"
	"fmt"
	"log"
	"strings"

	"pgtest/pkg/protocol"
	"pgtest/pkg/sql"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ExecuteInterpretedQuery recebe uma query que já passou por parse e interceptação.
// Ela decide se é comando único ou múltiplos comandos e encaminha para o banco.
//
// sendReadyForQuery:
//   - true para fluxo "Simple Query" (envia ReadyForQuery ao final).
//   - false para fluxo "Extended Query" (não envia, espera-se recebimento de Sync depois).
func (p *proxyConnection) ExecuteInterpretedQuery(query string, sendReadyForQuery bool) error {
	commands := sql.SplitCommands(query)
	if len(commands) > 1 {
		return p.ForwardMultipleCommandsToDB(commands, sendReadyForQuery)
	}
	return p.ForwardCommandToDB(query, sendReadyForQuery)
}

// ForwardCommandToDB executa um único comando SQL na conexão/transação ativa.
func (p *proxyConnection) ForwardCommandToDB(query string, sendReadyForQuery bool) error {
	session := p.getSession()
	if session == nil || session.DB == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", p.testID)
	}

	// All commands run inside the transaction (session.DB uses tx for Query/Exec).
	if !session.DB.HasActiveTransaction() {
		return fmt.Errorf("sessão sem transação ativa para testID: %s", p.testID)
	}

	if sql.IsSelect(query) {
		return p.ExecuteSelectQuery(query, sendReadyForQuery)
	}

	var tag pgconn.CommandTag
	var err error

	log.Printf("[PROXY] ForwardCommandToDB: Executando via transação: %s", query)

	// Check if command is transaction control (SAVEPOINT, RELEASE, ROLLBACK)
	// We MUST NOT wrap these in a guard savepoint because RELEASE guard
	// would destroy the inner savepoints created by the command.
	cmdType := sql.AnalyzeCommand(query).Type
	isTransactionControl := cmdType == "SAVEPOINT" || cmdType == "RELEASE" || cmdType == "ROLLBACK"

	if isTransactionControl {
		tag, err = session.DB.Exec(context.Background(), query)
		if err != nil {
			log.Printf("[PROXY] Erro na execução transacional (TCL): %v", err)
			return fmt.Errorf("falha ao executar comando TCL: %w", err)
		}
	} else {
		tag, err = execQuerySafeSavepoint(context.Background(), session.DB, "pgtest_exec_guard", query)
		if err != nil {
			return err
		}
	}

	// Envia o CommandTag real ANTES do ReadyForQuery.
	// Aplica workaround para INSERT com oid=0 para compatibilidade com drivers.
	tagStr := tag.String()
	rowsAffected := tag.RowsAffected()
	originalTagStr := tagStr

	if strings.HasPrefix(tagStr, "INSERT 0 ") && rowsAffected >= 0 {
		tagStr = fmt.Sprintf("INSERT %d", rowsAffected)
		log.Printf("[PROXY] Workaround INSERT aplicado: '%s' -> '%s' (linhas=%d)", originalTagStr, tagStr, rowsAffected)
	}

	if tagStr != "" {
		//log.Printf("[PROXY] Enviando CommandComplete: '%s'", tagStr)
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tagStr)})
	} else {
		//log.Printf("[PROXY] Tag vazia recebida, enviando 'SELECT 0' default")
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
	}

	if sendReadyForQuery {
		log.Printf("[PROXY] Enviando ReadyForQuery (Simple Query)")
		p.SendReadyForQuery()
	} else {
		log.Printf("[PROXY] Não enviando ReadyForQuery (Fluxo Estendido)")
	}
	return nil
}

// ForwardMultipleCommandsToDB lida com strings contendo múltiplos comandos separados por ponto e vírgula.
func (p *proxyConnection) ForwardMultipleCommandsToDB(commands []string, sendReadyForQuery bool) error {
	session := p.getSession()
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", p.testID)
	}

	fullQuery := strings.Join(commands, "; ")
	if !strings.HasSuffix(fullQuery, ";") {
		fullQuery += ";"
	}

	pgConn := session.DB.PgConn()
	if pgConn == nil {
		return fmt.Errorf("sessão sem conexão para testID: '%s'", p.testID)
	}
	if !session.DB.HasActiveTransaction() {
		return fmt.Errorf("sessão existe mas sem transaction: '%s'", p.testID)
	}
	//mrr := pgConn.Exec(context.Background(), "savepoint ")

	mrr := pgConn.Exec(context.Background(), fullQuery)
	defer mrr.Close()

	var lastSelectResult *pgproto3.RowDescription
	var lastSelectRows []*pgproto3.DataRow
	var lastSelectTag []byte

	// Itera sobre todos os resultados
	for mrr.NextResult() {
		rr := mrr.ResultReader()
		if rr == nil {
			continue
		}

		fieldDescs := rr.FieldDescriptions()
		if len(fieldDescs) > 0 {
			// É um SELECT. O protocolo simples do Postgres normalmente retorna apenas
			// o resultado do último comando se forem múltiplos SELECTs, ou todos se o cliente suportar.
			// Aqui acumulamos para enviar o último (comportamento comum de drivers simples).
			fields := protocol.ConvertFieldDescriptions(fieldDescs)
			rowDesc := &pgproto3.RowDescription{Fields: fields}
			var rows []*pgproto3.DataRow

			rowCount := 0
			for rr.NextRow() {
				rowCount++
				values := rr.Values()
				valuesCopy := make([][]byte, len(values))
				for i, v := range values {
					if v != nil {
						valuesCopy[i] = make([]byte, len(v))
						copy(valuesCopy[i], v)
					}
				}
				rows = append(rows, &pgproto3.DataRow{Values: valuesCopy})
			}

			tag, err := rr.Close()
			if err != nil {
				return fmt.Errorf("erro ao fechar result reader: %w", err)
			}

			if rowCount > 0 {
				lastSelectResult = rowDesc
				lastSelectRows = rows
				lastSelectTag = []byte(tag.String())
			}
		} else {
			// Comando sem retorno de linhas (UPDATE, INSERT, SET, etc).
			// Envia o CommandComplete imediatamente.
			tag, err := rr.Close()
			if err != nil {
				return fmt.Errorf("erro ao fechar result reader: %w", err)
			}
			if tagStr := tag.String(); tagStr != "" {
				p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tagStr)})
			}
		}
	}

	// Se houve algum SELECT com resultados, envia agora o último acumulado.
	if lastSelectResult != nil {
		p.backend.Send(lastSelectResult)
		for _, row := range lastSelectRows {
			p.backend.Send(row)
		}
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: lastSelectTag})
	}

	if err := mrr.Close(); err != nil {
		return fmt.Errorf("erro ao processar múltiplos resultados: %w", err)
	}

	if err := p.backend.Flush(); err != nil {
		return fmt.Errorf("falha no flush de múltiplos resultados: %w", err)
	}

	if sendReadyForQuery {
		p.SendReadyForQuery()
	}
	return nil
}

func (p *proxyConnection) ExecuteSelectQueryFromPreparedStatement(preparedStatement string, sendReadyForQuery bool) (pgx.Rows, error) {
	session := p.getSession()
	if session == nil {
		return nil, fmt.Errorf("sessão não encontrada para testID: %s", p.testID)
	}

	query := fmt.Sprintf(`
		SELECT
			statement
		FROM pg_prepared_statements
		WHERE name = '%s';
		`, preparedStatement)
	rows, err := querySafeSavepoint(context.Background(), session.DB, "pgtest_exec_guard", query)
	//if err != nil {
	//	return err
	//}
	//return p.ExecuteSelectQuery(tag.String(), sendReadyForQuery)
	return rows, err
}

// ExecuteSelectQuery executa um SELECT simples e envia os resultados.
func (p *proxyConnection) ExecuteSelectQuery(query string, sendReadyForQuery bool) error {
	session := p.getSession()
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", p.testID)
	}

	rows, err := session.DB.Query(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err := p.SendSelectResults(rows); err != nil {
		return err
	}

	if sendReadyForQuery {
		p.SendReadyForQuery()
	}
	return nil
}
