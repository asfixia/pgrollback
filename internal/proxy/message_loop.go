package proxy

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

// RunMessageLoop é o loop principal que processa as mensagens do cliente.
// Ele mantém a conexão aberta e despacha cada mensagem para o handler apropriado.
func (p *proxyConnection) RunMessageLoop(testID string) {
	defer p.clientConn.Close()

	// Log para rastrear qual conexão TCP está processando mensagens
	remoteAddr := p.clientConn.RemoteAddr().String()
	log.Printf("[PROXY] Iniciando loop de mensagens (testID=%s, conn=%s)", testID, remoteAddr)
	defer log.Printf("[PROXY] Finalizando loop de mensagens (testID=%s, conn=%s)", testID, remoteAddr)

	for {
		msg, err := p.backend.Receive()
		if err != nil {
			//if err != io.EOF {
			log.Printf("[PROXY] xxxxxxx Erro ao receber mensagem do cliente (testID=%s, conn=%s): %v", testID, remoteAddr, err)
			//}
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			// Flow "Simple Query": O cliente envia uma string SQL direta.
			// Espera-se que retornemos RowDescription, DataRow(s), CommandComplete e ReadyForQuery.
			queryStr := msg.String
			log.Printf("[PROXY] Query Simples Recebida (testID=%s, conn=%s): %s", testID, remoteAddr, queryStr)
			if os.Getenv("PGTEST_LOG_MESSAGE_ORDER") == "1" {
				preview := queryStr
				if len(preview) > 60 {
					preview = strings.TrimSpace(preview[:60]) + "..."
				}
				log.Printf("[MSG_ORDER] RECV SimpleQuery: %s", preview)
			}
			//p.mu.Lock()
			//p.lastQuery = "" // Limpa a query armazenada para evitar execução duplicada
			//p.inExtendedQuery = false
			//p.mu.Unlock()
			if err := p.ProcessSimpleQuery(testID, queryStr); err != nil {
				log.Printf("[PROXY] Erro ao processar Query Simples: %v", err)
				p.SendErrorResponse(err)
			} else {
				log.Printf("[PROXY] Query Simples processada com sucesso: %s", queryStr)
			}
			p.backend.Flush()

		case *pgproto3.Parse:
			// Flow "Extended Query": Fase 1 - Preparação.
			// O cliente envia o SQL para ser parseado e preparado.
			// Interceptamos aqui para modificar o SQL se necessário (ex: BEGIN -> SAVEPOINT).
			session := p.server.Pgtest.GetSession(testID)
			if session == nil {
				p.SendErrorResponse(fmt.Errorf("sessão não encontrada para testID: %s", testID))
				continue
			}

			interceptedQuery, err := p.server.Pgtest.InterceptQuery(testID, msg.Query)
			if err != nil {
				p.SendErrorResponse(err)
				continue
			}

			// Store query by statement name so Execute can run the correct query for each portal.
			if session.DB != nil {
				session.DB.SetPreparedStatement(msg.Name, interceptedQuery)
			}

			// Confirma o Parse para o cliente
			p.backend.Send(&pgproto3.ParseComplete{})
			p.backend.Flush()

		case *pgproto3.Bind:
			// Flow "Extended Query": Fase 2 - Bind de parâmetros.
			// Record which portal is bound to which statement so Execute runs the correct query.
			if session := p.server.Pgtest.GetSession(testID); session != nil && session.DB != nil {
				session.DB.BindPortal(msg.DestinationPortal, msg.PreparedStatement)
			}
			p.backend.Send(&pgproto3.BindComplete{})
			p.backend.Flush()

		case *pgproto3.Execute:
			// Flow "Extended Query": Fase 3 - Execução.
			// Resolve portal -> statement -> query and execute that query.
			session := p.server.Pgtest.GetSession(testID)
			if session == nil || session.DB == nil {
				p.SendErrorResponse(fmt.Errorf("sessão não encontrada para testID: %s", testID))
				continue
			}
			query, ok := session.DB.QueryForPortal(msg.Portal)
			if !ok {
				p.SendErrorResponse(fmt.Errorf("portal ou statement não encontrado para execução (portal=%q)", msg.Portal))
				continue
			}

			// Executa sem enviar ReadyForQuery, pois no fluxo estendido o Sync vem depois.
			if err := p.ProcessExtendedQuery(testID, query); err != nil {
				log.Printf("[PROXY] Erro ao processar Execução Estendida: %v", err)
				p.SendErrorResponse(err)
			}
			p.backend.Flush()

		case *pgproto3.Describe:
			// O cliente pede descrição dos tipos. Retornamos vazio por enquanto
			// pois o pgtest foca em emular o comportamento de comandos, não a tipagem estrita.
			p.backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{}})
			p.backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{}})
			p.backend.Flush()

		case *pgproto3.Sync:
			// Flow "Extended Query": Finalização.
			// O cliente indica que terminou a sequência de comandos estendidos.
			// Agora sim enviamos ReadyForQuery.
			//p.mu.Lock()
			//p.lastQuery = ""
			////p.inExtendedQuery = false
			//p.mu.Unlock()
			p.server.Pgtest.GetSession(testID).DB.conn.PgConn().SyncConn(context.Background())
			p.SendReadyForQuery()
			p.backend.Flush()

		case *pgproto3.Terminate:
			// Cliente solicitou desconexão graciosa.
			return

		case *pgproto3.Flush:
			// O cliente quer forçar o envio de dados bufferizados.
			// Como nosso backend faz flush automático na maioria dos sends, apenas logamos.
			log.Printf("[PROXY] Flush recebido (testID=%s, conn=%s)", testID, remoteAddr)
			p.backend.Flush()

		case *pgproto3.Close:
			// O cliente quer fechar um Portal ou Statement. Remove dos maps para evitar crescimento indefinido.
			if session := p.server.Pgtest.GetSession(testID); session != nil && session.DB != nil {
				session.DB.CloseStatementOrPortal(msg.ObjectType, msg.Name)
			}
			p.backend.Send(&pgproto3.CloseComplete{})
			p.backend.Flush()

		case *pgproto3.CopyData:
			// Mensagens de tráfego de dados (COPY). Ignoramos no log para evitar spam,
			// mas mantemos o fallback seguro de enviar ReadyForQuery para não travar.
			log.Printf("[PROXY] CopyData ignorado (testID=%s, conn=%s)", testID, remoteAddr)
			p.SendReadyForQuery()
			p.backend.Flush()

		default:
			// Captura qualquer outra mensagem não tratada explicitamente.
			log.Printf("[PROXY] ----------------- Mensagem não tratada: %T (testID=%s, conn=%s) - Enviando ReadyForQuery como fallback", msg, testID, remoteAddr)
			p.SendReadyForQuery()
			p.backend.Flush()
		}
	}
}

// ProcessSimpleQuery lida com o fluxo de "Simple Query" (pgproto3.Query).
// Intercepta o SQL, executa e garante o envio de ReadyForQuery ao final via executeQuery(..., true).
func (p *proxyConnection) ProcessSimpleQuery(testID string, query string) error {
	session := p.server.Pgtest.GetSession(testID)
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
	}

	if strings.Contains(query, "SAVEPOINT b") {
		log.Printf("[PROXY] ProcessSimpleQuery: query = %s", query)
	}
	interceptedQuery, err := p.server.Pgtest.InterceptQuery(testID, query)
	if err != nil {
		return err
	}

	if interceptedQuery == FULLROLLBACK_SENTINEL && session.DB != nil {
		session.DB.ClearLastQuery()
	}
	// Se a interceptação "engoliu" a query (retornou vazia ou marcador), apenas finalizamos.
	// Isso acontece com comandos pgtest internos ou quando queremos silenciar uma query.
	if interceptedQuery == "" || interceptedQuery == FULLROLLBACK_SENTINEL {
		if os.Getenv("PGTEST_LOG_MESSAGE_ORDER") == "1" {
			log.Printf("[MSG_ORDER] SEND CommandComplete: SELECT (intercepted)")
			log.Printf("[MSG_ORDER] SEND ReadyForQuery")
		}
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
		p.SendReadyForQuery()
		return nil
	}

	// true = Enviar ReadyForQuery ao final
	return p.ExecuteInterpretedQuery(testID, interceptedQuery, true)
}

// ProcessExtendedQuery lida com a fase de execução do fluxo estendido (pgproto3.Execute).
// Executa a query mas NÃO envia ReadyForQuery, pois o cliente enviará um Sync depois.
func (p *proxyConnection) ProcessExtendedQuery(testID string, query string) error {
	if p.server.Pgtest.GetSession(testID) == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", testID)
	}

	interceptedQuery, err := p.server.Pgtest.InterceptQuery(testID, query)
	if err != nil {
		return err
	}

	if interceptedQuery == "" || interceptedQuery == "-- intercepted" {
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
		return nil
	}

	// false = NÃO enviar ReadyForQuery (esperar Sync)
	return p.ExecuteInterpretedQuery(testID, interceptedQuery, false)
}
