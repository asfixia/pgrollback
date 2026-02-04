package proxy

import (
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgproto3"
)

// RunMessageLoop é o loop principal que processa as mensagens do cliente.
// Ele mantém a conexão aberta e despacha cada mensagem para o handler apropriado.
func (p *proxyConnection) RunMessageLoop() {
	defer p.clientConn.Close()

	// Log para rastrear qual conexão TCP está processando mensagens
	remoteAddr := p.clientConn.RemoteAddr().String()
	testID := p.testID
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
			//p.mu.Lock()
			//p.lastQuery = "" // Limpa a query armazenada para evitar execução duplicada
			//p.inExtendedQuery = false
			//p.mu.Unlock()
			if err := p.ProcessSimpleQuery(queryStr); err != nil {
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
			if p.getSession() == nil {
				p.SendErrorResponse(fmt.Errorf("sessão não encontrada para testID: %s", p.testID))
				continue
			}

			interceptedQuery, err := p.interceptQuery(msg.Query)
			if err != nil {
				//p.mu.Lock()
				//p.lastQuery = "" // Limpa a query armazenada para evitar execução duplicada
				//p.inExtendedQuery = false
				//p.mu.Unlock()
				p.SendErrorResponse(err)
				continue
			}

			// Armazena a query (possivelmente modificada) para ser executada posteriormente na fase Execute.
			p.mu.Lock()
			p.lastQuery = interceptedQuery
			//p.inExtendedQuery = true
			p.mu.Unlock()

			// Confirma o Parse para o cliente
			p.backend.Send(&pgproto3.ParseComplete{})
			p.backend.Flush()

		case *pgproto3.Bind:
			///**
			//i have a prepared_statement "stmtcache_dbd2f4889840c4d053ba6d2d04f62d8d39895a983820adda"
			//How can i know the contents of this saved query?
			//*/
			//getQueryStatement := fmt.Sprintf(`
			//	SELECT
			//		name,
			//		statement,
			//		parameter_types,
			//		from_sql,
			//		prepare_time
			//	FROM pg_prepared_statements
			//	WHERE name = '%s';
			//	`, msg.PreparedStatement)
			//rows1, err := p.ExecuteSelectQueryFromPreparedStatement(getQueryStatement, true)
			//if err != nil {
			//	p.SendErrorResponse(err)
			//	continue
			//}
			//log.Printf("[PROXY] SAVEDQuery Statement: %s", printR(rows1))
			// Flow "Extended Query": Fase 2 - Bind de parâmetros.
			// Nossos testes geralmente não usam parâmetros complexos nesta camada de proxy ainda,
			// então apenas confirmamos o Bind.
			p.backend.Send(&pgproto3.BindComplete{})
			p.backend.Flush()

		case *pgproto3.Execute:
			// Flow "Extended Query": Fase 3 - Execução.
			// Executa a query que foi preparada na fase Parse.
			p.mu.Lock()
			query := p.lastQuery
			//p.inExtendedQuery = false
			p.mu.Unlock()

			if query == "" {
				p.SendErrorResponse(fmt.Errorf("nenhuma query encontrada para execução (fase Parse pulada?)"))
				continue
			}

			// Executa sem enviar ReadyForQuery, pois no fluxo estendido o Sync vem depois.
			if err := p.ProcessExtendedQuery(query); err != nil {
				log.Fatalf("[PROXY] Erro ao processar Execução Estendida: %v", err)
				p.SendErrorResponse(err)
			}
			//p.mu.Lock()
			//p.lastQuery = ""
			//p.inExtendedQuery = false
			//p.mu.Unlock()
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
			// O cliente quer fechar um Portal ou Statement.
			log.Printf("[PROXY] Close recebido (testID=%s, conn=%s)", testID, remoteAddr)
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
func (p *proxyConnection) ProcessSimpleQuery(query string) error {
	session := p.getSession()
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", p.testID)
	}

	interceptedQuery, err := p.interceptQuery(query)
	if err != nil {
		return err
	}

	//Danilo aqui precisa verificar se a consulta é um full rollback.
	if interceptedQuery == "-- fullrollback" {
		session.mu.Lock()
		p.mu.Lock()
		p.lastQuery = ""
		p.mu.Unlock()
		session.mu.Unlock()
	}
	// Se a interceptação "engoliu" a query (retornou vazia ou marcador), apenas finalizamos.
	// Isso acontece com comandos pgtest internos ou quando queremos silenciar uma query.
	if interceptedQuery == "" || interceptedQuery == "-- fullrollback" {
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
		p.SendReadyForQuery()
		return nil
	}

	// true = Enviar ReadyForQuery ao final
	return p.ExecuteInterpretedQuery(interceptedQuery, true)
}

// ProcessExtendedQuery lida com a fase de execução do fluxo estendido (pgproto3.Execute).
// Executa a query mas NÃO envia ReadyForQuery, pois o cliente enviará um Sync depois.
func (p *proxyConnection) ProcessExtendedQuery(query string) error {
	session := p.getSession()
	if session == nil {
		return fmt.Errorf("sessão não encontrada para testID: %s", p.testID)
	}

	interceptedQuery, err := p.interceptQuery(query)
	if err != nil {
		return err
	}

	if interceptedQuery == "" || interceptedQuery == "-- intercepted" {
		p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
		return nil
	}

	// false = NÃO enviar ReadyForQuery (esperar Sync)
	return p.ExecuteInterpretedQuery(interceptedQuery, false)
}
