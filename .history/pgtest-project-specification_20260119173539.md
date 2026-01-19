# PGTest - PostgreSQL Transaction Proxy para Testes E2E

## Visão Geral do Projeto

**PGTest** é um serviço proxy que intercepta conexões PostgreSQL e gerencia transações compartilhadas por Test-ID, permitindo que múltiplos processos PHP compartilhem a mesma transação durante testes E2E.

### Problema que Resolve

Em testes E2E com PHP-FPM tradicional:
- Cada requisição HTTP = novo processo PHP = nova conexão = nova transação isolada
- Não é possível compartilhar transações entre múltiplas requisições do mesmo teste
- Rollback de uma transação não afeta outras transações do mesmo teste

**Solução:** Proxy que intercepta protocolo PostgreSQL e roteia queries para manter transações compartilhadas por Test-ID.

## Arquitetura

```
┌─────────────┐         ┌──────────────┐         ┌─────────────┐
│   Laravel   │────────▶│   PGTest     │────────▶│ PostgreSQL  │
│  (PHP-FPM)  │         │   (Proxy)    │         │   (Real)    │
└─────────────┘         └──────────────┘         └─────────────┘
                              │
                              │ Mantém pool de conexões
                              │ por Test-ID
                              │
                    ┌─────────┴─────────┐
                    │  Test-ID → Conn   │
                    │  abc123 → Conn1   │
                    │  xyz789 → Conn2   │
                    └───────────────────┘
```

## Requisitos Funcionais

### 1. Interceptação de Protocolo PostgreSQL

- **Escutar porta TCP** (configurável, padrão: 5432)
- **Implementar protocolo PostgreSQL wire** (versão 3.0)
- **Parse StartupMessage** para extrair Test-ID
- **Proxy de mensagens** Query/Response entre cliente e servidor

### 2. Identificação de Test-ID

**Método: Via Parâmetro de Conexão**
- O Test-ID é passado como um parâmetro adicional na conexão PostgreSQL
- **Não é mais extraído do database name** - o database name permanece o mesmo do PostgreSQL real
- Opções de implementação (em ordem de prioridade):
  1. **Via parâmetro customizado no StartupMessage:** 
     - Parâmetro: `pgtest.test_id=abc123` na URI de conexão
     - Extração: `startupMsg.Parameters["pgtest.test_id"]`
  2. **Via application_name:**
     - Parâmetro: `application_name=pgtest_abc123` na URI de conexão
     - Extração: regex `^pgtest_(.+)$` em `startupMsg.Parameters["application_name"]`
     - Retorna: `abc123`
  3. **Via variável de sessão (fallback):**
     - Após conexão estabelecida, cliente envia: `SET pgtest.test_id = 'abc123'`
     - PGTest intercepta e armazena na sessão

**Implementação:**
```go
func extractTestID(params map[string]string) (string, error) {
    // Prioridade 1: Parâmetro customizado
    if testID := params["pgtest.test_id"]; testID != "" {
        return testID, nil
    }
    
    // Prioridade 2: application_name
    if appName := params["application_name"]; appName != "" {
        if match := regexp.MustCompile(`^pgtest_(.+)$`).FindStringSubmatch(appName); match != nil {
            return match[1], nil
        }
    }
    
    return "", fmt.Errorf("Test-ID não encontrado nos parâmetros de conexão")
}
```

**Importante:**
- O database name (`params["database"]`) não é modificado e permanece como o nome real do banco PostgreSQL
- Apenas o endereço do servidor muda (conecta no PGTest ao invés do PostgreSQL direto)

### 3. Session Pinning

- **Mapa:** `Test-ID → conexão PostgreSQL dedicada`
- **Garantia:** Mesmo Test-ID sempre usa mesma conexão física
- **Isolamento:** Testes diferentes nunca compartilham conexão

### 4. Gerenciamento de Transações

**Lifecycle:**
1. Primeira requisição com Test-ID → cria conexão PostgreSQL → `BEGIN` → armazena
2. Requisições subsequentes → reutiliza conexão → continua mesma transação
3. Rollback → `ROLLBACK` → fecha conexão → remove do pool

**Timeout:**
- Configurável (padrão: 1 hora)
- Transações antigas são automaticamente rollbackadas

### 5. Comandos Especiais do PGTest

**Comandos de Gerenciamento de Transação:**

O interceptador reconhece comandos especiais no formato `pgtest <ação> <test_id>`:

| Comando | Descrição | Comportamento |
|---------|-----------|---------------|
| `pgtest begin abc123` | Inicia uma transação para o Test-ID | Conecta no banco PostgreSQL real, cria sessão com Test-ID `abc123`, executa `BEGIN`, armazena no pool |
| `pgtest rollback abc123` | Faz rollback de uma transação ativa | Localiza sessão do Test-ID `abc123`, executa `ROLLBACK`, fecha conexão, remove do pool |

**Implementação:**
- O interceptador detecta queries que começam com `pgtest`
- Parse do comando para extrair ação (`begin`/`rollback`) e Test-ID
- Executa ação correspondente no gerenciador de sessões
- Retorna resposta apropriada ao cliente

### 6. Interceptação de BEGIN/COMMIT/ROLLBACK

**Conversão Automática:**

| Comando do App | Nível > 0 (Savepoint) | Nível = 0 (Transação Externa) |
|----------------|----------------------|-------------------------------|
| `BEGIN` | `SAVEPOINT sp_n` | Bloqueado (já em transação) |
| `COMMIT` | `RELEASE SAVEPOINT sp_n` | Bloqueado (retorna SELECT 1) |
| `ROLLBACK` | `ROLLBACK TO SAVEPOINT sp_n` | Bloqueado (retorna SELECT 1) |

**Contador de Profundidade:**
- Cada `BEGIN` incrementa nível
- Cada `COMMIT`/`ROLLBACK` decrementa nível
- Nível 0 = transação externa controlada pelo PGTest

### 7. API de Controle HTTP (Apenas Monitoramento)

**Endpoints:**

```
GET /status?test_id=abc123
  - Retorna status da transação
  - Retorna: { "test_id": "abc123", "active": true, "level": 2, "created_at": "..." }

GET /list
  - Lista todas as transações ativas
  - Retorna: { "transactions": [...] }

POST /cleanup
  - Limpa transações expiradas (mais antigas que timeout)
  - Retorna: { "cleaned": 3 }
```

**Nota:** Os comandos `begin` e `rollback` são executados via comandos SQL (`pgtest begin abc123` e `pgtest rollback abc123`), não via endpoints HTTP. A API HTTP é usada apenas para monitoramento e gerenciamento administrativo.

## Especificações Técnicas

### Linguagem e Tecnologias

**Recomendado: Go**
- Performance excelente
- Concorrência nativa (goroutines)
- Bibliotecas: `github.com/jackc/pgx/v5`, `github.com/jackc/pgproto3/v3`

**Alternativas:**
- Node.js: `pg`, `pg-protocol`
- Python: `psycopg2`, `asyncpg`

### Estrutura de Dados

```go
type TestSession struct {
    TestID         string
    Conn           *pgx.Conn      // Conexão PostgreSQL real
    Transaction    *pgx.Tx        // Transação externa (nível 0)
    SavepointLevel int            // Contador de profundidade
    Savepoints     []string        // Stack: ["sp_1", "sp_2", ...]
    CreatedAt      time.Time
    LastActivity   time.Time
    mu             sync.RWMutex
}

type PGTest struct {
    Sessions       map[string]*TestSession
    PostgresHost   string
    PostgresPort   int
    PostgresDB     string
    PostgresUser   string
    PostgresPass   string
    Timeout        time.Duration
    mu             sync.RWMutex
}
```

### Fluxo de Conexão

1. **Cliente (Laravel) conecta:**
   ```
   StartupMessage {
     protocol_version: 3.0
     parameters: {
       database: "mydb"                    // Nome do banco real (não modificado)
       user: "myuser"
       pgtest.test_id: "abc123"            // Parâmetro customizado com Test-ID
       // OU application_name: "pgtest_abc123"
     }
   }
   ```

2. **PGTest extrai Test-ID:**
   ```go
   testID := extractTestID(startupMsg.Parameters)
   // Extrai de "pgtest.test_id" ou "application_name" (regex: ^pgtest_(.+)$)
   // Retorna: "abc123"
   ```

3. **PGTest obtém/reusa sessão:**
   ```go
   session := pgtest.GetOrCreateSession(testID)
   // Se não existe: cria conexão → BEGIN → armazena
   // Se existe: reutiliza
   ```

4. **PGTest responde AuthenticationOK**

5. **Proxy de queries:**
   - Cliente envia Query
   - PGTest intercepta BEGIN/COMMIT/ROLLBACK
   - PGTest roteia para conexão PostgreSQL real
   - Resposta volta para cliente

### Interceptação de Comandos SQL

**Parser de Queries:**

```go
func (p *PGTest) InterceptQuery(session *TestSession, query string) (string, error) {
    queryTrimmed := strings.TrimSpace(query)
    queryUpper := strings.ToUpper(queryTrimmed)
    
    // Detectar comandos especiais do PGTest
    if strings.HasPrefix(queryUpper, "PGTEST") {
        return p.handlePGTestCommand(queryTrimmed)
    }
    
    // Detectar BEGIN
    if strings.HasPrefix(queryUpper, "BEGIN") {
        return p.handleBegin(session)
    }
    
    // Detectar COMMIT
    if strings.HasPrefix(queryUpper, "COMMIT") {
        return p.handleCommit(session)
    }
    
    // Detectar ROLLBACK
    if strings.HasPrefix(queryUpper, "ROLLBACK") {
        return p.handleRollback(session)
    }
    
    // Query normal - não interceptar
    return query, nil
}

func (p *PGTest) handlePGTestCommand(query string) (string, error) {
    // Parse: "pgtest begin abc123" ou "pgtest rollback abc123"
    parts := strings.Fields(query)
    if len(parts) < 3 {
        return "", fmt.Errorf("comando pgtest inválido: %s", query)
    }
    
    action := strings.ToLower(parts[1])
    testID := parts[2]
    
    switch action {
    case "begin":
        // Conecta no banco com transação ativa
        session, err := p.GetOrCreateSession(testID)
        if err != nil {
            return "", err
        }
        // Sessão já tem transação iniciada, retorna sucesso
        return "SELECT 1", nil
        
    case "rollback":
        // Faz rollback e fecha conexão
        err := p.RollbackSession(testID)
        if err != nil {
            return "", err
        }
        return "SELECT 1", nil
        
    default:
        return "", fmt.Errorf("ação desconhecida: %s", action)
    }
}
```

**Implementação:**

```go
func (p *PGTest) handleBegin(session *TestSession) (string, error) {
    session.mu.Lock()
    defer session.mu.Unlock()
    
    session.SavepointLevel++
    savepointName := fmt.Sprintf("sp_%d", session.SavepointLevel)
    session.Savepoints = append(session.Savepoints, savepointName)
    
    return fmt.Sprintf("SAVEPOINT %s", savepointName), nil
}

func (p *PGTest) handleCommit(session *TestSession) (string, error) {
    session.mu.Lock()
    defer session.mu.Unlock()
    
    if session.SavepointLevel > 0 {
        // COMMIT de savepoint interno
        savepointName := session.Savepoints[len(session.Savepoints)-1]
        session.Savepoints = session.Savepoints[:len(session.Savepoints)-1]
        session.SavepointLevel--
        
        return fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName), nil
    } else {
        // Tentativa de COMMIT da transação externa - BLOQUEAR
        return "SELECT 1", nil // Query vazia que não faz nada
    }
}

func (p *PGTest) handleRollback(session *TestSession) (string, error) {
    session.mu.Lock()
    defer session.mu.Unlock()
    
    if session.SavepointLevel > 0 {
        // ROLLBACK para savepoint interno
        savepointName := session.Savepoints[len(session.Savepoints)-1]
        session.Savepoints = session.Savepoints[:len(session.Savepoints)-1]
        session.SavepointLevel--
        
        return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s; RELEASE SAVEPOINT %s", 
            savepointName, savepointName), nil
    } else {
        // Tentativa de ROLLBACK da transação externa - BLOQUEAR
        return "SELECT 1", nil
    }
}
```

## Configuração

### Variáveis de Ambiente

```env
# PostgreSQL Real (backend)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=mydb
POSTGRES_USER=user
POSTGRES_PASSWORD=pass

# PGTest (proxy)
PGTEST_LISTEN_PORT=5432
PGTEST_HTTP_PORT=8080
PGTEST_TIMEOUT=3600s

# Logging
PGTEST_LOG_LEVEL=info
PGTEST_LOG_FILE=/var/log/pgtest.log
```

### Arquivo de Configuração (opcional)

```yaml
# pgtest.yaml
postgres:
  host: localhost
  port: 5432
  database: mydb
  user: user
  password: pass

proxy:
  listen_port: 5432
  http_port: 8080
  timeout: 3600s

logging:
  level: info
  file: /var/log/pgtest.log
```

## Interface com Laravel

### Configuração no Laravel

```php
// config/database.php
'pgsql' => [
    'driver' => 'pgsql',
    'host' => env('DB_HOST', '127.0.0.1'), // PGTest proxy (no lugar do PostgreSQL)
    'port' => env('DB_PORT', '5432'),
    'database' => env('DB_DATABASE', 'laravel'), // Nome do banco real (não modifica)
    // ...
],

// .env - apenas muda o endereço do servidor
DB_HOST=pgtest-service-host  // Endereço do PGTest
DB_PORT=5432
DB_DATABASE=mydb             // Nome do banco PostgreSQL real
```

### Middleware Laravel (para adicionar Test-ID como parâmetro)

```php
// app/Http/Middleware/TestTransactionMiddleware.php
public function handle($request, Closure $next)
{
    $testId = $request->header('X-Test-Run-Id');
    
    if ($testId) {
        // Adiciona Test-ID como parâmetro de conexão
        // Opção 1: Via application_name
        Config::set('database.connections.pgsql.options', [
            PDO::ATTR_EMULATE_PREPARES => false,
        ]);
        // Modificar connection string para incluir: application_name=pgtest_{$testId}
        
        // Opção 2: Via variável de sessão após conexão
        DB::statement("SET pgtest.test_id = '{$testId}'");
    }
    
    return $next($request);
}
```

### Uso dos Comandos Especiais

```php
// Iniciar transação para um Test-ID específico
DB::statement("pgtest begin abc123");

// Fazer rollback de uma transação ativa
DB::statement("pgtest rollback abc123");
```

### Rollback no Final do Teste

```php
// No final do teste E2E - via comando SQL
DB::statement("pgtest rollback abc123");

// Ou via Laravel em um hook de teste
public function tearDown(): void
{
    if ($testId = $this->getTestId()) {
        DB::statement("pgtest rollback {$testId}");
    }
    parent::tearDown();
}
```

## Exemplos de Uso

### Cenário 1: Teste Simples

1. **Playwright inicia teste:**
   ```typescript
   const testId = generateTestId(); // "abc123"
   await page.setExtraHTTPHeaders({ 'X-Test-Run-Id': testId });
   ```

2. **Laravel conecta:**
   - Host: `pgtest-service-host` (no lugar do PostgreSQL)
   - Database name: `mydb` (nome real do banco)
   - Test-ID: `abc123` (via parâmetro `pgtest.test_id` ou `application_name`)
   - PGTest cria sessão → `BEGIN`

3. **Laravel executa queries:**
   - `INSERT INTO users ...` → executado na transação
   - `UPDATE products ...` → executado na mesma transação

4. **ORM emite BEGIN:**
   - Laravel: `BEGIN`
   - PGTest: `SAVEPOINT sp_1` (convertido)

5. **ORM emite COMMIT:**
   - Laravel: `COMMIT`
   - PGTest: `RELEASE SAVEPOINT sp_1` (convertido)

6. **Teste termina:**
   - Laravel: `DB::statement("pgtest rollback abc123")`
   - PGTest: `ROLLBACK` → todas as mudanças revertidas ✅

### Cenário 2: Múltiplas Requisições

1. **Requisição 1:** `POST /api/users` → cria usuário
2. **Requisição 2:** `GET /api/users` → lista usuários (vê o criado)
3. **Requisição 3:** `PUT /api/users/1` → atualiza usuário
4. **Rollback:** Todas as 3 requisições são revertidas juntas ✅

## Checklist de Implementação

### Fase 1: Core (MVP)

- [ ] Servidor TCP que escuta protocolo PostgreSQL
- [ ] Parse StartupMessage e extrair Test-ID de parâmetro de conexão (não do database name)
- [ ] Pool de conexões PostgreSQL por Test-ID
- [ ] Proxy básico de queries (sem interceptação)
- [ ] Interceptar comandos especiais `pgtest begin` e `pgtest rollback`
- [ ] Endpoint HTTP para rollback
- [ ] Testes básicos de conexão

### Fase 2: Interceptação

- [ ] Parser de queries SQL
- [ ] Detectar BEGIN/COMMIT/ROLLBACK
- [ ] Conversão em SAVEPOINT
- [ ] Contador de profundidade
- [ ] Bloqueio de COMMIT/ROLLBACK externo
- [ ] Testes de interceptação

### Fase 3: Features Avançadas

- [ ] Timeout automático de transações
- [ ] Cleanup de transações expiradas
- [ ] Endpoint de status (`/status`)
- [ ] Endpoint de listagem (`/list`)
- [ ] Logging estruturado
- [ ] Métricas (Prometheus/StatsD)

### Fase 4: Robustez

- [ ] Tratamento de erros (transação "aborted")
- [ ] Reconexão automática
- [ ] Health checks
- [ ] Graceful shutdown
- [ ] Testes de carga
- [ ] Documentação completa

### Fase 5: Deploy

- [ ] Container Docker
- [ ] Docker Compose para desenvolvimento
- [ ] Kubernetes manifests (opcional)
- [ ] CI/CD pipeline
- [ ] Monitoramento (logs, métricas, alertas)

## Estrutura do Projeto

```
pgtest/
├── cmd/
│   └── pgtest/
│       └── main.go              # Entry point
├── internal/
│   ├── proxy/
│   │   ├── server.go           # Servidor TCP PostgreSQL
│   │   ├── session.go          # Gerenciamento de sessões
│   │   └── interceptor.go      # Interceptação de queries
│   ├── postgres/
│   │   └── pool.go             # Pool de conexões
│   └── http/
│       └── api.go              # API HTTP de controle
├── pkg/
│   └── protocol/
│       └── parser.go           # Parser de protocolo PostgreSQL
├── config/
│   └── config.yaml             # Configuração
├── Dockerfile
├── docker-compose.yml
├── go.mod
├── go.sum
└── README.md
```

## Testes

### Testes Unitários

- Parser de Test-ID de parâmetros de conexão
- Interceptação de comandos `pgtest begin` e `pgtest rollback`
- Conversão BEGIN → SAVEPOINT
- Contador de profundidade
- Bloqueio de COMMIT/ROLLBACK

### Testes de Integração

- Conexão Laravel → PGTest → PostgreSQL
- Múltiplas requisições compartilhando transação
- Rollback funcional
- Timeout automático

### Testes E2E

- Teste completo com Laravel real
- Múltiplos testes paralelos
- Cleanup de transações

## Performance

### Benchmarks Esperados

- **Latência:** < 1ms overhead por query
- **Throughput:** > 10k queries/segundo
- **Conexões:** Suportar 100+ Test-IDs simultâneos
- **Memória:** < 100MB para 100 sessões ativas

## Segurança

- **Autenticação:** Validar credenciais PostgreSQL
- **Isolamento:** Garantir que Test-IDs não vazem dados
- **Rate Limiting:** Limitar requisições por Test-ID
- **Logs:** Não logar senhas ou dados sensíveis

## Monitoramento

### Métricas

- `pgtest_sessions_active` - Número de sessões ativas
- `pgtest_queries_total` - Total de queries processadas
- `pgtest_queries_duration_seconds` - Duração de queries
- `pgtest_interceptions_total` - Comandos interceptados
- `pgtest_rollbacks_total` - Rollbacks executados

### Logs

- Nível INFO: Conexões criadas/fechadas, rollbacks
- Nível DEBUG: Queries interceptadas, conversões
- Nível ERROR: Falhas de conexão, erros de proxy

## Documentação Adicional Necessária

1. **README.md** com:
   - Instalação
   - Configuração
   - Exemplos de uso
   - Troubleshooting

2. **API.md** com:
   - Endpoints HTTP detalhados
   - Exemplos de requisições/respostas
   - Códigos de erro

3. **ARCHITECTURE.md** com:
   - Diagramas detalhados
   - Decisões de design
   - Trade-offs

## Dependências Externas

- PostgreSQL (backend real)
- Go 1.21+ (se usar Go)
- Docker (para containerização)

## Próximos Passos Após Implementação

1. Integrar com projeto Laravel
2. Adicionar testes E2E que usam PGTest
3. Monitorar performance em produção
4. Iterar baseado em feedback

---

**Nota para Implementador:** Este documento contém todas as especificações necessárias para implementar o PGTest. Qualquer dúvida sobre requisitos específicos deve ser esclarecida antes de começar a implementação. O foco principal é garantir que múltiplos processos PHP possam compartilhar a mesma transação PostgreSQL através do proxy.
