# PGTest - Quick Reference

## O Que É

Proxy PostgreSQL que permite múltiplos processos PHP compartilharem a mesma transação durante testes E2E.

## Como Funciona

1. Laravel conecta ao PGTest (como se fosse PostgreSQL) - apenas muda o endereço/URI do servidor
2. **Test-ID é passado via `application_name` na connection string (padrão PostgreSQL)**
3. Formato: `application_name=pgtest_abc123` → PGTest extrai Test-ID `abc123`
4. PGTest usa locks para garantir thread-safety ao acessar pool de conexões
5. PGTest conecta ao PostgreSQL real sem passar o Test-ID (usa `application_name` normal)
6. PGTest mantém pool interno com locks: `Test-ID → conexão PostgreSQL`
7. **Múltiplas requisições com mesmo Test-ID → mesma conexão → mesma transação ✅**
8. **Cada Test-ID diferente → transação isolada ✅**
9. Comandos especiais para gerenciar transações: `pgtest begin abc123` e `pgtest rollback abc123`

## Comandos Especiais do PGTest

O interceptador reconhece comandos especiais para gerenciar transações:

| Comando | Descrição | Retorno |
|---------|-----------|---------|
| `pgtest begin abc123` | Conecta no banco com uma transação ativa para o Test-ID `abc123` | `SELECT 1` (sucesso) |
| `pgtest rollback abc123` | Faz rollback de uma transação ativa do Test-ID `abc123` | `SELECT 1` (sucesso) |
| `pgtest status abc123` | Retorna status da transação do Test-ID `abc123` | ResultSet com colunas: `test_id`, `active`, `level`, `created_at` |
| `pgtest list` | Lista todas as transações ativas | ResultSet com todas as sessões ativas |
| `pgtest cleanup` | Limpa transações expiradas (mais antigas que timeout) | `SELECT N` (número de transações limpas) |

## Conversão de Comandos

| App Envia | PGTest Converte (se nível > 0) | PGTest Bloqueia (se nível = 0) |
|-----------|-------------------------------|-------------------------------|
| `BEGIN` | `SAVEPOINT sp_n` | (já em transação) |
| `COMMIT` | `RELEASE SAVEPOINT sp_n` | `SELECT 1` (bloqueado) |
| `ROLLBACK` | `ROLLBACK TO SAVEPOINT sp_n` | `SELECT 1` (bloqueado) |

## Exemplos de Uso

```php
// Iniciar transação para um Test-ID
DB::statement("pgtest begin abc123");

// Verificar status de uma transação
$status = DB::select("pgtest status abc123");
// Retorna: [{ test_id: 'abc123', active: true, level: 2, created_at: '...' }]

// Listar todas as transações ativas
$sessions = DB::select("pgtest list");
// Retorna: [{ test_id: 'abc123', ... }, { test_id: 'xyz789', ... }]

// Fazer rollback de uma transação
DB::statement("pgtest rollback abc123");

// Limpar transações expiradas
$cleaned = DB::select("pgtest cleanup");
// Retorna: [{ cleaned: 3 }]
```

## Configuração Laravel

**Forma padrão PostgreSQL - via connection string:**

```php
// .env - apenas muda o endereço do servidor
DB_HOST=pgtest-host  // Endereço do PGTest (no lugar do PostgreSQL)
DB_PORT=5432
DB_DATABASE=mydb     // Nome do banco real

// Middleware: modificar connection string com application_name (padrão PostgreSQL)
public function handle($request, Closure $next)
{
    $testId = $request->header('X-Test-Run-Id');
    
    if ($testId) {
        // Modifica a connection string para incluir application_name
        // application_name é um parâmetro padrão do PostgreSQL - todas as bibliotecas suportam
        $dsn = config('database.connections.pgsql.dsn') 
            ?: "pgsql:host=" . config('database.connections.pgsql.host') 
            . ";port=" . config('database.connections.pgsql.port')
            . ";dbname=" . config('database.connections.pgsql.database')
            . ";application_name=pgtest_{$testId}";
        
        Config::set('database.connections.pgsql.dsn', $dsn);
    }
    
    return $next($request);
}
```

**Ou via URI (também padrão PostgreSQL):**
```php
// Connection string URI format (padrão PostgreSQL)
$dsn = "postgresql://user:pass@pgtest-host:5432/mydb?application_name=pgtest_{$testId}";
```

**Importante:** 
- `application_name` é um parâmetro padrão do PostgreSQL (suportado por todas as bibliotecas)
- O Test-ID é usado apenas pelo PGTest para roteamento interno
- Cada Test-ID tem sua própria transação isolada
- PGTest usa locks para garantir thread-safety

## Stack Recomendado

- **Linguagem:** Go
- **Bibliotecas:** `pgx/v5`, `pgproto3/v3`
- **Porta TCP:** 5432 (protocolo PostgreSQL)
- **Sem API HTTP:** Todas as operações via comandos SQL

## Checklist Mínimo

- [ ] Servidor TCP PostgreSQL wire protocol
- [ ] Extrair Test-ID de `application_name` na connection string (padrão PostgreSQL)
- [ ] Pool de conexões por Test-ID com locks para thread-safety
- [ ] Locks (sync.RWMutex) para acesso seguro ao pool em ambiente concorrente
- [ ] Interceptar comandos especiais: `pgtest begin`, `pgtest rollback`, `pgtest status`, `pgtest list`, `pgtest cleanup`
- [ ] Interceptar BEGIN/COMMIT/ROLLBACK
- [ ] Converter em SAVEPOINT
- [ ] Retornar ResultSet para comandos de consulta (`status`, `list`)
- [ ] Timeout automático

---

**Documento completo:** `pgtest-project-specification.md`
