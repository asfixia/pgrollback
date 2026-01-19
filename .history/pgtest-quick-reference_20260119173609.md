# PGTest - Quick Reference

## O Que É

Proxy PostgreSQL que permite múltiplos processos PHP compartilharem a mesma transação durante testes E2E.

## Como Funciona

1. Laravel conecta ao PGTest (como se fosse PostgreSQL) - apenas muda o endereço/URI do servidor
2. PGTest recebe Test-ID como parâmetro adicional na conexão (ex: `pgtest.test_id=abc123`)
3. PGTest mantém pool: `Test-ID → conexão PostgreSQL`
4. Múltiplas requisições com mesmo Test-ID → mesma conexão → mesma transação ✅
5. Comandos especiais para gerenciar transações: `pgtest begin abc123` e `pgtest rollback abc123`

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

```php
// .env - apenas muda o endereço do servidor
DB_HOST=pgtest-host  // Endereço do PGTest (no lugar do PostgreSQL)
DB_PORT=5432
DB_DATABASE=mydb     // Nome do banco real (não precisa modificar)

// Conexão com Test-ID como parâmetro adicional
// Exemplo de URI: postgresql://user:pass@pgtest-host:5432/mydb?pgtest.test_id=abc123
Config::set('database.connections.pgsql.options', [
    PDO::MYSQL_ATTR_INIT_COMMAND => "SET pgtest.test_id = '{$testId}'"
]);
// Ou via application_name: application_name=pgtest_abc123
```

## Stack Recomendado

- **Linguagem:** Go
- **Bibliotecas:** `pgx/v5`, `pgproto3/v3`
- **Porta TCP:** 5432 (protocolo PostgreSQL)
- **Sem API HTTP:** Todas as operações via comandos SQL

## Checklist Mínimo

- [ ] Servidor TCP PostgreSQL wire protocol
- [ ] Extrair Test-ID de parâmetro de conexão (não do database name)
- [ ] Pool de conexões por Test-ID
- [ ] Interceptar comandos especiais `pgtest begin` e `pgtest rollback`
- [ ] Interceptar BEGIN/COMMIT/ROLLBACK
- [ ] Converter em SAVEPOINT
- [ ] Endpoint HTTP /status (monitoramento)
- [ ] Timeout automático

---

**Documento completo:** `pgtest-project-specification.md`
