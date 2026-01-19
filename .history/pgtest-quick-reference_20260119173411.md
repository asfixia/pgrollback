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

| Comando | Descrição |
|---------|-----------|
| `pgtest begin abc123` | Conecta no banco com uma transação ativa para o Test-ID `abc123` |
| `pgtest rollback abc123` | Faz rollback de uma transação ativa do Test-ID `abc123` |

## Conversão de Comandos

| App Envia | PGTest Converte (se nível > 0) | PGTest Bloqueia (se nível = 0) |
|-----------|-------------------------------|-------------------------------|
| `BEGIN` | `SAVEPOINT sp_n` | (já em transação) |
| `COMMIT` | `RELEASE SAVEPOINT sp_n` | `SELECT 1` (bloqueado) |
| `ROLLBACK` | `ROLLBACK TO SAVEPOINT sp_n` | `SELECT 1` (bloqueado) |

## API HTTP

```
POST /rollback?test_id=abc123  → Rollback e fecha conexão
GET  /status?test_id=abc123    → Status da transação
GET  /list                     → Lista transações ativas
POST /cleanup                  → Limpa transações expiradas
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
- **Porta HTTP:** 8080 (API de controle)

## Checklist Mínimo

- [ ] Servidor TCP PostgreSQL wire protocol
- [ ] Extrair Test-ID de parâmetro de conexão (não do database name)
- [ ] Pool de conexões por Test-ID
- [ ] Interceptar comandos especiais `pgtest begin` e `pgtest rollback`
- [ ] Interceptar BEGIN/COMMIT/ROLLBACK
- [ ] Converter em SAVEPOINT
- [ ] Endpoint HTTP /rollback
- [ ] Timeout automático

---

**Documento completo:** `pgtest-project-specification.md`
