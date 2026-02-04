package proxy

// Este arquivo foi dividido em módulos menores para melhor organização:
// - query_handler.go: funções de processamento de queries (handleMultiCommandQuery, handleResultSetQuery)
// - response.go: funções de resposta do protocolo (sendErrorResponse, sendSelect1Response, sendCommandComplete)
// - interceptors.go: interceptação e modificação de queries (InterceptQuery, handlePGTestCommand, etc.)
