package proxy

import (
	"github.com/jackc/pgx/v5/pgproto3"
)

// sendErrorToClient envia uma mensagem de erro para o cliente
func sendErrorToClient(backend *pgproto3.Backend, message string) {
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Message:  message,
		Code:     "XX000",
	})
	// Flush é necessário para garantir que a mensagem de erro seja enviada imediatamente
	backend.Flush()
}
