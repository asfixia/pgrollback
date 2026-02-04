package protocol

import (
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ConvertFieldDescriptions converte FieldDescriptions do pgx para pgproto3
func ConvertFieldDescriptions(fieldDescs []pgconn.FieldDescription) []pgproto3.FieldDescription {
	fields := make([]pgproto3.FieldDescription, len(fieldDescs))
	for i, fieldDesc := range fieldDescs {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(fieldDesc.Name),
			TableOID:             fieldDesc.TableOID,
			TableAttributeNumber: fieldDesc.TableAttributeNumber,
			DataTypeOID:          fieldDesc.DataTypeOID,
			DataTypeSize:         fieldDesc.DataTypeSize,
			TypeModifier:         fieldDesc.TypeModifier,
			Format:               fieldDesc.Format,
		}
	}
	return fields
}

