package protocol

import (
	"encoding/binary"
	"strconv"

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

// DataTypeSizeForOID returns the typical size for a type OID (-1 for variable-length).
// INT8/bigint = 8; TEXT/varchar = -1.
func DataTypeSizeForOID(oid uint32) int16 {
	switch oid {
	case 20: // INT8OID
		return 8
	case 23: // INT4OID
		return 4
	default:
		return -1 // variable length (TEXT, etc.)
	}
}

// FieldDescriptionsFromNamesAndOIDs builds pgproto3.FieldDescription slice from parallel name and OID slices.
// Used for Describe (Portal/Statement) when the query has RETURNING so clients see the correct result shape.
// Sets DataTypeSize so clients (e.g. PHP PDO) that expect it get a valid value.
func FieldDescriptionsFromNamesAndOIDs(names []string, oids []uint32) []pgproto3.FieldDescription {
	if len(names) == 0 || len(names) != len(oids) {
		return nil
	}
	fields := make([]pgproto3.FieldDescription, len(names))
	for i := range names {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(names[i]),
			TableOID:             0, // RETURNING columns are expressions, not table attributes
			TableAttributeNumber: 0,
			DataTypeOID:          oids[i],
			DataTypeSize:         DataTypeSizeForOID(oids[i]),
			TypeModifier:         -1,
			Format:               0, // text
		}
	}
	return fields
}

// RawValueToText converts a single wire-format value to text (Format 0) for the given type OID.
// Used when sending synthetic RowDescription (Format 0) so DataRow values match; backend may send binary.
func RawValueToText(oid uint32, raw []byte) []byte {
	if raw == nil {
		return nil
	}
	switch oid {
	case 20: // INT8OID
		if len(raw) == 8 {
			return []byte(strconv.FormatInt(int64(binary.BigEndian.Uint64(raw)), 10))
		}
	case 23: // INT4OID
		if len(raw) == 4 {
			return []byte(strconv.FormatInt(int64(int32(binary.BigEndian.Uint32(raw))), 10))
		}
	}
	// TEXT and other types: assume already UTF-8
	return raw
}

