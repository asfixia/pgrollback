package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	ProtocolVersion        = 196608
	PostgresSSLRequestCode = 80877103 // Código da mensagem SSLRequest do PostgreSQL
	// specialRequestTotalBytes is the on-wire size of SSLRequest / CancelRequest (4-byte length + 4-byte code).
	specialRequestTotalBytes = 8
)

// IsSSLRequestLength reports whether the first int32 on the wire is PostgreSQL's special-request
// frame size (8 bytes total), i.e. SSLRequest or CancelRequest, as opposed to a StartupMessage length.
func IsSSLRequestLength(length int32) bool {
	return length == specialRequestTotalBytes
}

// ReadFrontendMessageLength reads the first 4 bytes of a PostgreSQL frontend connection as a big-endian int32.
// That value is either the total byte length of a StartupMessage (including the length field itself) or 8 for an
// SSLRequest / CancelRequest special frame (4-byte length + 4-byte request code).
func ReadFrontendMessageLength(r io.Reader) (length int32, err error) {
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return 0, fmt.Errorf("read frontend message length: %w", err)
	}
	return length, nil
}

// ReadSpecialRequestCode reads the 4-byte request code that follows the length field of a PostgreSQL
// special request (caller must have already read the length and verified IsSSLRequestLength(length)).
func ReadSpecialRequestCode(r io.Reader) (code int32, err error) {
	if err := binary.Read(r, binary.BigEndian, &code); err != nil {
		return 0, fmt.Errorf("read special request code: %w", err)
	}
	return code, nil
}

// IsPostgresSSLRequestCode reports whether code is the PostgreSQL SSLRequest payload (after the 4-byte length).
func IsPostgresSSLRequestCode(code int32) bool {
	return code == PostgresSSLRequestCode
}

type StartupMessage struct {
	ProtocolVersion int32
	Parameters      map[string]string
}

func ReadStartupMessage(reader io.Reader) (*StartupMessage, error) {
	var length int32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}

	if length < 8 {
		return nil, fmt.Errorf("invalid message length: %d", length)
	}

	var protocolVersion int32
	if err := binary.Read(reader, binary.BigEndian, &protocolVersion); err != nil {
		return nil, fmt.Errorf("failed to read protocol version: %w", err)
	}

	params := make(map[string]string)
	remaining := length - 8

	for remaining > 0 {
		var key string
		var value string

		keyBytes := make([]byte, 0)
		for {
			b := make([]byte, 1)
			if _, err := reader.Read(b); err != nil {
				return nil, fmt.Errorf("failed to read key: %w", err)
			}
			remaining--
			if b[0] == 0 {
				break
			}
			keyBytes = append(keyBytes, b[0])
		}
		key = string(keyBytes)

		if key == "" {
			break
		}

		valueBytes := make([]byte, 0)
		for {
			b := make([]byte, 1)
			if _, err := reader.Read(b); err != nil {
				return nil, fmt.Errorf("failed to read value: %w", err)
			}
			remaining--
			if b[0] == 0 {
				break
			}
			valueBytes = append(valueBytes, b[0])
		}
		value = string(valueBytes)

		params[key] = value
	}

	return &StartupMessage{
		ProtocolVersion: protocolVersion,
		Parameters:      params,
	}, nil
}

func WriteAuthenticationOK(writer io.Writer) error {
	message := []byte{
		'R',
		0, 0, 0, 8,
		0, 0, 0, 0,
	}
	_, err := writer.Write(message)
	return err
}

// WriteAuthenticationCleartextPassword solicita senha em texto claro do cliente
func WriteAuthenticationCleartextPassword(writer io.Writer) error {
	message := []byte{
		'R',
		0, 0, 0, 8,
		0, 0, 0, 3, // AuthenticationCleartextPassword
	}
	_, err := writer.Write(message)
	return err
}

func WriteReadyForQuery(writer io.Writer) error {
	message := []byte{
		'Z',
		0, 0, 0, 5,
		'I',
	}
	_, err := writer.Write(message)
	return err
}

func WriteErrorResponse(writer io.Writer, message string) error {
	// Formato PostgreSQL ErrorResponse:
	// Byte 'E' + Length (4 bytes) + Fields (cada campo: tipo byte + valor + null) + null final
	// O length inclui os 4 bytes do próprio length + o conteúdo
	msgBytes := []byte(message)
	severityBytes := []byte("ERROR")

	// Campos: 'S' + severity + null + 'M' + message + null + null final
	contentLength := 1 + len(severityBytes) + 1 + 1 + len(msgBytes) + 1 + 1
	// Length = 4 (bytes do length) + conteúdo
	totalLength := 4 + contentLength

	response := make([]byte, 0, totalLength+1)

	response = append(response, 'E') // ErrorResponse type
	response = append(response, byte(totalLength>>24), byte(totalLength>>16), byte(totalLength>>8), byte(totalLength))
	response = append(response, 'S') // Severity field
	response = append(response, severityBytes...)
	response = append(response, 0)   // null terminator for severity
	response = append(response, 'M') // Message field
	response = append(response, msgBytes...)
	response = append(response, 0) // null terminator for message
	response = append(response, 0) // final null terminator

	_, err := writer.Write(response)
	return err
}

// WriteSSLResponse responde à solicitação SSL do cliente
// 'S' = SSL permitido, 'N' = SSL não permitido
func WriteSSLResponse(writer io.Writer, allowSSL bool) error {
	if allowSSL {
		_, err := writer.Write([]byte{'S'})
		return err
	}
	_, err := writer.Write([]byte{'N'})
	return err
}
