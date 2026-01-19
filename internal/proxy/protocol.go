package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	ProtocolVersion = 196608
)

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
	msgBytes := []byte(message)
	length := 4 + len(msgBytes) + 1
	
	response := make([]byte, 0, length+1)
	response = append(response, 'E')
	response = append(response, byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
	response = append(response, 'M')
	response = append(response, msgBytes...)
	response = append(response, 0)
	response = append(response, 0)

	_, err := writer.Write(response)
	return err
}
