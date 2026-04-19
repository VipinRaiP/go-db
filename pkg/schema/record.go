package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// For Milestone 2: Fixed Schema
// We'll assume a record consists of an ID (uint32) and Text Data (fixed string of 255 chars).
const MaxTextSize = 255

type Record struct {
	ID   uint32
	Data string // Limited to MaxTextSize
}

// Serialize converts a Record into a byte array
func (r *Record) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)
	
	// Write ID
	err := binary.Write(buf, binary.LittleEndian, r.ID)
	if err != nil {
		return nil, err
	}

	// Write Data length
	if len(r.Data) > MaxTextSize {
		return nil, fmt.Errorf("text data exceeds maximum size of %d", MaxTextSize)
	}

	length := uint32(len(r.Data))
	err = binary.Write(buf, binary.LittleEndian, length)
	if err != nil {
		return nil, err
	}

	// Write Data payload with padding zeros up to MaxTextSize
	textBytes := make([]byte, MaxTextSize)
	copy(textBytes, r.Data)
	
	err = binary.Write(buf, binary.LittleEndian, textBytes)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Deserialize reconstructs a Record from bytes
func Deserialize(data []byte) (*Record, error) {
	reader := bytes.NewReader(data)
	var rec Record

	err := binary.Read(reader, binary.LittleEndian, &rec.ID)
	if err != nil {
		return nil, err
	}

	var length uint32
	err = binary.Read(reader, binary.LittleEndian, &length)
	if err != nil {
		return nil, err
	}
	
	if length > MaxTextSize {
		return nil, fmt.Errorf("length prefix %d exceeds maximum text size %d", length, MaxTextSize)
	}

	textBytes := make([]byte, MaxTextSize)
	err = binary.Read(reader, binary.LittleEndian, &textBytes)
	if err != nil {
		return nil, err
	}

	rec.Data = string(textBytes[:length])
	return &rec, nil
}

// RecordSize returns the fixed size for a serialized record
func RecordSize() uint32 {
	// ID (4 bytes) + Length Prefix (4 bytes) + Text Data (255 bytes)
	return 4 + 4 + MaxTextSize
}
