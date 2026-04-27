package btree

import (
	"encoding/binary"
	"fmt"
)

/*
RECORD FORMAT:
  [0-7]     Key (uint64, 8 bytes, fixed-length, big-endian for byte comparison)
  [8...]    Value (variable-length field sequence)

FIELD ENCODING (repeated, no end marker):
  [0]       Field tag (uint8, starts at 1)
  [1]       Type (uint8): 0x00=null, 0x01=int, 0x02=string, 0x03=list

  null (0x00):    no additional bytes
  int (0x01):     8 bytes (int64, little-endian)
  string (0x02):  2-byte length (uint16) + string bytes
  list (0x03):    2-byte count (uint16) + 1-byte element type + encoded elements
*/

// record format offsets
const (
	KeyOffset   = 0
	ValueOffset = 8
)

// field type constants
const (
	FieldTypeNull   = 0x00
	FieldTypeInt    = 0x01
	FieldTypeString = 0x02
	FieldTypeList   = 0x03
)

// Value is the sealed interface for typed field values.
// Only the four concrete types below satisfy it.
type Value interface {
	fieldType() uint8
}

type NullValue struct{}
type IntValue struct{ V int64 }
type StringValue struct{ V string }
type ListValue struct {
	ElemType uint8
	Elems    []Value
}

func (NullValue) fieldType() uint8   { return FieldTypeNull }
func (IntValue) fieldType() uint8    { return FieldTypeInt }
func (StringValue) fieldType() uint8 { return FieldTypeString }
func (ListValue) fieldType() uint8   { return FieldTypeList }

// validate returns an error if any element's type doesn't match ElemType.
// Call this at the start of any encode path that handles ListValue.
func (l ListValue) validate() error {
	for i, e := range l.Elems {
		if e.fieldType() != l.ElemType {
			return fmt.Errorf("list element %d: expected type 0x%02x, got 0x%02x", i, l.ElemType, e.fieldType())
		}
	}
	return nil
}

// Field encoding struct — Type is derived from Value at encode time.
type Field struct {
	Tag   uint8
	Value Value
}

// encodeValuePayload encodes only the value bytes for v, with no tag or type prefix.
// Used by the list encoder so elements don't repeat metadata already in the list header.
func encodeValuePayload(v Value) ([]byte, error) {
	switch v := v.(type) {
	case NullValue:
		return []byte{}, nil
	case IntValue:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(v.V))
		return b, nil
	case StringValue:
		strBytes := []byte(v.V)
		if len(strBytes) > 65535 {
			return nil, fmt.Errorf("string too long: %d bytes, max is 65535", len(strBytes))
		}
		lenBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(lenBytes, uint16(len(strBytes)))
		return append(lenBytes, strBytes...), nil
	default:
		return nil, fmt.Errorf("unsupported element type: %T", v)
	}
}

// decodeValuePayload reads one value of elemType from b, returning the value, bytes consumed, and success.
func decodeValuePayload(b []byte, elemType uint8) (Value, int, bool) {
	switch elemType {
	case FieldTypeNull:
		return NullValue{}, 0, true
	case FieldTypeInt:
		if len(b) < 8 {
			return nil, 0, false
		}
		return IntValue{V: int64(binary.LittleEndian.Uint64(b[:8]))}, 8, true
	case FieldTypeString:
		if len(b) < 2 {
			return nil, 0, false
		}
		strLen := int(binary.LittleEndian.Uint16(b[:2]))
		if len(b) < 2+strLen {
			return nil, 0, false
		}
		return StringValue{V: string(b[2 : 2+strLen])}, 2 + strLen, true
	default:
		return nil, 0, false
	}
}

// Encoding functions
// EncodeKey encodes a uint64 key into an 8 byte big endian slice.
func EncodeKey(key uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, key)
	return b
}

// EncodeField encodes a single field
func EncodeField(f Field) ([]byte, error) {
	var valueBytes []byte
	switch v := f.Value.(type) {
	case NullValue:
		valueBytes = []byte{}
	case IntValue:
		valueBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBytes, uint64(v.V))
	case StringValue:
		strBytes := []byte(v.V)
		if len(strBytes) > 65535 {
			return nil, fmt.Errorf("string too long: %d bytes, max is 65535", len(strBytes))
		}
		lenBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(lenBytes, uint16(len(strBytes)))
		valueBytes = append(lenBytes, strBytes...)
	case ListValue:
		if err := v.validate(); err != nil {
			return nil, fmt.Errorf("error validating list: %w", err)
		}
		count := len(v.Elems)
		if count > 65535 {
			return nil, fmt.Errorf("list too long: %d elements, max is 65535", count)
		}
		countBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(countBytes, uint16(count))
		valueBytes = append(countBytes, v.ElemType)
		for _, e := range v.Elems {
			elemBytes, err := encodeValuePayload(e)
			if err != nil {
				return nil, fmt.Errorf("error encoding list element: %w", err)
			}
			valueBytes = append(valueBytes, elemBytes...)
		}
	default:
		return nil, fmt.Errorf("unsupported field value type: %T", f.Value)
	}

	return append([]byte{f.Tag, f.Value.fieldType()}, valueBytes...), nil
}

// EncodeLeafRecord encodes a key and fields into a leaf record byte slice.
func EncodeLeafRecord(key uint64, fields []Field) ([]byte, error) {
	keyBytes := EncodeKey(key)
	var fieldBytes []byte

	for _, f := range fields {
		fb, err := EncodeField(f)
		if err != nil {
			return nil, fmt.Errorf("error encoding field with tag %d: %w", f.Tag, err)
		}
		fieldBytes = append(fieldBytes, fb...)
	}

	return append(keyBytes, fieldBytes...), nil
}

// EncodeInternalRecord encodes a key and child pointer into an internal record byte slice.
func EncodeInternalRecord(key uint64, childPageId uint32) []byte {
	keyBytes := EncodeKey(key)
	childBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(childBytes, childPageId)
	return append(keyBytes, childBytes...)
}

// Decoding functions
// DecodeKey decodes an 8 byte big endian slice into a uint64 key.
func DecodeKey(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid key length: expected 8 bytes, got %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

// decodeOneField decodes a single field from b, returning the field, bytes consumed, and success.
func decodeOneField(b []byte) (Field, int, bool) {
	if len(b) < 2 {
		return Field{}, 0, false
	}
	tag := b[0]
	fieldType := b[1]
	i := 2

	var value Value
	switch fieldType {
	case FieldTypeNull:
		value = NullValue{}
	case FieldTypeInt:
		if i+8 > len(b) {
			return Field{}, 0, false
		}
		value = IntValue{V: int64(binary.LittleEndian.Uint64(b[i : i+8]))}
		i += 8
	case FieldTypeString:
		if i+2 > len(b) {
			return Field{}, 0, false
		}
		strLen := int(binary.LittleEndian.Uint16(b[i : i+2]))
		i += 2
		if i+strLen > len(b) {
			return Field{}, 0, false
		}
		value = StringValue{V: string(b[i : i+strLen])}
		i += strLen
	case FieldTypeList:
		if i+3 > len(b) {
			return Field{}, 0, false
		}
		count := int(binary.LittleEndian.Uint16(b[i : i+2]))
		elemType := b[i+2]
		i += 3

		var elems []Value
		for range count {
			v, n, ok := decodeValuePayload(b[i:], elemType)
			if !ok {
				return Field{}, 0, false
			}
			elems = append(elems, v)
			i += n
		}
		value = ListValue{ElemType: elemType, Elems: elems}
	default:
		return Field{}, 0, false
	}

	return Field{Tag: tag, Value: value}, i, true
}

// DecodeFields decodes all fields from b, returning the fields and the number of bytes consumed.
// If bytesConsumed < len(b), the remaining bytes could not be parsed (truncated or corrupt).
func DecodeFields(b []byte) ([]Field, int) {
	var fields []Field
	i := 0
	for i < len(b) {
		f, n, ok := decodeOneField(b[i:])
		if !ok {
			break
		}
		fields = append(fields, f)
		i += n
	}
	return fields, i
}

// DecodeLeafRecord decodes a leaf record byte slice into a key and fields.
func DecodeLeafRecord(b []byte) (uint64, []Field, error) {
	if len(b) < ValueOffset {
		return 0, nil, fmt.Errorf("record too short: expected at least %d bytes, got %d", ValueOffset, len(b))
	}

	key, err := DecodeKey(b[KeyOffset:ValueOffset])
	if err != nil {
		return 0, nil, fmt.Errorf("error decoding key: %w", err)
	}

	fields, consumed := DecodeFields(b[ValueOffset:])
	if consumed != len(b)-ValueOffset {
		return 0, nil, fmt.Errorf("leaf record has %d unparseable trailing bytes", len(b)-ValueOffset-consumed)
	}

	return key, fields, nil
}

// DecodeInternalRecord decodes an internal record byte slice into a key and child pageId.
func DecodeInternalRecord(b []byte) (uint64, uint32, error) {
	if len(b) != ValueOffset+4 {
		return 0, 0, fmt.Errorf("invalid internal record length: expected %d bytes, got %d", ValueOffset+4, len(b))
	}

	key, err := DecodeKey(b[KeyOffset:ValueOffset])
	if err != nil {
		return 0, 0, fmt.Errorf("error decoding key: %w", err)
	}

	childPageId := binary.LittleEndian.Uint32(b[ValueOffset : ValueOffset+4])

	return key, childPageId, nil
}
