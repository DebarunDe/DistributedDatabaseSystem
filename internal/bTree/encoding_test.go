package btree

import (
	"strings"
	"testing"
)

// --- EncodeKey / DecodeKey ---

func TestEncodeKey_BigEndianOrdering(t *testing.T) {
	// Big-endian means smaller uint64 → lexicographically smaller bytes.
	a := EncodeKey(1)
	b := EncodeKey(2)
	for i := range a {
		if a[i] < b[i] {
			return
		}
		if a[i] > b[i] {
			t.Fatal("EncodeKey(1) is not lexicographically less than EncodeKey(2)")
		}
	}
	t.Fatal("EncodeKey(1) and EncodeKey(2) produced identical bytes")
}

func TestEncodeKey_Length(t *testing.T) {
	for _, k := range []uint64{0, 1, 255, 1 << 32, ^uint64(0)} {
		if got := len(EncodeKey(k)); got != 8 {
			t.Fatalf("EncodeKey(%d): length = %d, want 8", k, got)
		}
	}
}

func TestDecodeKey_RoundTrip(t *testing.T) {
	keys := []uint64{0, 1, 127, 128, 255, 256, 1 << 16, 1 << 32, ^uint64(0)}
	for _, k := range keys {
		got, err := DecodeKey(EncodeKey(k))
		if err != nil {
			t.Fatalf("DecodeKey(EncodeKey(%d)): unexpected error: %v", k, err)
		}
		if got != k {
			t.Fatalf("key %d: round-trip produced %d", k, got)
		}
	}
}

func TestDecodeKey_WrongLength(t *testing.T) {
	for _, bad := range [][]byte{{}, {0}, make([]byte, 7), make([]byte, 9)} {
		if _, err := DecodeKey(bad); err == nil {
			t.Fatalf("DecodeKey(%v): expected error for wrong length", bad)
		}
	}
}

// --- EncodeField: NullValue ---

func TestEncodeField_Null(t *testing.T) {
	f := Field{Tag: 1, Value: NullValue{}}
	b, err := EncodeField(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// expect exactly 2 bytes: tag + type
	if len(b) != 2 {
		t.Fatalf("null field: want 2 bytes, got %d", len(b))
	}
	if b[0] != 1 || b[1] != FieldTypeNull {
		t.Fatalf("null field: wrong header bytes %v", b)
	}
}

// --- EncodeField: IntValue ---

func TestEncodeField_Int_LittleEndian(t *testing.T) {
	f := Field{Tag: 5, Value: IntValue{V: 1}}
	b, err := EncodeField(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 2 header bytes + 8 int bytes
	if len(b) != 10 {
		t.Fatalf("int field: want 10 bytes, got %d", len(b))
	}
	if b[0] != 5 || b[1] != FieldTypeInt {
		t.Fatalf("int field: wrong header %v", b[:2])
	}
	// little-endian 1 → first byte = 1, rest = 0
	if b[2] != 1 {
		t.Fatalf("int field: byte[2] = %d, want 1 (little-endian)", b[2])
	}
	for _, v := range b[3:] {
		if v != 0 {
			t.Fatalf("int field: expected zero tail byte, got %d", v)
		}
	}
}

func TestEncodeField_Int_Negative(t *testing.T) {
	f := Field{Tag: 2, Value: IntValue{V: -1}}
	b, err := EncodeField(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(b) != 10 {
		t.Fatalf("negative int field: want 10 bytes, got %d", len(b))
	}
}

// --- EncodeField: StringValue ---

func TestEncodeField_String_Empty(t *testing.T) {
	f := Field{Tag: 3, Value: StringValue{V: ""}}
	b, err := EncodeField(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 2 header + 2 length + 0 data
	if len(b) != 4 {
		t.Fatalf("empty string field: want 4 bytes, got %d", len(b))
	}
	if b[2] != 0 || b[3] != 0 {
		t.Fatalf("empty string field: length bytes not zero: %v", b[2:4])
	}
}

func TestEncodeField_String_Content(t *testing.T) {
	f := Field{Tag: 7, Value: StringValue{V: "hello"}}
	b, err := EncodeField(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 2 header + 2 length + 5 data
	if len(b) != 9 {
		t.Fatalf("string field: want 9 bytes, got %d", len(b))
	}
	// little-endian length = 5
	if b[2] != 5 || b[3] != 0 {
		t.Fatalf("string field: wrong length prefix: %v", b[2:4])
	}
	if string(b[4:]) != "hello" {
		t.Fatalf("string field: wrong content: %s", b[4:])
	}
}

func TestEncodeField_String_TooLong(t *testing.T) {
	f := Field{Tag: 1, Value: StringValue{V: strings.Repeat("x", 65536)}}
	_, err := EncodeField(f)
	if err == nil {
		t.Fatal("expected error for string > 65535 bytes")
	}
}

// --- EncodeField: ListValue ---

func TestEncodeField_List_Empty(t *testing.T) {
	f := Field{Tag: 1, Value: ListValue{ElemType: FieldTypeInt, Elems: nil}}
	b, err := EncodeField(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 2 header + 2 count + 1 elemType = 5
	if len(b) != 5 {
		t.Fatalf("empty list field: want 5 bytes, got %d", len(b))
	}
	// count = 0
	if b[2] != 0 || b[3] != 0 {
		t.Fatalf("empty list: wrong count bytes: %v", b[2:4])
	}
	if b[4] != FieldTypeInt {
		t.Fatalf("empty list: wrong elemType: %d", b[4])
	}
}

func TestEncodeField_List_Ints(t *testing.T) {
	elems := []Value{IntValue{V: 10}, IntValue{V: 20}}
	f := Field{Tag: 2, Value: ListValue{ElemType: FieldTypeInt, Elems: elems}}
	b, err := EncodeField(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 2 header + 2 count + 1 elemType + 2*8 = 21
	if len(b) != 21 {
		t.Fatalf("int list field: want 21 bytes, got %d", len(b))
	}
	// little-endian count = 2
	if b[2] != 2 || b[3] != 0 {
		t.Fatalf("int list: wrong count bytes")
	}
}

func TestEncodeField_List_TypeMismatch(t *testing.T) {
	elems := []Value{IntValue{V: 1}, StringValue{V: "bad"}}
	f := Field{Tag: 1, Value: ListValue{ElemType: FieldTypeInt, Elems: elems}}
	_, err := EncodeField(f)
	if err == nil {
		t.Fatal("expected error for type-mismatched list elements")
	}
}

func TestEncodeField_List_TooLong(t *testing.T) {
	elems := make([]Value, 65536)
	for i := range elems {
		elems[i] = IntValue{V: int64(i)}
	}
	f := Field{Tag: 1, Value: ListValue{ElemType: FieldTypeInt, Elems: elems}}
	_, err := EncodeField(f)
	if err == nil {
		t.Fatal("expected error for list with > 65535 elements")
	}
}

func TestEncodeField_List_Strings(t *testing.T) {
	elems := []Value{StringValue{V: "a"}, StringValue{V: "bb"}}
	f := Field{Tag: 3, Value: ListValue{ElemType: FieldTypeString, Elems: elems}}
	_, err := EncodeField(f)
	if err != nil {
		t.Fatalf("unexpected error encoding string list: %v", err)
	}
}

func TestEncodeField_List_Nulls(t *testing.T) {
	elems := []Value{NullValue{}, NullValue{}}
	f := Field{Tag: 1, Value: ListValue{ElemType: FieldTypeNull, Elems: elems}}
	b, err := EncodeField(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 2 header + 2 count + 1 elemType + 2*0 = 5
	if len(b) != 5 {
		t.Fatalf("null list field: want 5 bytes, got %d", len(b))
	}
}

// --- DecodeFields round-trips ---

func mustEncode(t *testing.T, fields []Field) []byte {
	t.Helper()
	var out []byte
	for _, f := range fields {
		b, err := EncodeField(f)
		if err != nil {
			t.Fatalf("EncodeField: %v", err)
		}
		out = append(out, b...)
	}
	return out
}

func TestDecodeFields_SingleNull(t *testing.T) {
	f := Field{Tag: 1, Value: NullValue{}}
	fields, _ := DecodeFields(mustEncode(t, []Field{f}))
	if len(fields) != 1 {
		t.Fatalf("want 1 field, got %d", len(fields))
	}
	if _, ok := fields[0].Value.(NullValue); !ok {
		t.Fatalf("expected NullValue, got %T", fields[0].Value)
	}
	if fields[0].Tag != 1 {
		t.Fatalf("tag: want 1, got %d", fields[0].Tag)
	}
}

func TestDecodeFields_SingleInt(t *testing.T) {
	f := Field{Tag: 42, Value: IntValue{V: -999}}
	fields, _ := DecodeFields(mustEncode(t, []Field{f}))
	if len(fields) != 1 {
		t.Fatalf("want 1 field, got %d", len(fields))
	}
	iv, ok := fields[0].Value.(IntValue)
	if !ok {
		t.Fatalf("expected IntValue, got %T", fields[0].Value)
	}
	if iv.V != -999 {
		t.Fatalf("int value: want -999, got %d", iv.V)
	}
	if fields[0].Tag != 42 {
		t.Fatalf("tag: want 42, got %d", fields[0].Tag)
	}
}

func TestDecodeFields_SingleString(t *testing.T) {
	f := Field{Tag: 7, Value: StringValue{V: "world"}}
	fields, _ := DecodeFields(mustEncode(t, []Field{f}))
	if len(fields) != 1 {
		t.Fatalf("want 1 field, got %d", len(fields))
	}
	sv, ok := fields[0].Value.(StringValue)
	if !ok {
		t.Fatalf("expected StringValue, got %T", fields[0].Value)
	}
	if sv.V != "world" {
		t.Fatalf("string value: want 'world', got %q", sv.V)
	}
}

func TestDecodeFields_SingleList(t *testing.T) {
	elems := []Value{IntValue{V: 1}, IntValue{V: 2}, IntValue{V: 3}}
	f := Field{Tag: 9, Value: ListValue{ElemType: FieldTypeInt, Elems: elems}}
	fields, _ := DecodeFields(mustEncode(t, []Field{f}))
	if len(fields) != 1 {
		t.Fatalf("want 1 field, got %d", len(fields))
	}
	lv, ok := fields[0].Value.(ListValue)
	if !ok {
		t.Fatalf("expected ListValue, got %T", fields[0].Value)
	}
	if len(lv.Elems) != 3 {
		t.Fatalf("list elems: want 3, got %d", len(lv.Elems))
	}
	for i, e := range lv.Elems {
		iv, ok := e.(IntValue)
		if !ok {
			t.Fatalf("elem %d: expected IntValue, got %T", i, e)
		}
		if iv.V != int64(i+1) {
			t.Fatalf("elem %d: want %d, got %d", i, i+1, iv.V)
		}
	}
}

func TestDecodeFields_MultipleFields(t *testing.T) {
	input := []Field{
		{Tag: 1, Value: IntValue{V: 100}},
		{Tag: 2, Value: StringValue{V: "foo"}},
		{Tag: 3, Value: NullValue{}},
	}
	fields, _ := DecodeFields(mustEncode(t, input))
	if len(fields) != 3 {
		t.Fatalf("want 3 fields, got %d", len(fields))
	}
	if fields[0].Tag != 1 {
		t.Errorf("field 0 tag: want 1, got %d", fields[0].Tag)
	}
	if fields[1].Tag != 2 {
		t.Errorf("field 1 tag: want 2, got %d", fields[1].Tag)
	}
	if fields[2].Tag != 3 {
		t.Errorf("field 2 tag: want 3, got %d", fields[2].Tag)
	}
	if _, ok := fields[2].Value.(NullValue); !ok {
		t.Errorf("field 2: expected NullValue, got %T", fields[2].Value)
	}
}

func TestDecodeFields_EmptyInput(t *testing.T) {
	fields, _ := DecodeFields([]byte{})
	if len(fields) != 0 {
		t.Fatalf("empty input: want 0 fields, got %d", len(fields))
	}
}

func TestDecodeFields_TruncatedInput(t *testing.T) {
	f := Field{Tag: 1, Value: IntValue{V: 42}}
	b, _ := EncodeField(f)
	truncated := b[:5] // cut in the middle of the int bytes
	fields, consumed := DecodeFields(truncated)
	if len(fields) != 0 {
		t.Fatalf("truncated: want 0 fields, got %d", len(fields))
	}
	if consumed != 0 {
		t.Fatalf("truncated: want 0 bytes consumed, got %d", consumed)
	}
	if consumed == len(truncated) {
		t.Fatal("truncated: consumed == len(input), corruption would be silently ignored")
	}
}

func TestDecodeFields_UnknownFieldType(t *testing.T) {
	b := []byte{0x01, 0xFF}
	fields, consumed := DecodeFields(b)
	if len(fields) != 0 {
		t.Fatalf("unknown type: want 0 fields, got %d", len(fields))
	}
	if consumed != 0 {
		t.Fatalf("unknown type: want 0 bytes consumed, got %d", consumed)
	}
	if consumed == len(b) {
		t.Fatal("unknown type: consumed == len(input), corruption would be silently ignored")
	}
}

// --- Full record round-trip (key + fields) ---

func TestRecord_RoundTrip(t *testing.T) {
	key := uint64(12345)
	inputFields := []Field{
		{Tag: 1, Value: IntValue{V: 42}},
		{Tag: 2, Value: StringValue{V: "test"}},
		{Tag: 3, Value: NullValue{}},
		{Tag: 4, Value: ListValue{
			ElemType: FieldTypeString,
			Elems:    []Value{StringValue{V: "x"}, StringValue{V: "y"}},
		}},
	}

	keyBytes := EncodeKey(key)
	fieldsBytes := mustEncode(t, inputFields)
	record := append(keyBytes, fieldsBytes...)

	gotKey, err := DecodeKey(record[:ValueOffset])
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	if gotKey != key {
		t.Fatalf("key: want %d, got %d", key, gotKey)
	}

	gotFields, _ := DecodeFields(record[ValueOffset:])
	if len(gotFields) != len(inputFields) {
		t.Fatalf("fields: want %d, got %d", len(inputFields), len(gotFields))
	}
}

// --- ListValue.validate ---

func TestListValue_Validate_OK(t *testing.T) {
	l := ListValue{
		ElemType: FieldTypeInt,
		Elems:    []Value{IntValue{V: 1}, IntValue{V: 2}},
	}
	if err := l.validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListValue_Validate_Mismatch(t *testing.T) {
	l := ListValue{
		ElemType: FieldTypeInt,
		Elems:    []Value{IntValue{V: 1}, StringValue{V: "oops"}},
	}
	if err := l.validate(); err == nil {
		t.Fatal("expected error for type mismatch")
	}
}

func TestListValue_Validate_Empty(t *testing.T) {
	l := ListValue{ElemType: FieldTypeString, Elems: nil}
	if err := l.validate(); err != nil {
		t.Fatalf("empty list validation: unexpected error: %v", err)
	}
}

// --- Edge cases ---

func TestEncodeField_AllIntValues(t *testing.T) {
	cases := []int64{0, 1, -1, 1<<63 - 1, -1 << 63}
	for _, v := range cases {
		f := Field{Tag: 1, Value: IntValue{V: v}}
		b, err := EncodeField(f)
		if err != nil {
			t.Fatalf("EncodeField(%d): %v", v, err)
		}
		fields, _ := DecodeFields(b)
		if len(fields) != 1 {
			t.Fatalf("value %d: decoded %d fields, want 1", v, len(fields))
		}
		if got := fields[0].Value.(IntValue).V; got != v {
			t.Fatalf("value %d: round-trip produced %d", v, got)
		}
	}
}

func TestEncodeField_StringWithUnicode(t *testing.T) {
	s := "こんにちは世界" // multi-byte UTF-8
	f := Field{Tag: 1, Value: StringValue{V: s}}
	b, err := EncodeField(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fields, _ := DecodeFields(b)
	if len(fields) != 1 {
		t.Fatalf("unicode: decoded %d fields, want 1", len(fields))
	}
	if got := fields[0].Value.(StringValue).V; got != s {
		t.Fatalf("unicode: want %q, got %q", s, got)
	}
}

func TestDecodeFields_ListWithStringElems_RoundTrip(t *testing.T) {
	elems := []Value{StringValue{V: "alpha"}, StringValue{V: "beta"}, StringValue{V: ""}}
	f := Field{Tag: 5, Value: ListValue{ElemType: FieldTypeString, Elems: elems}}
	fields, _ := DecodeFields(mustEncode(t, []Field{f}))
	if len(fields) != 1 {
		t.Fatalf("want 1 field, got %d", len(fields))
	}
	lv := fields[0].Value.(ListValue)
	if len(lv.Elems) != 3 {
		t.Fatalf("want 3 elems, got %d", len(lv.Elems))
	}
	want := []string{"alpha", "beta", ""}
	for i, e := range lv.Elems {
		sv := e.(StringValue)
		if sv.V != want[i] {
			t.Fatalf("elem %d: want %q, got %q", i, want[i], sv.V)
		}
	}
}

func TestDecodeFields_TagPreservation(t *testing.T) {
	tags := []uint8{0, 1, 127, 255}
	for _, tag := range tags {
		f := Field{Tag: tag, Value: NullValue{}}
		fields, _ := DecodeFields(mustEncode(t, []Field{f}))
		if len(fields) != 1 {
			t.Fatalf("tag %d: decoded %d fields", tag, len(fields))
		}
		if fields[0].Tag != tag {
			t.Fatalf("tag %d: round-trip gave tag %d", tag, fields[0].Tag)
		}
	}
}

// --- Real-life record scenarios ---

// A "users" table row: id (int), name (string), email (string),
// age (int), deleted_at (null), tags (list<string>).
func TestScenario_UserRecord(t *testing.T) {
	const (
		tagID        = 1
		tagName      = 2
		tagEmail     = 3
		tagAge       = 4
		tagDeletedAt = 5
		tagTags      = 6
	)

	userID := uint64(8472)
	inputFields := []Field{
		{Tag: tagID, Value: IntValue{V: int64(userID)}},
		{Tag: tagName, Value: StringValue{V: "Alice Nguyen"}},
		{Tag: tagEmail, Value: StringValue{V: "alice@example.com"}},
		{Tag: tagAge, Value: IntValue{V: 31}},
		{Tag: tagDeletedAt, Value: NullValue{}},
		{Tag: tagTags, Value: ListValue{
			ElemType: FieldTypeString,
			Elems:    []Value{StringValue{V: "admin"}, StringValue{V: "beta-tester"}},
		}},
	}

	record := append(EncodeKey(userID), mustEncode(t, inputFields)...)

	gotKey, err := DecodeKey(record[:ValueOffset])
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	if gotKey != userID {
		t.Fatalf("key: want %d, got %d", userID, gotKey)
	}

	fields, _ := DecodeFields(record[ValueOffset:])
	if len(fields) != 6 {
		t.Fatalf("want 6 fields, got %d", len(fields))
	}

	// id
	if v := fields[0].Value.(IntValue).V; v != int64(userID) {
		t.Errorf("id: want %d, got %d", userID, v)
	}
	// name
	if v := fields[1].Value.(StringValue).V; v != "Alice Nguyen" {
		t.Errorf("name: want 'Alice Nguyen', got %q", v)
	}
	// email
	if v := fields[2].Value.(StringValue).V; v != "alice@example.com" {
		t.Errorf("email: want 'alice@example.com', got %q", v)
	}
	// age
	if v := fields[3].Value.(IntValue).V; v != 31 {
		t.Errorf("age: want 31, got %d", v)
	}
	// deleted_at is null
	if _, ok := fields[4].Value.(NullValue); !ok {
		t.Errorf("deleted_at: expected NullValue, got %T", fields[4].Value)
	}
	// tags list
	lv := fields[5].Value.(ListValue)
	if lv.ElemType != FieldTypeString {
		t.Errorf("tags: wrong elemType %d", lv.ElemType)
	}
	wantTags := []string{"admin", "beta-tester"}
	if len(lv.Elems) != len(wantTags) {
		t.Fatalf("tags: want %d elems, got %d", len(wantTags), len(lv.Elems))
	}
	for i, want := range wantTags {
		if got := lv.Elems[i].(StringValue).V; got != want {
			t.Errorf("tag[%d]: want %q, got %q", i, want, got)
		}
	}
}

// A "products" table row: sku (string), price_cents (int),
// stock (int), category_ids (list<int>), description (string), discontinued (null).
func TestScenario_ProductRecord(t *testing.T) {
	const (
		tagSKU          = 1
		tagPriceCents   = 2
		tagStock        = 3
		tagCategoryIDs  = 4
		tagDescription  = 5
		tagDiscontinued = 6
	)

	productKey := uint64(99001)
	inputFields := []Field{
		{Tag: tagSKU, Value: StringValue{V: "WIDGET-42-BLU"}},
		{Tag: tagPriceCents, Value: IntValue{V: 1999}},
		{Tag: tagStock, Value: IntValue{V: 500}},
		{Tag: tagCategoryIDs, Value: ListValue{
			ElemType: FieldTypeInt,
			Elems:    []Value{IntValue{V: 7}, IntValue{V: 13}, IntValue{V: 42}},
		}},
		{Tag: tagDescription, Value: StringValue{V: "A sturdy blue widget with ergonomic grip."}},
		{Tag: tagDiscontinued, Value: NullValue{}},
	}

	record := append(EncodeKey(productKey), mustEncode(t, inputFields)...)

	gotKey, _ := DecodeKey(record[:ValueOffset])
	if gotKey != productKey {
		t.Fatalf("key: want %d, got %d", productKey, gotKey)
	}

	fields, _ := DecodeFields(record[ValueOffset:])
	if len(fields) != 6 {
		t.Fatalf("want 6 fields, got %d", len(fields))
	}

	if v := fields[0].Value.(StringValue).V; v != "WIDGET-42-BLU" {
		t.Errorf("sku: got %q", v)
	}
	if v := fields[1].Value.(IntValue).V; v != 1999 {
		t.Errorf("price_cents: got %d", v)
	}
	if v := fields[2].Value.(IntValue).V; v != 500 {
		t.Errorf("stock: got %d", v)
	}

	catIDs := fields[3].Value.(ListValue)
	wantIDs := []int64{7, 13, 42}
	if len(catIDs.Elems) != 3 {
		t.Fatalf("category_ids: want 3, got %d", len(catIDs.Elems))
	}
	for i, want := range wantIDs {
		if got := catIDs.Elems[i].(IntValue).V; got != want {
			t.Errorf("category_ids[%d]: want %d, got %d", i, want, got)
		}
	}

	if v := fields[4].Value.(StringValue).V; v != "A sturdy blue widget with ergonomic grip." {
		t.Errorf("description: got %q", v)
	}
	if _, ok := fields[5].Value.(NullValue); !ok {
		t.Errorf("discontinued: expected NullValue, got %T", fields[5].Value)
	}
}

// An "events" table row: event_id (int), payload (string with JSON-like content),
// scores (list<int>), label (null = unclassified), source (string).
func TestScenario_EventRecord(t *testing.T) {
	const (
		tagEventID = 1
		tagPayload = 2
		tagScores  = 3
		tagLabel   = 4
		tagSource  = 5
	)

	eventKey := uint64(1<<40 + 7) // large key to stress big-endian encoding
	scores := make([]Value, 10)
	for i := range scores {
		scores[i] = IntValue{V: int64(i * 100)}
	}

	inputFields := []Field{
		{Tag: tagEventID, Value: IntValue{V: int64(eventKey)}},
		{Tag: tagPayload, Value: StringValue{V: `{"action":"click","target":"button#submit"}`}},
		{Tag: tagScores, Value: ListValue{ElemType: FieldTypeInt, Elems: scores}},
		{Tag: tagLabel, Value: NullValue{}},
		{Tag: tagSource, Value: StringValue{V: "web-frontend"}},
	}

	record := append(EncodeKey(eventKey), mustEncode(t, inputFields)...)

	gotKey, err := DecodeKey(record[:ValueOffset])
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	if gotKey != eventKey {
		t.Fatalf("key: want %d, got %d", eventKey, gotKey)
	}

	fields, _ := DecodeFields(record[ValueOffset:])
	if len(fields) != 5 {
		t.Fatalf("want 5 fields, got %d", len(fields))
	}

	scoreList := fields[2].Value.(ListValue)
	if len(scoreList.Elems) != 10 {
		t.Fatalf("scores: want 10 elems, got %d", len(scoreList.Elems))
	}
	for i, e := range scoreList.Elems {
		if v := e.(IntValue).V; v != int64(i*100) {
			t.Errorf("score[%d]: want %d, got %d", i, i*100, v)
		}
	}

	if _, ok := fields[3].Value.(NullValue); !ok {
		t.Errorf("label: expected NullValue, got %T", fields[3].Value)
	}
}

// --- EncodeLeafRecord / DecodeLeafRecord helpers ---

func mustEncodeLeafRecord(t *testing.T, key uint64, fields []Field) []byte {
	t.Helper()
	b, err := EncodeLeafRecord(key, fields)
	if err != nil {
		t.Fatalf("EncodeLeafRecord: %v", err)
	}
	return b
}

func mustDecodeLeafRecord(t *testing.T, b []byte) (uint64, []Field) {
	t.Helper()
	key, fields, err := DecodeLeafRecord(b)
	if err != nil {
		t.Fatalf("DecodeLeafRecord: %v", err)
	}
	return key, fields
}

// --- EncodeInternalRecord / DecodeInternalRecord helpers ---

func mustEncodeInternalRecord(key uint64, childPageId uint32) []byte {
	return EncodeInternalRecord(key, childPageId)
}

func mustDecodeInternalRecord(t *testing.T, b []byte) (uint64, uint32) {
	t.Helper()
	key, child, err := DecodeInternalRecord(b)
	if err != nil {
		t.Fatalf("DecodeInternalRecord: %v", err)
	}
	return key, child
}

// --- EncodeLeafRecord / DecodeLeafRecord tests ---

func TestEncodeLeafRecord_Length(t *testing.T) {
	fields := []Field{
		{Tag: 1, Value: IntValue{V: 42}},
		{Tag: 2, Value: StringValue{V: "hi"}},
	}
	b := mustEncodeLeafRecord(t, 1, fields)
	// 8 key + 10 int field + (2+2+2) string field = 8+10+6 = 24
	if len(b) < 8 {
		t.Fatalf("leaf record too short: %d bytes", len(b))
	}
}

func TestEncodeLeafRecord_KeyIsFirst8Bytes(t *testing.T) {
	key := uint64(0xDEADBEEFCAFEBABE)
	b := mustEncodeLeafRecord(t, key, nil)
	got, err := DecodeKey(b[:8])
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	if got != key {
		t.Fatalf("key in record: want %x, got %x", key, got)
	}
}

func TestEncodeLeafRecord_NoFields(t *testing.T) {
	b := mustEncodeLeafRecord(t, 0, nil)
	if len(b) != 8 {
		t.Fatalf("no-field record: want 8 bytes, got %d", len(b))
	}
}

func TestDecodeLeafRecord_RoundTrip(t *testing.T) {
	key := uint64(54321)
	fields := []Field{
		{Tag: 1, Value: IntValue{V: -7}},
		{Tag: 2, Value: StringValue{V: "leaf"}},
		{Tag: 3, Value: NullValue{}},
	}
	b := mustEncodeLeafRecord(t, key, fields)
	gotKey, gotFields := mustDecodeLeafRecord(t, b)

	if gotKey != key {
		t.Fatalf("key: want %d, got %d", key, gotKey)
	}
	if len(gotFields) != len(fields) {
		t.Fatalf("fields: want %d, got %d", len(fields), len(gotFields))
	}
	if gotFields[0].Value.(IntValue).V != -7 {
		t.Errorf("field 0 int: want -7, got %d", gotFields[0].Value.(IntValue).V)
	}
	if gotFields[1].Value.(StringValue).V != "leaf" {
		t.Errorf("field 1 string: want 'leaf', got %q", gotFields[1].Value.(StringValue).V)
	}
	if _, ok := gotFields[2].Value.(NullValue); !ok {
		t.Errorf("field 2: expected NullValue, got %T", gotFields[2].Value)
	}
}

func TestDecodeLeafRecord_TooShort(t *testing.T) {
	_, _, err := DecodeLeafRecord([]byte{0x00, 0x01})
	if err == nil {
		t.Fatal("expected error for record shorter than 8 bytes")
	}
}

func TestDecodeLeafRecord_ExactlyKeyBytes(t *testing.T) {
	b := mustEncodeLeafRecord(t, 999, nil)
	gotKey, gotFields := mustDecodeLeafRecord(t, b)
	if gotKey != 999 {
		t.Fatalf("key: want 999, got %d", gotKey)
	}
	if len(gotFields) != 0 {
		t.Fatalf("fields: want 0, got %d", len(gotFields))
	}
}

func TestEncodeLeafRecord_FieldError_Propagates(t *testing.T) {
	fields := []Field{
		{Tag: 1, Value: StringValue{V: strings.Repeat("x", 65536)}},
	}
	_, err := EncodeLeafRecord(1, fields)
	if err == nil {
		t.Fatal("expected error for oversized string field")
	}
}

// --- EncodeInternalRecord / DecodeInternalRecord tests ---

func TestEncodeInternalRecord_Length(t *testing.T) {
	b := mustEncodeInternalRecord(1, 42)
	if len(b) != 12 {
		t.Fatalf("internal record: want 12 bytes, got %d", len(b))
	}
}

func TestEncodeInternalRecord_KeyIsFirst8Bytes(t *testing.T) {
	key := uint64(0x0102030405060708)
	b := mustEncodeInternalRecord(key, 0)
	got, err := DecodeKey(b[:8])
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	if got != key {
		t.Fatalf("key: want %x, got %x", key, got)
	}
}

func TestDecodeInternalRecord_RoundTrip(t *testing.T) {
	cases := []struct {
		key   uint64
		child uint32
	}{
		{0, 0},
		{1, 1},
		{^uint64(0), ^uint32(0)},
		{1 << 40, 99999},
	}
	for _, c := range cases {
		b := mustEncodeInternalRecord(c.key, c.child)
		gotKey, gotChild := mustDecodeInternalRecord(t, b)
		if gotKey != c.key {
			t.Errorf("key %d: round-trip gave %d", c.key, gotKey)
		}
		if gotChild != c.child {
			t.Errorf("child %d: round-trip gave %d", c.child, gotChild)
		}
	}
}

func TestDecodeInternalRecord_WrongLength(t *testing.T) {
	for _, bad := range [][]byte{
		{},
		make([]byte, 8),
		make([]byte, 11),
		make([]byte, 13),
	} {
		_, _, err := DecodeInternalRecord(bad)
		if err == nil {
			t.Fatalf("DecodeInternalRecord(%d bytes): expected error", len(bad))
		}
	}
}

func TestDecodeInternalRecord_ChildPageIdLittleEndian(t *testing.T) {
	b := mustEncodeInternalRecord(0, 1)
	// child is little-endian: byte[8]=1, rest=0
	if b[8] != 1 {
		t.Fatalf("child LE: byte[8] = %d, want 1", b[8])
	}
	for _, v := range b[9:] {
		if v != 0 {
			t.Fatalf("child LE: expected zero tail byte, got %d", v)
		}
	}
}

// A "sessions" table row with an empty tags list and empty string fields
// to verify zero-value handling across all types together.
func TestScenario_SparseRecord_ZeroValues(t *testing.T) {
	inputFields := []Field{
		{Tag: 1, Value: IntValue{V: 0}},
		{Tag: 2, Value: StringValue{V: ""}},
		{Tag: 3, Value: NullValue{}},
		{Tag: 4, Value: ListValue{ElemType: FieldTypeInt, Elems: nil}},
	}

	fields, _ := DecodeFields(mustEncode(t, inputFields))
	if len(fields) != 4 {
		t.Fatalf("want 4 fields, got %d", len(fields))
	}
	if v := fields[0].Value.(IntValue).V; v != 0 {
		t.Errorf("int zero: got %d", v)
	}
	if v := fields[1].Value.(StringValue).V; v != "" {
		t.Errorf("string empty: got %q", v)
	}
	if _, ok := fields[2].Value.(NullValue); !ok {
		t.Errorf("null: got %T", fields[2].Value)
	}
	lv := fields[3].Value.(ListValue)
	if len(lv.Elems) != 0 {
		t.Errorf("empty list: got %d elems", len(lv.Elems))
	}
}
