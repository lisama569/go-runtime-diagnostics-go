package trace

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// String renders an ID as lowercase hex. The zero ID renders as all zeroes.
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// IsZero reports whether the ID is the invalid zero value.
func (id ID) IsZero() bool { return id == ID{} }

// String renders a SpanID as lowercase hex.
func (s SpanID) String() string { return hex.EncodeToString(s[:]) }

// IsZero reports whether the SpanID is the zero value.
func (s SpanID) IsZero() bool { return s == SpanID{} }

// ParseID parses a hex execution ID.
func ParseID(s string) (ID, error) {
	var id ID
	b, err := hex.DecodeString(s)
	if err != nil {
		return ID{}, fmt.Errorf("trace: parse id %q: %w", s, err)
	}
	if len(b) != len(id) {
		return ID{}, fmt.Errorf("trace: parse id %q: got %d bytes, want %d", s, len(b), len(id))
	}
	copy(id[:], b)
	return id, nil
}

// ParseSpanID parses a hex span id.
func ParseSpanID(s string) (SpanID, error) {
	var id SpanID
	b, err := hex.DecodeString(s)
	if err != nil {
		return SpanID{}, fmt.Errorf("trace: parse span id %q: %w", s, err)
	}
	if len(b) != len(id) {
		return SpanID{}, fmt.Errorf("trace: parse span id %q: got %d bytes, want %d", s, len(b), len(id))
	}
	copy(id[:], b)
	return id, nil
}

func randomID() ID {
	var id ID
	if _, err := rand.Read(id[:]); err != nil {
		// crypto/rand failure is non-recoverable for correctness; surface it
		// deterministically rather than silently reusing a zero id.
		panic(err)
	}
	return id
}

func randomSpanID() SpanID {
	var id SpanID
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return id
}
