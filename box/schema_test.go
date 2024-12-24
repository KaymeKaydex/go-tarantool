package box

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSchema(t *testing.T) {
	// Create a schema instance with a nil connection. This should lead to a panic later.
	b := NewSchema(nil)

	// Ensure the schema is not nil (which it shouldn't be), but this is not meaningful
	// since we will panic when we call the schema methods with the nil connection.
	require.NotNil(t, b)
}
