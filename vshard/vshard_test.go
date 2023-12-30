package vshard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouterBucketIDStrCRC32(t *testing.T) {
	require.Equal(t, 103202, RouterBucketIDStrCRC32("2707623829", 128000))
}
