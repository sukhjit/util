package ptr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPtr(t *testing.T) {
	// test string pointer
	vs := "test"
	assert.Equal(t, &vs, Ptr("test"))

	// test int pointer
	vi := 10
	assert.Equal(t, &vi, Ptr(10))
}
