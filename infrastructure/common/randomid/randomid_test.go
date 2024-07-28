package randomid

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNotTheSame(t *testing.T) {
	a := Generate(16)
	b := Generate(16)
	fmt.Println(a)
	fmt.Println(b)

	assert.NotEqual(t, a, b)
}
