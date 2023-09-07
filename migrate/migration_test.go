package migrate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStepPositionToLineCol(t *testing.T) {
	t.Run("base", func(t *testing.T) {
		step := Step{}
		line, col := step.PositionToLineCol(42)
		assert.Equal(t, 0, line)
		assert.Equal(t, 0, col)
	})

	t.Run("one", func(t *testing.T) {
		step := Step{
			SQL:        "select 1;",
			LineNumber: 1,
		}
		line, col := step.PositionToLineCol(1)
		assert.Equal(t, 1, line)
		assert.Equal(t, 1, col)
	})

	t.Run("offset", func(t *testing.T) {
		step := Step{
			SQL:        "select 1;\nselect 2;",
			LineNumber: 3,
		}
		line, col := step.PositionToLineCol(18)
		assert.Equal(t, 4, line)
		assert.Equal(t, 8, col)
	})

	t.Run("unicode", func(t *testing.T) {
		step := Step{
			SQL:        "select '⌘' from foo;",
			LineNumber: 1,
		}
		line, col := step.PositionToLineCol(17)
		assert.Equal(t, 1, line)
		assert.Equal(t, 17, col)
	})
}
