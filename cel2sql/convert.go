package cel2sql

import (
	"fmt"

	"github.com/google/cel-go/cel"
)

// Convert takes CEL expressions and attempt to convert them into Postgres SQL
// filters.
func Convert(env *cel.Env, filters string) (string, error) {
	ast, issues := env.Compile(filters)
	if issues != nil && issues.Err() != nil {
		return "", fmt.Errorf("error compiling CEL filters: %w", issues.Err())
	}

	interpreter, err := newInterpreter(ast)
	if err != nil {
		return "", fmt.Errorf("error creating cel2sql interpreter: %w", err)
	}

	return interpreter.interpret()
}
