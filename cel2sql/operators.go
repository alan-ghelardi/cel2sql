package cel2sql

import (
	"github.com/google/cel-go/common/operators"
)

var (
	unaryOperators = map[string]string{
		operators.Negate: "NOT",
	}

	binaryOperators = map[string]string{
		operators.LogicalAnd:    "AND",
		operators.LogicalOr:     "OR",
		operators.LogicalNot:    "NOT",
		operators.Equals:        "=",
		operators.NotEquals:     "<>",
		operators.Less:          "<",
		operators.LessEquals:    "<=",
		operators.Greater:       ">",
		operators.GreaterEquals: ">=",
		operators.Add:           "+",
		operators.Subtract:      "-",
		operators.Multiply:      "*",
		operators.Divide:        "/",
		operators.Modulo:        "%",
		operators.In:            "IN",
	}
)

// isUnaryOperator returns true if the symbol in question is a CEL unary
// operator.
func isUnaryOperator(symbol string) bool {
	_, found := unaryOperators[symbol]
	return found
}

// isBinaryOperator returns true if the symbol in question is a CEL binary
// operator.
func isBinaryOperator(symbol string) bool {
	_, found := binaryOperators[symbol]
	return found
}
