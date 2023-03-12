package cel2sql

import (
	"fmt"

	"github.com/google/cel-go/common/operators"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func (i *Interpreter) mayBeTranslatedIntoJSONPathContainsExpression(arg1 *exprpb.Expr, function string, arg2 *exprpb.Expr) bool {
	constExpr := arg2.GetConstExpr()
	if constExpr == nil {
		return false
	}
	if _, ok := constExpr.GetConstantKind().(*exprpb.Constant_StringValue); !ok {
		return false
	}
	return isIndexExpr(arg1) &&
		function == operators.Equals &&
		!i.isDyn(arg1.GetCallExpr().Args[0])
}

func isIndexExpr(expr *exprpb.Expr) bool {
	if callExpr := expr.GetCallExpr(); callExpr != nil && isIndexOperator(callExpr.GetFunction()) {
		return true
	}
	return false
}

func isIndexOperator(symbol string) bool {
	return symbol == operators.Index
}

func (i *Interpreter) translateIntoJSONPathContainsExpression(arg1 *exprpb.Expr, arg2 *exprpb.Expr) error {
	callExprArgs := arg1.GetCallExpr().GetArgs()
	key := callExprArgs[len(callExprArgs)-1]
	for _, expr := range callExprArgs[0 : len(callExprArgs)-1] {
		if err := i.InterpretExpr(expr); err != nil {
			return err
		}
	}

	fmt.Fprintf(&i.query, ` @> '{"%s":"%s"}'::jsonb`,
		key.GetConstExpr().GetStringValue(),
		arg2.GetConstExpr().GetStringValue())

	return nil
}
