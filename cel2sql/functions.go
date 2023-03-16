package cel2sql

import (
	"fmt"

	"github.com/google/cel-go/common/overloads"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func (i *interpreter) interpretFunctionCallExpr(id int64, expr *exprpb.Expr_CallExpr) error {
	function := expr.CallExpr.GetFunction()
	switch function {
	case overloads.Contains:
		return i.interpretContainsFunction(expr)

	case overloads.EndsWith:
		return i.translateIntoBinaryCall(expr, "LIKE '%' ||")

	case overloads.TimeGetDate:
		return i.translateIntoExtractFunctionCall(expr, "DAY", false)

	case overloads.TimeGetDayOfMonth:
		return i.translateIntoExtractFunctionCall(expr, "DAY", true)

	case overloads.TimeGetDayOfWeek:
		return i.translateIntoExtractFunctionCall(expr, "DOW", false)

	case overloads.TimeGetDayOfYear:
		return i.translateIntoExtractFunctionCall(expr, "DOY", true)

	case overloads.TimeGetFullYear:
		return i.translateIntoExtractFunctionCall(expr, "YEAR", false)

	case overloads.StartsWith:
		return i.interpretStartsWithFunction(expr)

	case overloads.Matches:
		return i.translateIntoBinaryCall(expr, "~")

	case overloads.TypeConvertTimestamp:
		return i.interpretTimestampFunction(expr)

	}

	return i.unsupportedExprError(id, fmt.Sprintf("`%s` function", function))
}

func (i *interpreter) interpretContainsFunction(expr *exprpb.Expr_CallExpr) error {
	fmt.Fprintf(&i.query, "POSITION(")
	if err := i.interpretExpr(expr.CallExpr.Args[0]); err != nil {
		return err
	}
	fmt.Fprintf(&i.query, " IN ")
	if err := i.interpretExpr(expr.CallExpr.GetTarget()); err != nil {
		return err
	}
	i.query.WriteString(") <> 0")
	return nil
}

func (i *interpreter) interpretStartsWithFunction(expr *exprpb.Expr_CallExpr) error {
	if err := i.translateIntoBinaryCall(expr, "LIKE"); err != nil {
		return err
	}
	i.query.WriteString(" || '%'")
	return nil
}

func (i *interpreter) translateIntoBinaryCall(expr *exprpb.Expr_CallExpr, infixTerm string) error {
	if err := i.interpretExpr(expr.CallExpr.GetTarget()); err != nil {
		return err
	}
	fmt.Fprintf(&i.query, " %s ", infixTerm)
	if err := i.interpretExpr(expr.CallExpr.Args[0]); err != nil {
		return err
	}

	return nil
}

func (i *interpreter) translateIntoExtractFunctionCall(expr *exprpb.Expr_CallExpr, field string, decrementReturnValue bool) error {
	if decrementReturnValue {
		i.query.WriteString("(")
	}
	fmt.Fprintf(&i.query, "EXTRACT(%s FROM ", field)
	if err := i.interpretExpr(expr.CallExpr.GetTarget()); err != nil {
		return err
	}
	if i.isDyn(expr.CallExpr.Target) {
		i.coerceWellKnownType(exprpb.Type_TIMESTAMP)
	}
	i.query.WriteString(")")
	if decrementReturnValue {
		i.query.WriteString(" - 1)")
	}
	return nil
}

func (i *interpreter) interpretTimestampFunction(expr *exprpb.Expr_CallExpr) error {
	if err := i.interpretExpr(expr.CallExpr.Args[0]); err != nil {
		return err
	}
	i.query.WriteString("::TIMESTAMP WITH TIME ZONE")
	return nil
}
