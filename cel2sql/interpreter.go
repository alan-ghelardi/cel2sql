package cel2sql

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

const (
	space = " "
)

// ErrUnsupportedExpression is a sentinel error returned when the CEL expression
// cannot be converted to a set of compatible SQL filters.
var ErrUnsupportedExpression = errors.New("unsupported CEL")

// interpreter is a statefull converter of CEL expressions to equivalent SQL
// filters in the Postgres dialect.
type interpreter struct {
	checkedExpr *exprpb.CheckedExpr

	query strings.Builder
}

// newInterpreter takes an abstract syntax tree and returns an Interpreter object capable
// of converting it to a set of SQL filters.
func newInterpreter(ast *cel.Ast) (*interpreter, error) {
	checkedExpr, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		return nil, err
	}
	return &interpreter{
		checkedExpr: checkedExpr,
	}, nil
}

// interpret attempts to convert the CEL AST into a set of valid SQL filters. It
// returns an error if the conversion cannot be done.
func (i *interpreter) interpret() (string, error) {
	if err := i.interpretExpr(i.checkedExpr.Expr); err != nil {
		return "", err
	}
	return strings.TrimSpace(i.query.String()), nil
}

func (i *interpreter) interpretExpr(expr *exprpb.Expr) error {
	id := expr.Id
	switch node := expr.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		return i.interpretConstExpr(id, node.ConstExpr)

	case *exprpb.Expr_IdentExpr:
		return i.interpretIdentExpr(id, node)

	case *exprpb.Expr_SelectExpr:
		return i.interpretSelectExpr(id, node)

	case *exprpb.Expr_CallExpr:
		return i.interpretCallExpr(id, node)

	case *exprpb.Expr_ListExpr:
		return i.interpretListExpr(id, node)

	default:
		return i.unsupportedExprError(id, "")
	}
}

// unsupportedExprError attempts to return a descriptive error on why the
// provided CEL expression could not be converted.
func (i *interpreter) unsupportedExprError(id int64, name string) error {
	sourceInfo := i.checkedExpr.SourceInfo
	column := sourceInfo.Positions[id]
	var line int32
	for i, offset := range sourceInfo.LineOffsets {
		line = int32(i) + 1
		if offset > column {
			break
		}
	}

	if name != "" {
		name += " "
	}

	return fmt.Errorf("%w %sstatement at line %d, column %d", ErrUnsupportedExpression, name, line, column)
}

func (i *interpreter) interpretConstExpr(id int64, expr *exprpb.Constant) error {
	switch expr.ConstantKind.(type) {

	case *exprpb.Constant_NullValue:
		i.query.WriteString("NULL")

	case *exprpb.Constant_BoolValue:
		if expr.GetBoolValue() {
			i.query.WriteString("TRUE")
		} else {
			i.query.WriteString("FALSE")
		}

	case *exprpb.Constant_Int64Value:
		fmt.Fprintf(&i.query, "%d", expr.GetInt64Value())

	case *exprpb.Constant_Uint64Value:
		fmt.Fprintf(&i.query, "%d", expr.GetInt64Value())

	case *exprpb.Constant_DoubleValue:
		fmt.Fprintf(&i.query, "%f", expr.GetDoubleValue())

	case *exprpb.Constant_StringValue:
		fmt.Fprintf(&i.query, "'%s'", expr.GetStringValue())

	case *exprpb.Constant_DurationValue:
		fmt.Fprintf(&i.query, "'%d SECONDS'", expr.GetDurationValue().Seconds)

	case *exprpb.Constant_TimestampValue:
		timestamp := expr.GetTimestampValue()
		fmt.Fprintf(&i.query, "TIMESTAMP WITH TIME ZONE '%s'", timestamp.AsTime().Format(time.RFC3339))

	default:
		return i.unsupportedExprError(id, "constant")
	}
	return nil
}

func (i *interpreter) interpretIdentExpr(id int64, expr *exprpb.Expr_IdentExpr) error {
	if reference, found := i.checkedExpr.ReferenceMap[id]; found && reference.GetValue() != nil {
		return i.interpretConstExpr(id, reference.GetValue())
	}
	name := expr.IdentExpr.GetName()
	if name == "data_type" {
		// This field maps to the records.type column.
		name = "type"
	}
	i.query.WriteString(name)
	return nil
}

func (i *interpreter) interpretSelectExpr(id int64, expr *exprpb.Expr_SelectExpr, additionalExprs ...*exprpb.Expr) error {
	fields := []string{expr.SelectExpr.GetField()}

	target := expr.SelectExpr.GetOperand()
	for target != nil {
		switch target.ExprKind.(type) {
		case *exprpb.Expr_SelectExpr:
			fields = append(fields, target.GetSelectExpr().GetField())

		case *exprpb.Expr_IdentExpr:
			fields = append(fields, target.GetIdentExpr().GetName())

		default:
			return ErrUnsupportedExpression
		}
		target = target.GetSelectExpr().GetOperand()
	}

	reversedFields := make([]string, len(fields))
	for j, k := 0, len(fields)-1; j < len(reversedFields); j, k = j+1, k-1 {
		reversedFields[j] = fields[k]
	}

	for _, node := range additionalExprs {
		switch node.ExprKind.(type) {
		case *exprpb.Expr_ConstExpr:
			reversedFields = append(reversedFields, node.GetConstExpr().GetStringValue())

		default:
			return ErrUnsupportedExpression
		}
	}

	if i.isDyn(expr.SelectExpr.GetOperand()) {
		i.translateToJSONAccessors(reversedFields)
		return nil
	}

	if i.isRecordSummary(expr.SelectExpr.GetOperand()) {
		i.translateIntoRecordSummaryColum(reversedFields)
		return nil
	}

	return fmt.Errorf("%w. %s: not recognized field.", i.unsupportedExprError(id, "select"), reversedFields[0])
}

func (i *interpreter) interpretCallExpr(id int64, expr *exprpb.Expr_CallExpr) error {
	function := expr.CallExpr.GetFunction()
	if isUnaryOperator(function) {
		return i.interpretUnaryCallExpr(expr)
	}
	if isBinaryOperator(function) {
		return i.interpretBinaryCallExpr(expr)
	}

	if isIndexOperator(function) {
		return i.interpretIndexExpr(id, expr)
	}

	return i.interpretFunctionCallExpr(id, expr)
}

func (i *interpreter) interpretUnaryCallExpr(expr *exprpb.Expr_CallExpr) error {
	sqlOperator := unaryOperators[expr.CallExpr.GetFunction()]
	i.query.WriteString(sqlOperator)
	i.query.WriteString(space)
	if err := i.interpretExpr(expr.CallExpr.Args[0]); err != nil {
		return err
	}
	i.query.WriteString(space)
	return nil
}

func (i *interpreter) interpretBinaryCallExpr(expr *exprpb.Expr_CallExpr) error {
	function := expr.CallExpr.GetFunction()
	arg1 := expr.CallExpr.Args[0]
	arg2 := expr.CallExpr.Args[1]

	if i.mayBeTranslatedIntoJSONPathContainsExpression(arg1, function, arg2) {
		return i.translateIntoJSONPathContainsExpression(arg1, arg2)
	}

	if i.mayBeTranslatedIntoJSONPathContainsExpression(arg2, function, arg1) {
		return i.translateIntoJSONPathContainsExpression(arg2, arg1)
	}

	sqlOperator := binaryOperators[function]

	if err := i.interpretExpr(arg1); err != nil {
		return err
	}

	// Implicit coercion
	if i.isDyn(arg1) {
		if err := i.coerceToTypeOf(arg2); err != nil {
			return err
		}
	}

	i.query.WriteString(space)
	i.query.WriteString(sqlOperator)
	i.query.WriteString(space)

	if err := i.interpretExpr(arg2); err != nil {
		return err
	}

	// Implicit coercion
	if i.isDyn(arg2) {
		if err := i.coerceToTypeOf(arg1); err != nil {
			return err
		}
	}
	i.query.WriteString(space)

	return nil
}

func (i *interpreter) interpretListExpr(id int64, expr *exprpb.Expr_ListExpr) error {
	elements := expr.ListExpr.GetElements()
	i.query.WriteString("(")
	for index, elem := range elements {
		if err := i.interpretExpr(elem); err != nil {
			return err
		}
		if index < len(elements)-1 {
			i.query.WriteString(", ")
		}
	}
	i.query.WriteString(")")
	return nil
}
