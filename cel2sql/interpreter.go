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

// Interpreter is a statefull converter of CEL expressions to equivalent SQL
// filters in the Postgres dialect.
type Interpreter struct {
	checkedExpr *exprpb.CheckedExpr

	query strings.Builder
}

// New takes an abstract syntax tree and returns an Interpreter object capable
// of converting it to a set of SQL filters.
func New(ast *cel.Ast) (*Interpreter, error) {
	checkedExpr, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		return nil, err
	}
	return &Interpreter{
		checkedExpr: checkedExpr,
	}, nil
}

// Interpret attempts to convert the CEL AST into a set of valid SQL filters. It
// returns an error if the conversion cannot be done.
func (i *Interpreter) Interpret() (string, error) {
	if err := i.InterpretExpr(i.checkedExpr.Expr); err != nil {
		return "", err
	}
	return strings.TrimSpace(i.query.String()), nil
}

func (i *Interpreter) InterpretExpr(expr *exprpb.Expr) error {
	id := expr.Id
	switch node := expr.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		return i.interpretConstExpr(id, node)

	case *exprpb.Expr_IdentExpr:
		return i.interpretIdentExpr(id, node)

	case *exprpb.Expr_SelectExpr:
		return i.interpretSelectExpr(id, node)

	case *exprpb.Expr_CallExpr:
		return i.interpretCallExpr(id, node)

	case *exprpb.Expr_ListExpr:
		return i.interpretListExpr(id, node)

	case *exprpb.Expr_StructExpr:
		return i.unsupportedExprError(id, "struct")

	case *exprpb.Expr_ComprehensionExpr:
		return i.unsupportedExprError(id, "comprehension")

	}
	return nil
}

// unsupportedExprError attempts to return a descriptive error on why the
// provided CEL expression could not be converted.
func (i *Interpreter) unsupportedExprError(id int64, name string) error {
	sourceInfo := i.checkedExpr.SourceInfo
	column := sourceInfo.Positions[id]
	var line int32
	for i, offset := range sourceInfo.LineOffsets {
		line = int32(i) + 1
		if offset > column {
			break
		}
	}

	return fmt.Errorf("%w %s at line %d, column %d", ErrUnsupportedExpression, name, line, column)
}

func (i *Interpreter) interpretConstExpr(id int64, expr *exprpb.Expr_ConstExpr) error {
	switch expr.ConstExpr.ConstantKind.(type) {

	case *exprpb.Constant_NullValue:
		i.query.WriteString("NULL")

	case *exprpb.Constant_BoolValue:
		if expr.ConstExpr.GetBoolValue() {
			i.query.WriteString("TRUE")
		} else {
			i.query.WriteString("FALSE")
		}

	case *exprpb.Constant_Int64Value:
		fmt.Fprintf(&i.query, "%d", expr.ConstExpr.GetInt64Value())

	case *exprpb.Constant_Uint64Value:
		fmt.Fprintf(&i.query, "%d", expr.ConstExpr.GetInt64Value())

	case *exprpb.Constant_DoubleValue:
		fmt.Fprintf(&i.query, "%f", expr.ConstExpr.GetDoubleValue())

	case *exprpb.Constant_StringValue:
		fmt.Fprintf(&i.query, "'%s'", expr.ConstExpr.GetStringValue())

	case *exprpb.Constant_BytesValue:

	case *exprpb.Constant_DurationValue:
		fmt.Fprintf(&i.query, "'%d SECONDS'", expr.ConstExpr.GetDurationValue().Seconds)

	case *exprpb.Constant_TimestampValue:
		timestamp := expr.ConstExpr.GetTimestampValue()
		fmt.Fprintf(&i.query, "TIMESTAMP WITH TIME ZONE '%s'", timestamp.AsTime().Format(time.RFC3339))
	default:
		return i.unsupportedExprError(id, "constant")
	}

	return nil
}

func (i *Interpreter) interpretIdentExpr(id int64, expr *exprpb.Expr_IdentExpr) error {
	i.query.WriteString(expr.IdentExpr.GetName())
	return nil
}

func (i *Interpreter) interpretSelectExpr(id int64, expr *exprpb.Expr_SelectExpr) error {
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

	sortedFields := make([]string, len(fields))
	for j, k := 0, len(fields)-1; j < len(sortedFields); j, k = j+1, k-1 {
		sortedFields[j] = fields[k]
	}

	firstField := sortedFields[0]
	lastField := sortedFields[len(sortedFields)-1]

	fmt.Fprintf(&i.query, "(%s->", firstField)
	if len(sortedFields) > 2 {
		for _, field := range sortedFields[1 : len(sortedFields)-1] {
			fmt.Fprintf(&i.query, "'%s'->", field)
		}
	}
	fmt.Fprintf(&i.query, ">'%s')", lastField)

	return nil
}

func (i *Interpreter) interpretCallExpr(id int64, expr *exprpb.Expr_CallExpr) error {
	function := expr.CallExpr.GetFunction()
	if isUnaryOperator(function) {
		return i.interpretUnaryCallExpr(expr)
	}
	if isBinaryOperator(function) {
		return i.interpretBinaryCallExpr(expr)
	}

	return i.interpretFunctionCallExpr(id, expr)
}

func (i *Interpreter) interpretUnaryCallExpr(expr *exprpb.Expr_CallExpr) error {
	sqlOperator := unaryOperators[expr.CallExpr.GetFunction()]
	i.query.WriteString(sqlOperator)
	i.query.WriteString(space)
	if err := i.InterpretExpr(expr.CallExpr.Args[0]); err != nil {
		return err
	}
	i.query.WriteString(space)
	return nil
}

func (i *Interpreter) interpretBinaryCallExpr(expr *exprpb.Expr_CallExpr) error {
	sqlOperator := binaryOperators[expr.CallExpr.GetFunction()]
	arg1 := expr.CallExpr.Args[0]
	arg2 := expr.CallExpr.Args[1]
	if err := i.InterpretExpr(arg1); err != nil {
		return err
	}

	if i.isDyn(arg1) {
		if err := i.coerceToTypeOf(arg2); err != nil {
			return err
		}
	}

	i.query.WriteString(space)
	i.query.WriteString(sqlOperator)
	i.query.WriteString(space)

	if err := i.InterpretExpr(arg2); err != nil {
		return err
	}

	if i.isDyn(arg2) {
		if err := i.coerceToTypeOf(arg1); err != nil {
			return err
		}
	}
	i.query.WriteString(space)

	return nil
}

func (i *Interpreter) interpretListExpr(id int64, expr *exprpb.Expr_ListExpr) error {
	elements := expr.ListExpr.GetElements()
	i.query.WriteString("(")
	for index, elem := range elements {
		if err := i.InterpretExpr(elem); err != nil {
			return err
		}
		if index < len(elements)-1 {
			i.query.WriteString(", ")
		}
	}
	i.query.WriteString(")")
	return nil
}
