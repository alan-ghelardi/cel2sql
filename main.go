package main

import (
	"cel2sql/cel2sql"
	"fmt"

	resultspb "github.com/tektoncd/results/proto/v1alpha2/results_go_proto"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
)

const expr = `data.metadata.name.startsWith("foo") && data.status.completionTime.getDayOfMonth() > 4`

func main() {
	env, err := cel.NewEnv(
		cel.Types(&resultspb.Record{}),
		cel.Declarations(decls.NewVar("name", decls.String)),
		cel.Declarations(decls.NewVar("data_type", decls.String)),
		cel.Declarations(decls.NewVar("data", decls.Any)),
	)
	if err != nil {
		panic(err)
	}

	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		panic(err)
	}

	interpreter, err := cel2sql.New(ast)
	if err != nil {
		panic(err)
	}

	sql, err := interpreter.Interpret()
	if err != nil {
		panic(err)
	}

	fmt.Println(sql)
}
