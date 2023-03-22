package lister

import (
	"cel2sql/cel"
	"testing"

	"gorm.io/gorm/utils/tests"

	pagetokenpb "cel2sql/lister/proto/pagetoken_go_proto"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestFilterValidateToken(t *testing.T) {
	filter := &filter{expr: `parent == "foo"`}
	token := &pagetokenpb.PageToken{Filter: filter.expr}

	t.Run("valid token", func(t *testing.T) {
		if err := filter.validateToken(token); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		token.Filter = `parent == "bar"`
		if err := filter.validateToken(token); err == nil {
			t.Error("Want error, but got nil")
		}
	})
}

func TestFilterBuild(t *testing.T) {
	env, err := cel.NewResultsEnv()
	if err != nil {
		t.Fatal(err)
	}

	db, _ := gorm.Open(tests.DummyDialector{})
	statement := &gorm.Statement{DB: db, Clauses: map[string]clause.Clause{}}
	db.Statement = statement

	t.Run("no where clause", func(t *testing.T) {
		filter := &filter{}
		testDB, err := filter.build(db)
		if err != nil {
			t.Fatal(err)
		}

		if got := len(testDB.Statement.Clauses); got != 0 {
			t.Errorf("Want 0 clauses in the statement, but got %d", got)
		}
	})

	t.Run("where clause with parent and id", func(t *testing.T) {
		filter := &filter{
			env: env,
			equalityClauses: []equalityClause{
				{columnName: "parent", value: "foo"},
				{columnName: "id", value: "bar"},
			},
		}

		testDB, err := filter.build(db)
		if err != nil {
			t.Fatal(err)
		}

		testDB.Statement.Build("WHERE")

		want := "WHERE parent = ? AND id = ?"
		if got := testDB.Statement.SQL.String(); want != got {
			t.Errorf("Want %q, but got %q", want, got)
		}
	})

	t.Run("where clause with cel2sql filters", func(t *testing.T) {
		filter := &filter{
			env:  env,
			expr: `summary.status == SUCCESS`,
		}

		testDB, err := filter.build(db)
		if err != nil {
			t.Fatal(err)
		}

		testDB.Statement.Build("WHERE")

		want := "WHERE recordsummary_status = 1"
		if got := testDB.Statement.SQL.String(); want != got {
			t.Errorf("Want %q, but got %q", want, got)
		}
	})

	t.Run("more complex filter", func(t *testing.T) {
		filter := &filter{
			env: env,
			equalityClauses: []equalityClause{
				{columnName: "parent", value: "foo"},
				{columnName: "id", value: "bar"},
			},
			expr: "summary.status != SUCCESS",
		}

		testDB, err := filter.build(db)
		if err != nil {
			t.Fatal(err)
		}

		testDB.Statement.Build("WHERE")

		want := "WHERE parent = ? AND id = ? AND recordsummary_status <> 1"
		if got := testDB.Statement.SQL.String(); want != got {
			t.Errorf("Want %q, but got %q", want, got)
		}
	})
}
