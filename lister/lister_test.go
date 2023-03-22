package lister

import (
	"cel2sql/cel"
	pagetokenpb "cel2sql/lister/proto/pagetoken_go_proto"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/utils/tests"
)

func TestBuildQuery(t *testing.T) {
	env, err := cel.NewResultsEnv()
	if err != nil {
		t.Fatal(err)
	}

	db, _ := gorm.Open(tests.DummyDialector{})
	statement := &gorm.Statement{DB: db, Clauses: map[string]clause.Clause{}}
	db.Statement = statement

	now := time.Now()

	order := &order{
		columnName: "created_time",
		direction:  "DESC",
	}

	token := &pagetokenpb.PageToken{
		Filter: `summary.status == SUCCESS`,
		LastItem: &pagetokenpb.Item{
			Id: "bar",
			OrderBy: &pagetokenpb.Order{
				FieldName: "create_time",
				Value:     timestamppb.New(now),
				Direction: pagetokenpb.Order_DESCENDING,
			},
		},
	}

	lister := &Lister[any, any]{
		queryBuilders: []queryBuilder{
			&offset{
				order:     order,
				pageToken: token,
			},
			&filter{
				env: env,
				equalityClauses: []equalityClause{{
					columnName: "parent",
					value:      "foo",
				},
				},
				expr: token.Filter,
			},
			order,
		},
		pageToken: token,
	}

	t.Run("complex query", func(t *testing.T) {
		testDB, err := lister.buildQuery(context.Background(), db)
		if err != nil {
			t.Fatal(err)
		}

		testDB.Statement.Build("WHERE", "ORDER BY")

		want := "WHERE (created_time, id) < (?, ?) AND parent = ? AND recordsummary_status = 1 ORDER BY created_time DESC,id DESC"
		if got := testDB.Statement.SQL.String(); want != got {
			t.Errorf("Want %q, but got %q", want, got)
		}

		wantVars := []any{now, "bar", "foo"}
		if diff := cmp.Diff(wantVars, testDB.Statement.Vars); diff != "" {
			t.Errorf("Mismatch in the statement's vars:\n%s", diff)
		}
	})

	t.Run("return an error if the provided page token is invalid", func(t *testing.T) {
		token.Filter = `parent == "bar"`
		_, err := lister.buildQuery(context.Background(), db)
		if err == nil {
			t.Fatal("Want error, but got nil")
		}
		if !strings.Contains(err.Error(), "invalid page token") {
t.Fatalf("Unexpected error %v", err)
		}
	})
}
