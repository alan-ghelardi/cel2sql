package lister

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm/utils/tests"

	pagetokenpb "cel2sql/lister/proto/pagetoken_go_proto"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestOffsetBuild(t *testing.T) {
	db, _ := gorm.Open(tests.DummyDialector{})
	statement := &gorm.Statement{DB: db, Clauses: map[string]clause.Clause{}}
	db.Statement = statement

	t.Run("no clauses", func(t *testing.T) {
		offset := &offset{}
		testDB, err := offset.build(db)
		if err != nil {
			t.Fatal(err)
		}

		if got := len(testDB.Statement.Clauses); got != 0 {
			t.Errorf("Want 0 clauses in the statement, but got %d", got)
		}
	})

	t.Run("use only the id to determine the page offset", func(t *testing.T) {
		offset := &offset{
			pageToken: &pagetokenpb.PageToken{
				LastItem: &pagetokenpb.Item{
					Id: "foo",
				},
			},
		}

		testDB, err := offset.build(db)
		if err != nil {
			t.Fatal(err)
		}

		testDB.Statement.Build("WHERE")

		want := "WHERE id > ?"
		if got := testDB.Statement.SQL.String(); want != got {
			t.Errorf("Want %q, but got %q", want, got)
		}

		wantVars := []any{"foo"}
		if diff := cmp.Diff(wantVars, testDB.Statement.Vars); diff != "" {
			t.Errorf("Mismatch in the statement's vars (-want +got):\n%s", diff)
		}
	})

	t.Run("use more than one field to determine the page offset", func(t *testing.T) {
		offset := &offset{
			order: &order{
				columnName: "created_time",
			},
			pageToken: &pagetokenpb.PageToken{
				LastItem: &pagetokenpb.Item{
					Id: "foo",
					OrderBy: &pagetokenpb.Order{
						FieldName: "create_time",
						Value:     timestamppb.New(time.Now()),
						Direction: pagetokenpb.Order_ASCENDING,
					},
				},
			},
		}

		testDB, err := offset.build(db)
		if err != nil {
			t.Fatal(err)
		}

		testDB.Statement.Build("WHERE")

		want := "WHERE (created_time, id) > (?, ?)"
		if got := testDB.Statement.SQL.String(); want != got {
			t.Errorf("Want %q, but got %q", want, got)
		}

		wantVars := []any{offset.pageToken.LastItem.OrderBy.Value.AsTime(), "foo"}
		if diff := cmp.Diff(wantVars, testDB.Statement.Vars); diff != "" {
			t.Errorf("Mismatch in the statement's vars (-want +got):\n%s", diff)
		}
	})

	t.Run("paginating results using descending order", func(t *testing.T) {
		offset := &offset{
			order: &order{
				columnName: "created_time",
			},
			pageToken: &pagetokenpb.PageToken{
				LastItem: &pagetokenpb.Item{
					Id: "foo",
					OrderBy: &pagetokenpb.Order{
						FieldName: "create_time",
						Value:     timestamppb.New(time.Now()),
						Direction: pagetokenpb.Order_DESCENDING,
					},
				},
			},
		}

		testDB, err := offset.build(db)
		if err != nil {
			t.Fatal(err)
		}

		testDB.Statement.Build("WHERE")

		want := "WHERE (created_time, id) < (?, ?)"
		if got := testDB.Statement.SQL.String(); want != got {
			t.Errorf("Want %q, but got %q", want, got)
		}

		wantVars := []any{offset.pageToken.LastItem.OrderBy.Value.AsTime(), "foo"}
		if diff := cmp.Diff(wantVars, testDB.Statement.Vars); diff != "" {
			t.Errorf("Mismatch in the statement's vars (-want +got):\n%s", diff)
		}
	})
}
