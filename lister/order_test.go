package lister

import (
	"testing"

	"gorm.io/gorm/utils/tests"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestOrderByBuild(t *testing.T) {
	db, _ := gorm.Open(tests.DummyDialector{})
	statement := &gorm.Statement{DB: db, Clauses: map[string]clause.Clause{}}
	db.Statement = statement

	t.Run("no order by clause", func(t *testing.T) {
		order := &order{}

		testDB, err := order.build(db)
		if err != nil {
			t.Fatal(err)
		}

		testDB.Statement.Build("ORDER BY")

		want := "ORDER BY id ASC"
		if got := testDB.Statement.SQL.String(); want != got {
			t.Errorf("Want %q, but got %q", want, got)
		}
	})

	t.Run("order by a given column", func(t *testing.T) {
		order := &order{
			columnName: "created_time",
			direction:  "DESC",
		}

		testDB, err := order.build(db)
		if err != nil {
			t.Fatal(err)
		}

		testDB.Statement.Build("ORDER BY")

		want := "ORDER BY created_time DESC,id DESC"
		if got := testDB.Statement.SQL.String(); want != got {
			t.Errorf("Want %q, but got %q", want, got)
		}
	})
}

func TestParseOrderBy(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		column    string
		direction string
	}{{
		name:      "valid order by statement",
		in:        "create_time DESC",
		column:    "created_time",
		direction: "DESC",
	},
		{
			name:      "sort in ascending order",
			in:        "create_time ASC",
			column:    "created_time",
			direction: "ASC",
		},
		{
			name:      "update_time field omitting the direction",
			in:        "update_time",
			column:    "updated_time",
			direction: "ASC",
		},
		{
			name:      "summary.start_time field",
			in:        "summary.start_time asc",
			column:    "recordsummary_start_time",
			direction: "ASC",
		},
		{
			name:      "summary.end_time field",
			in:        "summary.end_time desc",
			column:    "recordsummary_end_time",
			direction: "DESC",
		},
		{
			name:      "trailing and leading spaces",
			in:        "  summary.start_time   asc ",
			column:    "recordsummary_start_time",
			direction: "ASC",
		},
		{
			name:      "trailing and leading spaces with no direction",
			in:        "  summary.start_time   ",
			column:    "recordsummary_start_time",
			direction: "ASC",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotColumn, gotDirection, err := parseOrderBy(test.in)
			if err != nil {
				t.Fatal(err)
			}

			if test.column != gotColumn {
				t.Errorf("Want column %q, but got %q", test.column, gotColumn)
			}

			if test.direction != gotDirection {
				t.Errorf("Want direction %q, but got %q", test.direction, gotDirection)
			}
		})
	}
}

func TestParseOrderByErrors(t *testing.T) {
	tests := []struct {
		name string
		in   string
		err  error
	}{{
		name: "disallowed field in the order by clause",
		in:   "id",
		err:  status.Error(codes.InvalidArgument, "id: field is unknown or cannot be used in the order by clause"),
	},
		{
			name: "invalid order by",
			in:   "this is invalid",
			err:  status.Error(codes.InvalidArgument, "invalid order by statement"),
		},
		{
			name: "invalid direction",
			in:   "create_time ASCC",
			err:  status.Error(codes.InvalidArgument, "invalid order by statement"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := parseOrderBy(test.in)
			if err == nil {
				t.Fatal("want error, but got nil")
			}

			if gotCode := status.Code(test.err); gotCode != status.Code(test.err) {
				t.Fatalf("Want code %d, but got %d", status.Code(test.err), gotCode)
			}

			if diff := cmp.Diff(test.err.Error(), err.Error()); diff != "" {
				t.Errorf("Mismatch in the error message (-want +got):\n%s", diff)
			}
		})
	}
}
