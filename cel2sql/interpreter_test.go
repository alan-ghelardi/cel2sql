package cel2sql

import (
	"testing"

	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/go-cmp/cmp"
	resultspb "github.com/tektoncd/results/proto/v1alpha2/results_go_proto"
)

func TestInterpreteRecordExpressions(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{{
		name: "simple expression",
		in:   `name == "foo"`,
		want: "name = 'foo'",
	},
		{
			name: "select expression",
			in:   `data.metadata.namespace == "default"`,
			want: "(data->'metadata'->>'namespace') = 'default'",
		},
		{
			name: "type coercion with a dyn expression in the left hand side",
			in:   `data.status.completionTime > timestamp("2022/10/30T21:45:00.000Z")`,
			want: "(data->'status'->>'completionTime')::TIMESTAMP WITH TIME ZONE > '2022/10/30T21:45:00.000Z'::TIMESTAMP WITH TIME ZONE",
		},
		{
			name: "type coercion with a dyn expression in the right hand side",
			in:   `timestamp("2022/10/30T21:45:00.000Z") < data.status.completionTime`,
			want: "'2022/10/30T21:45:00.000Z'::TIMESTAMP WITH TIME ZONE < (data->'status'->>'completionTime')::TIMESTAMP WITH TIME ZONE",
		},
		{
			name: "in operator",
			in:   `data.metadata.namespace in ["foo", "bar"]`,
			want: "(data->'metadata'->>'namespace') IN ('foo', 'bar')",
		},
		{
			name: "contains string function",
			in:   `data.metadata.name.contains("foo")`,
			want: "POSITION('foo' IN (data->'metadata'->>'name')) <> 0",
		},
		{
			name: "endsWith string function",
			in:   `data.metadata.name.endsWith("bar")`,
			want: "(data->'metadata'->>'name') LIKE '%' || 'bar'",
		},
		{
			name: "getDate function",
			in:   `data.status.completionTime.getDate() == 2`,
			want: "EXTRACT(DAY FROM (data->'status'->>'completionTime')::TIMESTAMP WITH TIME ZONE) = 2",
		},
		{
			name: "getDayOfMonth function",
			in:   `data.status.completionTime.getDayOfMonth() == 2`,
			want: "(EXTRACT(DAY FROM (data->'status'->>'completionTime')::TIMESTAMP WITH TIME ZONE) - 1) = 2",
		},
		{
			name: "getDayOfWeek function",
			in:   `data.status.completionTime.getDayOfWeek() > 0`,
			want: "EXTRACT(DOW FROM (data->'status'->>'completionTime')::TIMESTAMP WITH TIME ZONE) > 0",
		},
		{
			name: "getDayOfYear function",
			in:   `data.status.completionTime.getDayOfYear() > 15`,
			want: "(EXTRACT(DOY FROM (data->'status'->>'completionTime')::TIMESTAMP WITH TIME ZONE) - 1) > 15",
		},
		{
			name: "getFullYear function",
			in:   `data.status.completionTime.getFullYear() >= 2022`,
			want: "EXTRACT(YEAR FROM (data->'status'->>'completionTime')::TIMESTAMP WITH TIME ZONE) >= 2022",
		},
		{
			name: "matches function",
			in:   `data.metadata.name.matches("^foo.*$")`,
			want: "(data->'metadata'->>'name') ~ '^foo.*$'",
		},
		{
			name: "startsWith string function",
			in:   `data.metadata.name.startsWith("bar")`,
			want: "(data->'metadata'->>'name') LIKE 'bar' || '%'",
		},
	}

	env, err := cel.NewEnv(
		cel.Types(&resultspb.Record{}),
		cel.Declarations(decls.NewVar("name", decls.String)),
		cel.Declarations(decls.NewVar("data_type", decls.String)),
		cel.Declarations(decls.NewVar("data", decls.Any)),
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ast, issues := env.Compile(test.in)
			if issues != nil && issues.Err() != nil {
				t.Fatal(issues.Err())
			}

			interpreter, err := New(ast)
			if err != nil {
				t.Fatal(err)
			}

			got, err := interpreter.Interpret()
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("Mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestInterpreteResultExpressions(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{{
		name: "Result.Summary.Record field",
		in:   `summary.record == "foo/results/bar/records/baz"`,
		want: "recordsummary_record = 'foo/results/bar/records/baz'",
	},
		{
			name: "Result.Summary.StartTime field",
			in:   `summary.start_time > timestamp("2022/10/30T21:45:00.000Z")`,
			want: "recordsummary_start_time > '2022/10/30T21:45:00.000Z'::TIMESTAMP WITH TIME ZONE",
		},
		{
			name: "comparison with the PIPELINE_RUN const value",
			in:   `summary.type == PIPELINE_RUN`,
			want: "recordsummary_type = 'tekton.dev/v1beta1.PipelineRun'",
		},
		{
			name: "comparison with the PIPELINE_RUN const value",
			in:   `summary.type == TASK_RUN`,
			want: "recordsummary_type = 'tekton.dev/v1beta1.TaskRun'",
		},
		{
			name: "status constants",
			in:   `summary.status == CANCELLED || summary.status == TIMEOUT`,
			want: "recordsummary_status = 4  OR recordsummary_status = 3",
		},
	}

	env, err := cel.NewEnv(
		cel.Declarations(stringConst("PIPELINE_RUN", "tekton.dev/v1beta1.PipelineRun"),
			stringConst("TASK_RUN", "tekton.dev/v1beta1.TaskRun"),
		),
		cel.Declarations(recordSummaryStatusConsts()...),
		cel.Types(&resultspb.RecordSummary{}),
		cel.Variable("summary",
			cel.ObjectType("tekton.results.v1alpha2.RecordSummary")),
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ast, issues := env.Compile(test.in)
			if issues != nil && issues.Err() != nil {
				t.Fatal(issues.Err())
			}

			interpreter, err := New(ast)
			if err != nil {
				t.Fatal(err)
			}

			got, err := interpreter.Interpret()
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("Mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// stringConst is a helper to create a CEL string constant declaration.
func stringConst(name, value string) *exprpb.Decl {
	return decls.NewConst(name,
		decls.String,
		&exprpb.Constant{ConstantKind: &exprpb.Constant_StringValue{StringValue: value}})
}

// recordSummaryStatusConsts exposes the values of the RecordSummary_Status enum
// as named constants.
func recordSummaryStatusConsts() []*exprpb.Decl {
	constants := make([]*exprpb.Decl, 0, len(resultspb.RecordSummary_Status_value))
	for name, value := range resultspb.RecordSummary_Status_value {
		constants = append(constants, decls.NewConst(name, decls.Int, &exprpb.Constant{ConstantKind: &exprpb.Constant_Int64Value{Int64Value: int64(value)}}))
	}
	return constants
}
