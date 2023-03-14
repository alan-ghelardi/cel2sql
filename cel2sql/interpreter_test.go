package cel2sql

import (
	"testing"

	"cel2sql/cel"

	"github.com/google/go-cmp/cmp"
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
			name: "index operator",
			in:   `data.metadata.labels["foo"] == "bar"`,
			want: "(data->'metadata'->'labels'->>'foo') = 'bar'",
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

	env, err := cel.NewRecordsEnv()
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
		name: "Result.Annotations field",
		in:   `annotations["repo"] == "tektoncd/results"`,
		want: `annotations @> '{"repo":"tektoncd/results"}'::jsonb`,
	},
		{
			name: "Result.Annotations field",
			in:   `"tektoncd/results" == annotations["repo"]`,
			want: `annotations @> '{"repo":"tektoncd/results"}'::jsonb`,
		},
		{
			name: "other operators involving the Result.Annotations field",
			in:   `annotations["repo"].startsWith("tektoncd")`,
			want: "annotations->>'repo' LIKE 'tektoncd' || '%'",
		},
		{
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
			name: "comparison with the TASK_RUN const value",
			in:   `summary.type == TASK_RUN`,
			want: "recordsummary_type = 'tekton.dev/v1beta1.TaskRun'",
		},
		{
			name: "RecordSummary_Status constants",
			in:   `summary.status == CANCELLED || summary.status == TIMEOUT`,
			want: "recordsummary_status = 4  OR recordsummary_status = 3",
		},
		{
			name: "Result.Summary.Annotations",
			in:   `summary.annotations["branch"] == "main"`,
			want: `recordsummary_annotations @> '{"branch":"main"}'::jsonb`,
		},
		{
			name: "Result.Summary.Annotations",
			in:   `"main" == summary.annotations["branch"]`,
			want: `recordsummary_annotations @> '{"branch":"main"}'::jsonb`,
		},
		{
			name: "more complex expression",
			in:   `summary.annotations["actor"] == "john-doe" && summary.annotations["branch"] == "feat/amazing" && summary.status == SUCCESS`,
			want: `recordsummary_annotations @> '{"actor":"john-doe"}'::jsonb AND recordsummary_annotations @> '{"branch":"feat/amazing"}'::jsonb  AND recordsummary_status = 1`,
		},
	}

	env, err := cel.NewResultsEnv()
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
