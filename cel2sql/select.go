package cel2sql

import (
	"fmt"

	"gorm.io/gorm/schema"
)

// translateToJSONAccessors converts the provided field path to a Postgres JSON
// property selection directive. This allows us to yield appropriate SQL
// expressions to navigate through the record.data field, for instance.
func (i *interpreter) translateToJSONAccessors(fieldPath []string) {
	firstField := fieldPath[0]
	lastField := fieldPath[len(fieldPath)-1]

	fmt.Fprintf(&i.query, "(%s->", firstField)
	if len(fieldPath) > 2 {
		for _, field := range fieldPath[1 : len(fieldPath)-1] {
			fmt.Fprintf(&i.query, "'%s'->", field)
		}
	}
	fmt.Fprintf(&i.query, ">'%s')", lastField)
}

// translateIntoRecordSummaryColum
func (i *interpreter) translateIntoRecordSummaryColum(fieldPath []string) {
	namer := &schema.NamingStrategy{}
	fmt.Fprintf(&i.query, "recordsummary_%s", namer.ColumnName("", fieldPath[1]))
}
