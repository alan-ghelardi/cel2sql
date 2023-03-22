package lister

import (
	pagetokenpb "cel2sql/lister/proto/pagetoken_go_proto"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

var (
	allowedOrderByFieldsForResults = map[string]string{
		// Deprecated fields in the Results type.
		"created_time": "created_time",
		"updated_time": "updated_time",

		"create_time": "created_time",
		"update_time": "updated_time",

		// Fields of RecordSummary type.
		"summary.start_time": "recordsummary_start_time",
		"summary.end_time":   "recordsummary_end_time",
	}

	orderByPattern = regexp.MustCompile(`^([\w\.]+)\s*(ASC|asc|DESC|desc)?$`)
)

type order struct {
	columnName string
	direction  string
}

// validateToken implements the queryBuilder interface.
func (o *order) validateToken(token *pagetokenpb.PageToken) error {
	return nil
}

// build implements the queryBuilder interface.
func (o *order) build(db *gorm.DB) (*gorm.DB, error) {
	direction := "ASC"
	if o.direction != "" {
		direction = o.direction
	}
	if o.columnName != "" {
		db = db.Order(o.columnName + " " + direction)
	}
	return db.Order("id " + direction), nil
}

// parseOrderBy attempts to parse the input into a suitable tuple of column and
// direction to be used in the sql order by clause.
func parseOrderBy(in string) (columnName string, direction string, err error) {
	in = strings.TrimSpace(in)
	if in == "" {
		return "", "", nil
	}

	matches := orderByPattern.FindStringSubmatch(in)
	if matches == nil {
		return "", "", status.Error(codes.InvalidArgument, "invalid order by statement")
	}

	fieldName := matches[1]
	columnName = allowedOrderByFieldsForResults[fieldName]
	if columnName == "" {
		return "", "", status.Errorf(codes.InvalidArgument, "%s: field is unknown or cannot be used in the order by clause", fieldName)
	}

	if desiredDirection := matches[2]; desiredDirection == "" {
		direction = "ASC"
	} else {
		direction = strings.ToUpper(strings.TrimSpace(matches[2]))
	}

	return
}
