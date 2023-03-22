package lister

import (
	"cel2sql/cel2sql"
	"errors"
	"strings"

	pagetokenpb "cel2sql/lister/proto/pagetoken_go_proto"

	"github.com/google/cel-go/cel"
	"gorm.io/gorm"
)

type filter struct {
	env             *cel.Env
	expr            string
	equalityClauses []equalityClause
}

type equalityClause struct {
	columnName string
	value      any
}

// validateToken implements the queryBuilder interface.
func (f *filter) validateToken(token *pagetokenpb.PageToken) error {
	if strings.TrimSpace(f.expr) != strings.TrimSpace(token.Filter) {
		return errors.New("the filter in the token differs from the filter used in the previous query")
	}
	return nil
}

// build implements the queryBuilder interface.
func (f *filter) build(db *gorm.DB) (*gorm.DB, error) {
	for _, clause := range f.equalityClauses {
		db = db.Where(clause.columnName+" = ?", clause.value)
	}

	if expr := strings.TrimSpace(f.expr); expr != "" {
		sql, err := cel2sql.Convert(f.env, expr)
		if err != nil {
			return nil, err
		}
		db = db.Where(sql)
	}
	return db, nil
}
