package lister

import (
	pagetokenpb "cel2sql/lister/proto/pagetoken_go_proto"
	"fmt"

	"gorm.io/gorm"
)

type offset struct {
	order     *order
	pageToken *pagetokenpb.PageToken
}

// validateToken implements the queryBuilder interface.
func (o *offset) validateToken(token *pagetokenpb.PageToken) error {
	return nil
}

// build implements the queryBuilder interface.
func (o *offset) build(db *gorm.DB) (*gorm.DB, error) {
	if o.pageToken != nil {
		if lastItem := o.pageToken.LastItem; lastItem != nil {
			var leftHandSideExpression, rightHandSideExpression string
			comparisonOperator := ">"
			values := []any{}
			if orderBy := lastItem.OrderBy; orderBy != nil {
				leftHandSideExpression = fmt.Sprintf("(%s, id)", o.order.columnName)
				rightHandSideExpression = "(?, ?)"
				values = append(values, orderBy.Value.AsTime())
				if orderBy.Direction == pagetokenpb.Order_DESCENDING {
					comparisonOperator = "<"
				}
			} else {
				leftHandSideExpression = "id"
				rightHandSideExpression = "?"
			}
			values = append(values, lastItem.Id)
			db = db.Where(fmt.Sprintf("%s %s %s", leftHandSideExpression, comparisonOperator, rightHandSideExpression), values...)
		}
	}
	return db, nil
}
