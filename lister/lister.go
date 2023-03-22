package lister

import (
	"context"

	pagetokenpb "cel2sql/lister/proto/pagetoken_go_proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type queryBuilder interface {
	build(db *gorm.DB) (*gorm.DB, error)
	validateToken(token *pagetokenpb.PageToken) error
}

type Lister[I any, W any] struct {
	queryBuilders []queryBuilder
	pageToken     *pagetokenpb.PageToken
}

func (l *Lister[I, W]) buildQuery(ctx context.Context, db *gorm.DB) (*gorm.DB, error) {
	var err error
	db = db.WithContext(ctx)
	for _, builder := range l.queryBuilders {
		if err := builder.validateToken(l.pageToken); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page token: %v", err)
		}

		db, err = builder.build(db)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	return db, nil
}
