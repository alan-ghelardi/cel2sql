package lister

import (
	pagetokenpb "cel2sql/lister/proto/pagetoken_go_proto"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEncodeAndDecodePageToken(t *testing.T) {
	pageToken := &pagetokenpb.PageToken{
		Parent: "foo",
		Filter: "summary.status == SUCCESS",
		LastItem: &pagetokenpb.Item{
			Id: "42",
			OrderBy: &pagetokenpb.Order{
				FieldName: "create_at",
				Value:     timestamppb.New(time.Now()),
				Direction: pagetokenpb.Order_ASCENDING,
			},
		},
	}

	encodedData, err := EncodePageToken(pageToken)
	if err != nil {
		t.Fatal(err)
	}

	got, err := DecodePageToken(encodedData)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(pageToken, got,
		cmpopts.IgnoreUnexported(pagetokenpb.PageToken{},
			pagetokenpb.Item{},
			pagetokenpb.Order{},
			timestamppb.Timestamp{})); diff != "" {
		t.Errorf("Mismatch (-want +got):\n%s", diff)
	}
}
