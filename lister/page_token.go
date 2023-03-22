package lister

import (
	pagetokenpb "cel2sql/lister/proto/pagetoken_go_proto"
	"encoding/base64"

	"google.golang.org/protobuf/proto"
)

// DecodePageToken ...
func DecodePageToken(in string) (*pagetokenpb.PageToken, error) {
	decodedData, err := base64.RawURLEncoding.DecodeString(in)
	if err != nil {
		return nil, err
	}
	pageToken := new(pagetokenpb.PageToken)
	if err := proto.Unmarshal(decodedData, pageToken); err != nil {
		return nil, err
	}
	return pageToken, nil
}

// EncodePageToken ...
func EncodePageToken(in *pagetokenpb.PageToken) (string, error) {
	wire, err := proto.Marshal(in)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(wire), nil
}
