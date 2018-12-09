package protocol

import (
	"reflect"
	"testing"
)

// we want a test that round trips my encoders and decoders

func TestReplyOp(t *testing.T) {
	type replyOpArgs struct {
		requestID, responseTo        uint32
		flags                        ResponseFlags
		cursorID                     uint64
		startingFrom, numberReturned uint32
		documents                    []interface{}
	}
	for _, c := range []replyOpArgs{
		{0, 0, 0, 0, 0, 1, []interface{}{
			map[string]interface{}{

				"hey": 1,
				"yo":  []interface{}{1, 2},
			}},
		},
	} {
		var docs []Document
		for _, d := range c.documents {
			doc, err := NewDocument(d)
			if err != nil {
				t.Fatalf("failed to create document from %v: %v", d, err)
			}
			docs = append(docs, doc)
		}
		decoded, err := NewReplyOp(c.requestID, c.responseTo, c.flags, c.cursorID, c.startingFrom, c.numberReturned, docs...)
		if err != nil {
			t.Fatalf("Failed to encode reply: %v", err)
		}
		got := replyOpArgs{
			requestID:      decoded.RequestID(),
			responseTo:     decoded.ResponseTo(),
			flags:          decoded.ResponseFlags,
			cursorID:       decoded.CursorID,
			startingFrom:   decoded.StartingFrom,
			numberReturned: decoded.NumberReturned,
		}
		for _, d := range decoded.Documents {
			var m map[string]interface{}
			if err := d.Unmarshal(&m); err != nil {
				t.Fatalf("Failed to unmarshal document: %v", err)
			}
			got.documents = append(got.documents, m)
		}
		if !reflect.DeepEqual(c, got) {
			t.Fatalf("Decoded is not the same as encoded:\n%#v\n%#v", c, got)
		}
	}
}
