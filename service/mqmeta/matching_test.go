package mqmeta

import (
	"testing"

	"github.com/meidoworks/nekoq/service/mqapi"
)

func TestPathMatching(t *testing.T) {
	r := &RoutingMatching{
		Rule:  "demo.bind01",
		TagId: mqapi.TagId{1, 1},
	}
	if !r.Match("demo.bind01") {
		t.Error("Demo.bind01 match failed")
	}

	r = &RoutingMatching{
		Rule:  "demo.*",
		TagId: mqapi.TagId{1, 1},
	}
	if !r.Match("demo.bind01") {
		t.Error("Demo.bind01 match failed")
	}
}
