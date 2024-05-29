package mqmeta

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/meidoworks/nekoq/service/mqapi"
)

type RoutingMatching struct {
	Rule string

	TagId mqapi.TagId
}

func (m *RoutingMatching) Match(routingKey string) bool {
	// support wildcard matching
	//FIXME need simple & reliable solution
	b, err := filepath.Match(m.Rule, routingKey)
	if err != nil {
		log.Println("matchBindingKey failed: " + fmt.Sprint(err))
		return false
	}
	return b
}
