package mq

import (
	"encoding/json"
	"log"
	"regexp"
	"strings"
)

func ConcatNames(strs ...string) string {
	return strings.Join(strs, ":")
}

func ValidateNameForBrokerMechanisms(v string) bool {
	matched, err := regexp.MatchString(`^([a-zA-Z0-9_-]+)(\.[a-zA-Z0-9_-]+)*$`, v)
	if err != nil {
		//FIXME should panic?
		panic(err)
	}
	return matched
}

func ValidateNameForBrokerMechanismsWithWildcard(v string) bool {
	// add * support
	matched, err := regexp.MatchString(`^([a-zA-Z0-9_\-*]+)(\.[a-zA-Z0-9_\-*]+)*$`, v)
	if err != nil {
		//FIXME should panic?
		panic(err)
	}
	return matched
}

func DebugJsonPrint(i interface{}) {
	data, _ := json.MarshalIndent(i, "", "    ")
	log.Println(string(data))
}
