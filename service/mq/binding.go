package mq

import (
	"fmt"
	"log"
	"path/filepath"
)

func validateBindingKey(key string) bool {
	//TODO validate binding key in binding flow
	return true
}

func validateMatchingBindingKey(key string) bool {
	return ValidateNameForBrokerMechanisms(key)
}

func matchBindingKey(bindingKey, messageBindingKey string) bool {
	// support wildcard matching
	//FIXME may need simple solution
	b, err := filepath.Match(bindingKey, messageBindingKey)
	if err != nil {
		log.Println("matchBindingKey failed: " + fmt.Sprint(err))
		return false
	}
	return b
}
