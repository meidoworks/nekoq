package rules

import "regexp"

func ValidateWarehouseKey(key string) bool {
	matched, err := regexp.MatchString(`^([a-zA-Z0-9_-]+)(\.[a-zA-Z0-9_-]+)*$`, key)
	if err != nil {
		//FIXME should panic?
		panic(err)
	}
	return matched
}
