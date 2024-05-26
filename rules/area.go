package rules

import "regexp"

func CheckAreaAllowedCharacters(v string) bool {
	matched, err := regexp.MatchString(`^([a-zA-Z0-9_-]+)(\.[a-zA-Z0-9_-]+)*$`, v)
	if err != nil {
		//FIXME should panic?
		panic(err)
	}
	return matched
}
