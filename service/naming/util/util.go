package util

import (
	"strings"
)

func CheckGroupPath(groupPath string) bool {
	if !strings.HasPrefix(groupPath, "/") {
		return false
	}
	if strings.Contains(groupPath, "//") {
		return false
	}
	if len(groupPath) > 1 && strings.HasSuffix(groupPath, "/") {
		return false
	}
	data := []byte(groupPath)
	for _, v := range data {
		if (v >= 'a' && v <= 'z') || (v >= 'A' && v <= 'Z' || (v >= '0' && v <= '9') || v == '/' || v == '_' || v == '.') {
			continue
		} else {
			return false
		}
	}
	return true
}

func SplitGroupPath(groupPath string) []string {
	return strings.Split(groupPath, "/")[1:]
}

func MakeGroupPath(paths []string) string {
	return "/" + strings.Join(paths, "/")
}
