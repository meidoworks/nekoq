package simplemq

import "encoding/json"

func PrintJson(obj interface{}) string {
	p, _ := json.Marshal(obj)
	return string(p)
}
