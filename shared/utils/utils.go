package utils

func CopyAddMap[K comparable, V any](src map[K]V, newKey K, newValue V) map[K]V {
	c := make(chan map[K]V)
	go func() {
		newMap := make(map[K]V)
		for k, v := range src {
			newMap[k] = v
		}
		newMap[newKey] = newValue

		c <- newMap
	}()
	return <-c
}
