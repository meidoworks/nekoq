package mqapi

import "errors"

type ComponentRegistry interface {
}

var (
	queueTypeContainer = make(map[string]func() QueueType)
)

func Register[T any](key string, v T) error {
	switch vv := (interface{})(v).(type) {
	case func() QueueType:
		_, ok := queueTypeContainer[key]
		if !ok {
			queueTypeContainer[key] = vv
			return nil
		} else {
			return ErrAddonAlreadyExist
		}
	default:
		panic(errors.New("should not reach here"))
	}
}

func GetQueueTypeContainer(key string) (func() QueueType, error) {
	v, ok := queueTypeContainer[key]
	if !ok {
		return nil, ErrQueueStoreUnknown
	} else {
		return v, nil
	}
}
