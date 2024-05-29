package api

type WatchEventType int

type WatchEvent struct {
	Path string
	Ev   []struct {
		Key       string
		EventType WatchEventType
	}
}

const (
	WatchEventUnknown WatchEventType = iota
	WatchEventFresh
	WatchEventCreated
	WatchEventModified
	WatchEventDelete
)
