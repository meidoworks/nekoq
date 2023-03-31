package hooks

type ShutdownHook struct {
	//TODO finish shutdown hook
}

func (s *ShutdownHook) AddBlockingTask(f func()) {
	//TODO need support hook task handling
	go f()
}
