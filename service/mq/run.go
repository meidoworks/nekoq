package mq

func run(event string, f func()) {
	go f()
}
