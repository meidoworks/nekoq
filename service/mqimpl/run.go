package mqimpl

func run(event string, f func()) {
	go f()
}
