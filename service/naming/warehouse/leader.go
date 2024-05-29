package warehouse

func (w *Warehouse) Leader(key string) (string, error) {
	return w.consistent.Leader(key)
}

func (w *Warehouse) AcquireLeader(key string, node string) (string, error) {
	return w.consistent.Acquire(key, node)
}
