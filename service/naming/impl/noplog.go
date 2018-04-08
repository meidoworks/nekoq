package impl

var _ Log = &noplogImpl{}

type noplogImpl struct {
}

func NewNopLog() *noplogImpl {
	return &noplogImpl{}
}

func (this *noplogImpl) IsClosed() bool {
	return false
}

func (this *noplogImpl) Close() error {
	return nil
}

func (this *noplogImpl) FlushLog() error {
	return nil
}

func (this *noplogImpl) WriteLog(data []byte) error {
	return nil
}

func (this *noplogImpl) WriteLogRecord(record *LogRecord) error {
	return nil
}
