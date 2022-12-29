package impl

import (
	"bufio"
	"errors"
	"os"
	"sync/atomic"
)

import (
	"github.com/meidoworks/nekoq-common/channel"
)

var (
	already_closed_error = errors.New("log file is already closed. cannot write anymore.")
)

type Log interface {
	IsClosed() bool
	WriteLog([]byte) error
	WriteLogRecord(*LogRecord) error
	FlushLog() error
	Close() error
}

type logImpl struct {
	queue       chan logEntry
	file        *os.File
	fileClosed  bool
	wri         *bufio.Writer
	closeFlag   uint64
	stopRequest uint64
}

func (this *logImpl) IsClosed() bool {
	return atomic.LoadUint64(&this.closeFlag) == 1
}

func (this *logImpl) markClosed() {
	atomic.StoreUint64(&this.closeFlag, 1)
}

func (this *logImpl) IsRequestStopped() bool {
	return atomic.LoadUint64(&this.stopRequest) == 1
}

func (this *logImpl) markStopRequest() {
	atomic.StoreUint64(&this.stopRequest, 1)
}

func (this *logImpl) WriteLog(logLine []byte) (resultErr error) {
	if this.IsRequestStopped() {
		return errors.New("logfile is stopped.")
	}
	le := logEntry{
		Data:        logLine,
		WriteResult: make(chan error, 1),
	}
	defer channel.JudgeSendToClosedChannel(func(err error) {
		resultErr = err
	})
	this.queue <- le
	resultErr = <-le.WriteResult
	return
}

func (this *logImpl) WriteLogRecord(logLine *LogRecord) (resultErr error) {
	if this.IsRequestStopped() {
		return errors.New("logfile is stopped.")
	}
	le := logEntry{
		Record:      logLine,
		WriteResult: make(chan error, 1),
	}
	defer channel.JudgeSendToClosedChannel(func(err error) {
		resultErr = err
	})
	this.queue <- le
	resultErr = <-le.WriteResult
	return
}

func (this *logImpl) FlushLog() (resultErr error) {
	if this.IsRequestStopped() {
		return errors.New("logfile is stopped. cannot flush.")
	}
	le := logEntry{
		Flush:       true,
		WriteResult: make(chan error, 1),
	}
	defer channel.JudgeSendToClosedChannel(func(err error) {
		resultErr = err
	})
	this.queue <- le
	return <-le.WriteResult
}

func (this *logImpl) Close() (resultErr error) {
	if this.IsClosed() {
		return errors.New("logfile is already closed.")
	}
	this.markStopRequest()
	le := logEntry{
		Close:       true,
		WriteResult: make(chan error, 1),
	}
	defer channel.JudgeSendToClosedChannel(func(err error) {
		resultErr = err
	})
	this.queue <- le
	return <-le.WriteResult
}

type logEntry struct {
	Data        []byte
	Record      *LogRecord
	WriteResult chan error
	Flush       bool
	Close       bool
}

func NewLog(filePath string) (Log, error) {
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if fi.Size() == 0 {
		err = writeLogFileHeader(f)
		if err != nil {
			return nil, err
		}
	}

	li := &logImpl{
		queue:     make(chan logEntry, 1024),
		file:      f,
		wri:       bufio.NewWriter(f),
		closeFlag: 0,
	}
	// event loop
	go func() {
		q := li.queue
		for {
			en, ok := <-q
			if ok {
				// process queue
				if en.Close {
					if li.IsClosed() {
						en.WriteResult <- already_closed_error
					} else {
						li.markClosed()
						close(li.queue)
						// both do flush and close
						err := li.wri.Flush()
						err2 := li.file.Close()
						if err != nil {
							en.WriteResult <- err
						} else if err2 != nil {
							en.WriteResult <- err2
						} else {
							en.WriteResult <- nil
						}
					}
				} else if en.Flush {
					if li.IsClosed() {
						en.WriteResult <- already_closed_error
					} else {
						err := li.wri.Flush()
						en.WriteResult <- err
					}
				} else {
					if li.IsClosed() {
						en.WriteResult <- already_closed_error
					} else {
						if en.Record != nil {
							err := en.Record.WriteTo(li.wri)
							en.WriteResult <- err
						} else {
							_, err := li.wri.Write(en.Data)
							en.WriteResult <- err
						}
					}
				}
			} else {
				// when queue closed, flush and close file, omit error
				li.wri.Flush()
				li.file.Close()
				break
			}
		}
	}()
	return li, nil
}

func writeLogFileHeader(file *os.File) error {
	fileHeader := []byte{
		0x77, 0x88, //magic nubmer
		0x01, 0x01, //version 1, 1
		0x01, 0x01, //file type: 0x0101 - log file,
		0x00, 0x00, //options
	}
	_, err := file.Write(fileHeader)
	return err
}
