package impl_test

import (
	"fmt"
	"testing"
)

import (
	"github.com/meidoworks/nekoq/service/naming/impl"
)

func TestIfNewLogWork(t *testing.T) {
	l, err := impl.NewLog("./logfile")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("file: ", l)
	entry := &impl.LogRecord{
		Data: []byte("helloworld123123\n"),
	}
	l.WriteLogRecord(entry)
	l.Close()
}

func TestReadLog(t *testing.T) {
	err := impl.ProcessLogFile("./logfile", LogReaderCallback{})
	if err != nil {
		t.Fatal(err)
	}
}

type LogReaderCallback struct {
}

func (LogReaderCallback) OnLogRecord(record *impl.LogRecord) error {
	fmt.Println(string(record.Data))
	return nil
}

func (LogReaderCallback) OnError(err error) {
	fmt.Println("err occurs:", err)
}

func (LogReaderCallback) OnFinish() {
	fmt.Println("finish")
}
