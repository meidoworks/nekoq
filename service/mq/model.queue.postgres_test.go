package mq_test

import (
	"database/sql"
	"encoding/binary"
	"testing"

	"goimport.moetang.info/nekoq/service/mq"

	_ "github.com/jackc/pgx/stdlib"
)

var _ mq.QueueType = new(pgQueueType)

type pgQueueType struct {
	ConnString string

	db            *sql.DB
	publishStmt   *sql.Stmt
	getMsgStmt    *sql.Stmt
	deleteMsgStmt *sql.Stmt

	queueId mq.IdType
}

type PgQueueOption struct {
	ConnString string
}

func NewPgQueueType(option *PgQueueOption) (mq.QueueType, error) {
	pg := new(pgQueueType)
	pg.ConnString = option.ConnString

	return pg, nil
}

func convertToBytes(id mq.IdType) []byte {
	idSlice := make([]byte, 16)
	binary.BigEndian.PutUint64(idSlice[0:8], uint64(id[0]))
	binary.BigEndian.PutUint64(idSlice[8:16], uint64(id[1]))
	return idSlice
}

func convertFromBytes(idBytes []byte) mq.IdType {
	result := mq.IdType{}
	result[0] = int64(binary.BigEndian.Uint64(idBytes[0:8]))
	result[1] = int64(binary.BigEndian.Uint64(idBytes[8:16]))
	return result
}

func (this *pgQueueType) PublishMessage(req *mq.Request, ctx *mq.Ctx) error {
	topic := req.Header.TopicId
	queue := this.queueId

	for _, v := range req.BatchMessage {
		msgid := v.MsgId
		data := v.Body

		msgIdSlice := convertToBytes(msgid)
		topicSlice := convertToBytes(topic)
		queueSlice := convertToBytes(queue)

		_, err := this.publishStmt.Exec(msgIdSlice, topicSlice, queueSlice, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *pgQueueType) CommitMessages(reg *mq.MessageCommit, ctx *mq.Ctx) error {
	return nil
}

func (this *pgQueueType) CreateRecord(subscribeGroupId mq.IdType, ctx *mq.Ctx) (*mq.QueueRecord, error) {
	//TODO
	return nil, nil
}

func (this *pgQueueType) BatchObtain(record *mq.QueueRecord, maxCnt int, ctx *mq.Ctx) (mq.BatchObtainResult, error) {
	rows, err := this.getMsgStmt.Query(convertToBytes(this.queueId))
	if err != nil {
		return mq.BatchObtainResult{}, err
	}
	result := mq.BatchObtainResult{
		Requests: []*mq.Request{
			{},
		},
	}
	req := result.Requests[0]
	var topicSlice, queueSlice, msgIdSlice, data []byte
	for rows.Next() {
		err = rows.Scan(&msgIdSlice, &queueSlice, &topicSlice, &data)
		if err != nil {
			return result, err
		}
		req.Header.TopicId = convertFromBytes(topicSlice)
		req.BatchMessage = append(req.BatchMessage, mq.Message{
			MessageId: mq.MessageId{
				MsgId: convertFromBytes(msgIdSlice),
			},
			Body: data[:],
		})
	}

	if len(req.BatchMessage) == 0 {
		return result, nil
	}

	_, err = this.deleteMsgStmt.Exec(msgIdSlice)
	if err != nil {
		return result, err
	}

	return result, nil
}

func (this *pgQueueType) ConfirmConsumed(record *mq.QueueRecord, ack *mq.Ack) error {
	//TODO
	return nil
}

func (this *pgQueueType) Init(queue *mq.Queue, option *mq.QueueOption) error {
	this.queueId = queue.QueueID

	db, err := sql.Open("pgx", this.ConnString)
	if err != nil {
		return err
	}
	this.db = db
	this.db.SetMaxOpenConns(100)

	publishStmt, err := db.Prepare(`INSERT INTO "message"("msgid", "topic", "queue", "data") VALUES ($1, $2, $3, $4)`)
	if err != nil {
		return err
	}
	this.publishStmt = publishStmt

	//getMsgStat, err := db.Prepare(`SELECT "msgid", "topic", "queue", "data" FROM "message" WHERE "id" > (SELECT "id" FROM "message" WHERE "msgid" = $1) AND "queue" = $2 LIMIT 10`)
	getMsgStat, err := db.Prepare(`SELECT "msgid", "topic", "queue", "data" FROM "message" WHERE "queue" = $1 LIMIT 10`)
	if err != nil {
		return err
	}
	this.getMsgStmt = getMsgStat

	deleteMsgStat, err := db.Prepare(`DELETE FROM "message" WHERE "id" <= (SELECT "id" FROM "message" WHERE "msgid" = $1)`)
	if err != nil {
		return err
	}
	this.deleteMsgStmt = deleteMsgStat

	return nil
}

func TestNewPgQueueType(t *testing.T) {
	option := &PgQueueOption{
		ConnString: "user=dev password=dev host=localhost port=5432 database=mq sslmode=disable",
	}

	q, err := NewPgQueueType(option)
	if err != nil {
		t.Fatal(err)
	}

	err = q.Init(&mq.Queue{
		QueueID: mq.IdType{1, 1},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	req := &mq.Request{
		Header: mq.Header{
			TopicId: mq.IdType{2, 2},
		},
		BatchMessage: []mq.Message{
			{
				MessageId: mq.MessageId{
					MsgId: mq.IdType{1, 12},
				},
				Body: make([]byte, 200),
			},
		},
	}

	err = q.PublishMessage(req, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = q.BatchObtain(nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
}
