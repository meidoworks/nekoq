package simplemq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type DbConfig struct {
	ConnString string
}

func InitDb(appCtx *AppCtx, dbCfg *DbConfig) error {
	toc, _ := context.WithTimeout(context.Background(), 10*time.Second)
	p, err := pgxpool.Connect(toc, dbCfg.ConnString)
	if err != nil {
		return err
	}
	appCtx.pool = p
	return nil
}

func LoadTopicQueueMapping(appCtx *AppCtx) (map[string]map[string]TopicQueueMapping, error) {
	toc, _ := context.WithTimeout(context.Background(), 10*time.Second)
	rows, err := appCtx.pool.Query(toc, "select mapping_id, topic, queue_name from simple_topic_queue_mapping")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	mapping := make(map[string]map[string]TopicQueueMapping)
	for rows.Next() {
		t := TopicQueueMapping{}
		err = rows.Scan(&t.MappingId, &t.Topic, &t.QueueName)
		if err != nil {
			return nil, err
		}
		topicMap, ok := mapping[t.Topic]
		if !ok {
			topicMap = make(map[string]TopicQueueMapping)
			mapping[t.Topic] = topicMap
		}
		queueItem, ok := topicMap[t.QueueName]
		if ok {
			return nil, errors.New("duplicated queue name in topic")
		}
		queueItem = t
		topicMap[t.QueueName] = queueItem
	}
	return mapping, nil
}

func SaveMessages(appCtx *AppCtx, messages []*Message) error {
	now := time.Now()
	createdTime := now.Unix()*1000 + now.UnixNano()/10000000

	err := appCtx.pool.BeginTxFunc(context.Background(), pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	}, func(tx pgx.Tx) error {
		for _, v := range messages {
			_, err := tx.Exec(context.Background(), "insert into simple_msg(msg_status, msg_deleted, queue_name, header, payload, time_created, time_delivered, schedule_time, topic) values($1, $2, $3, $4, $5, $6, $7, $8, $9)",
				v.MsgStatus, v.MsgDeleted, v.QueueName, v.Header, v.Payload, createdTime, nil, createdTime, v.Topic)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	} else {
		return nil
	}
}

func QueryMessage(appCtx *AppCtx, request *MessageRequest) (*Message, error) {
	now := time.Now()
	createdTime := now.Unix()*1000 + now.UnixNano()/10000000

	var r *Message

	err := appCtx.pool.BeginTxFunc(context.Background(), pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	}, func(tx pgx.Tx) error {

		rows, err := tx.Query(context.Background(), "select msg_id, msg_status, msg_deleted, topic, queue_name, header, payload, time_created, time_delivered, schedule_time from simple_msg where msg_deleted = 0 and queue_name = $1 and schedule_time <= $2 limit 1",
			request.QueueName, createdTime)
		if err != nil {
			return err
		}
		defer rows.Close()

		if rows.Next() {
			r = new(Message)
			err := rows.Scan(&r.MsgId, &r.MsgStatus, &r.MsgDeleted, &r.Topic, &r.QueueName, &r.Header, &r.Payload, &r.TimeCreated, &r.TimeDelivered, &r.ScheduleTime)
			if err != nil {
				return err
			}
			rows.Close()
			_, err = tx.Exec(context.Background(), "update simple_msg set msg_status = 1, schedule_time=$2 where msg_id = $1",
				r.MsgId, fmt.Sprint(createdTime+RESCHEDULE_INTERVAL))
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	} else {
		return r, nil
	}
}

func AckMessage(appCtx *AppCtx, request *MessageAckRequest) (bool, error) {
	_, err := appCtx.pool.Exec(context.Background(), "update simple_msg set msg_status=3, msg_deleted=1 where msg_id=$1", request.ReqId)
	if err != nil {
		return false, err
	} else {
		return true, nil
	}
}
