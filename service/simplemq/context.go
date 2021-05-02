package simplemq

import (
	"net/http"

	"github.com/jackc/pgx/v4/pgxpool"
)

type AppCtx struct {
	pool       *pgxpool.Pool
	httpServer *http.Server

	mapping map[string]map[string]TopicQueueMapping

	SubMessageRequestQueue    chan *MessageRequest
	SubAckMessageRequestQueue chan *MessageAckRequest

	DebugOutput bool
}
