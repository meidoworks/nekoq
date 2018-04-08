package lib

import (
	"net/http"

	"goimport.moetang.info/nekoq/service/mq"
)

type Manager struct {
	broker *mq.Broker
}

func (this *Manager) manageTopic(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET": // find
	case "PUT": // create
	case "DELETE": // delete
	}
}

func (this *Manager) manageQueue(w http.ResponseWriter, r *http.Request) {

}

func (this *Manager) managePublishGroup(w http.ResponseWriter, r *http.Request) {

}

func (this *Manager) manageSubscribeGroup(w http.ResponseWriter, r *http.Request) {

}

func (this *Manager) manageTopicQueueBinding(w http.ResponseWriter, r *http.Request) {

}

func (this *Manager) managePublishGroupTopicBinding(w http.ResponseWriter, r *http.Request) {

}

func (this *Manager) manageSubscribeGroupQueueBinding(w http.ResponseWriter, r *http.Request) {

}
