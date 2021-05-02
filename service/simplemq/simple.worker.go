package simplemq

import "log"

func (this *SimpleMq) QueryMessageWorker() {
LOOP:
	for {
		select {
		case <-this.loadTopicQueueMappingCh:
			break LOOP
		case r := <-this.AppCtx.SubMessageRequestQueue:
			msg, err := QueryMessage(this.AppCtx, r)
			if this.AppCtx.DebugOutput {
				log.Println("query message:", msg, err)
			}
			if err != nil {
				log.Println("[ERROR] query message error:", err)
				close(r.RespCh)
				continue
			}
			if msg == nil {
				close(r.RespCh)
			} else {
				r.RespCh <- msg
			}
		case r := <-this.AppCtx.SubAckMessageRequestQueue:
			ok, err := AckMessage(this.AppCtx, r)
			if this.AppCtx.DebugOutput {
				log.Println("ack message:", ok, err)
			}
			if err != nil {
				log.Println("[ERROR] query message error:", err)
				close(r.RespCh)
				continue
			}
			if ok {
				r.RespCh <- struct{}{}
			} else {
				close(r.RespCh)
			}
		}
	}
}
