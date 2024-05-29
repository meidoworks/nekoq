package leader

import (
	"fmt"
	"time"

	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/inproc"
)

type Leader interface {
	Leader() string
	PreemptLeader() error
	IsLeader() bool
	NotifyRoleChange() <-chan bool
}

const (
	cfgClusterLeaderPath = "/nekoq/mq_leader/%s"
)

type LeaderImpl struct {
	nodeName   string
	leader     string
	leaderPath string

	notify chan bool
}

func NewLeader(node string) (*LeaderImpl, error) {
	if len(config.Instance.MQ.ClusterName) == 0 {
		return nil, fmt.Errorf("cluster name is empty")
	}
	leader := &LeaderImpl{
		nodeName:   node,
		notify:     make(chan bool, 1024),
		leaderPath: fmt.Sprintf(cfgClusterLeaderPath, config.Instance.MQ.ClusterName),
	}

	return leader, nil
}

func (l *LeaderImpl) IsLeader() bool {
	return l.nodeName == l.leader
}

func (l *LeaderImpl) Leader() string {
	return l.leader
}

func (l *LeaderImpl) PreemptLeader() error {
	prevLeader := false
	for {
		time.Sleep(1 * time.Second) // try to preempt leader every second
		n, err := inproc.WarehouseInst.AcquireLeader(l.leaderPath, l.nodeName)
		if err != nil {
			//FIXME log error information
			continue
		}
		l.leader = n
		if l.IsLeader() {
			if !prevLeader {
				prevLeader = true
				select {
				case l.notify <- true:
				default:
				}
			}
		} else {
			if prevLeader {
				prevLeader = false
				select {
				case l.notify <- false:
				default:
				}
			}
		}
	}
}

func (l *LeaderImpl) NotifyRoleChange() <-chan bool {
	return l.notify
}
