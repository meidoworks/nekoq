package discovery

import (
	"time"

	"github.com/meidoworks/nekoq/shared/logging"
)

var _peerLogger = logging.NewLogger("Peer")

const (
	_peerScheduleLoopInterview = 1
	_peerSyncTTL               = 20
	_peerTTL                   = 60
)

type PeerState int

const (
	PeerStateInit = iota + 1
	PeerStateFull
	PeerStateExpired
)

type Peer struct {
	state           PeerState
	stateTimeStamp  time.Time
	lastSyncVersion string

	peerId      int16
	peerWrapper PeerService

	dataStore *DataStore
}

func StartPeer(peerWrapper PeerService, peerId int16, stor *DataStore) (*Peer, error) {
	peer := new(Peer)

	peer.state = PeerStateInit
	peer.stateTimeStamp = time.Now()

	peer.peerId = peerId
	peer.peerWrapper = peerWrapper

	peer.dataStore = stor

	go peer.ScheduleLoop()

	return peer, nil
}

func (p *Peer) ScheduleLoop() {
	// trigger interval
	ticker := time.NewTicker(_peerScheduleLoopInterview * time.Second)
	for now := range ticker.C {
		switch p.state {
		case PeerStateInit:
			f, err := p.peerWrapper.FullFetch()
			if err != nil {
				_peerLogger.Errorf("FullFetch [%d] failed: %s", p.peerId, err)
				continue
			}
			if err := p.dataStore.InitPeer(p.peerId, f); err != nil {
				_peerLogger.Errorf("InitPeer [%d] failed: %s", p.peerId, err)
				continue
			}
			p.state = PeerStateFull
			p.stateTimeStamp = time.Now()
			p.lastSyncVersion = f.CurrentVersion
		case PeerStateFull:
			if now.Sub(p.stateTimeStamp) > _peerSyncTTL*time.Second { // ttl after no successful update
				p.state = PeerStateExpired
				p.stateTimeStamp = time.Now()
				_peerLogger.Errorln("Peer state sync expired. Turning into expired state.")
				continue
			}
			inc, err := p.peerWrapper.IncrementalFetch(p.lastSyncVersion)
			if err != nil {
				_peerLogger.Errorf("IncrementalFetch [%d] failed: %s", p.peerId, err)
				continue
			}
			if inc.ReSync {
				// force re-sync
				_peerLogger.Warnf("LastVersion is expired. Need force ReSync for peer:[%d]", p.peerId)
				p.state = PeerStateExpired
				p.stateTimeStamp = time.Now()
				continue
			}
			if err := p.dataStore.UpdatePeer(p.peerId, inc); err != nil {
				_peerLogger.Errorf("UpdatePeer [%d] failed: %s", p.peerId, err)
				continue
			}
			p.stateTimeStamp = time.Now()
			p.lastSyncVersion = inc.CurrentVersion
		case PeerStateExpired:
			if now.Sub(p.stateTimeStamp) > _peerTTL*time.Second {
				// cleanup dead peer after ttl
				if err := p.dataStore.CleanupPeer(p.peerId); err != nil {
					_peerLogger.Errorf("CleanupPeer [%d] failed: %s", p.peerId, err)
				} else {
					p.stateTimeStamp = time.Now()
				}
			}
			f, err := p.peerWrapper.FullFetch()
			if err != nil {
				_peerLogger.Errorf("FullFetch [%d] in expired state failed: %s", p.peerId, err)
				continue
			}
			if err := p.dataStore.InitPeer(p.peerId, f); err != nil {
				_peerLogger.Errorf("InitPeer [%d] in expired state failed: %s", p.peerId, err)
				continue
			}
			p.state = PeerStateFull
			p.stateTimeStamp = time.Now()
			p.lastSyncVersion = f.CurrentVersion
		default:
			// immediately reset state to init and log
			_peerLogger.Errorf("Reached unknown state [%d] of peer [%d]", p.state, p.peerId)
			p.state = PeerStateInit
			p.stateTimeStamp = time.Now()
			p.lastSyncVersion = ""
		}
	}
}
