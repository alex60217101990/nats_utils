package nats_utils

import (
	"context"
	"log"
	"runtime/debug"

	"github.com/eapache/channels"
	"github.com/google/uuid"
	stan "github.com/nats-io/stan.go"
	cache "github.com/patrickmn/go-cache"
)

type NatsStreamingService struct {
	configs    *Config
	conn       stan.Conn
	pubID      string
	pubMsgChan *channels.RingChannel
	ctx        context.Context
	cache      *cache.Cache
}

func NewStreamingService(options ...func(*NatsStreamingService) error) *NatsStreamingService {
	service := &NatsStreamingService{
		cache: cache.New(-1, -1),
	}
	for _, op := range options {
		err := op(service)
		if err != nil {
			if service.configs.Logger != nil {
				service.configs.Logger.Fatalf("nats_:_NewStreamingService: error: %+v, stack: %s\n", err, string(debug.Stack()))
			} else {
				log.Fatalf("nats_:_NewStreamingService: error: %+v, stack: %s\n", err, string(debug.Stack()))
			}
		}
	}
	if service.configs != nil && service.configs.MsgChannelBufferSize != nil {
		service.pubMsgChan = channels.NewRingChannel(channels.BufferCap(*service.configs.MsgChannelBufferSize))
	} else {
		service.pubMsgChan = channels.NewRingChannel(channels.BufferCap(DefaultMsgChanSize))
	}
	if len(service.pubID) == 0 {
		uuid, err := uuid.NewUUID()
		if err != nil {
			if service.configs.Logger != nil {
				service.configs.Logger.Fatalf("nats_:_NewStreamingService: error: %+v, stack: %s\n", err, string(debug.Stack()))
			} else {
				log.Fatalf("nats_:_NewStreamingService: error: %+v, stack: %s\n", err, string(debug.Stack()))
			}
		}
		service.pubID = uuid.String()
	}
	return service
}

func SetConfigs(conf *Config) func(*NatsStreamingService) error {
	return func(service *NatsStreamingService) error {
		service.configs = conf
		return nil
	}
}

func SetContext(ctx context.Context) func(*NatsStreamingService) error {
	return func(service *NatsStreamingService) error {
		service.ctx = ctx
		return nil
	}
}

func SetPubID(pubID string) func(*NatsStreamingService) error {
	return func(service *NatsStreamingService) error {
		service.pubID = pubID
		return nil
	}
}

func (n *NatsStreamingService) Connect() {
	var err error

	// Setup the connect options
	n.configs.Options = n.changePubOptions(n.configs.Options)

	nc, err := n.configs.Options.Connect()
	if err != nil {
		if n.configs.Logger != nil {
			n.configs.Logger.Fatalf("nats_:_Connect: error: %+v, stack: %s\n", err, string(debug.Stack()))
		} else {
			log.Fatalf("nats_:_Connect: error: %+v, stack: %s\n", err, string(debug.Stack()))
		}
	}
	var maxPubAcksInflight int
	if n.configs != nil && n.configs.MaxPubAcksInflight != nil {
		maxPubAcksInflight = *n.configs.MaxPubAcksInflight
	} else {
		maxPubAcksInflight = DefaultMaxPubAcksInflight
	}
	n.conn, err = stan.Connect(*n.configs.ClusterID, n.pubID, stan.MaxPubAcksInflight(maxPubAcksInflight), stan.NatsConn(nc),
		stan.SetConnectionLostHandler(func(conn stan.Conn, reason error) {
			if n.configs.Logger != nil {
				n.configs.Logger.Fatalf("nats_:_Connect: error: Connection lost, reason: %+v, stats: %v, stack: %s\n", err, conn.NatsConn().Statistics, string(debug.Stack()))
			} else {
				log.Fatalf("nats_:_Connect: error: Connection lost, reason: %+v, stats: %v, stack: %s\n", err, conn.NatsConn().Statistics, string(debug.Stack()))
			}
		}))
	if err != nil {
		if n.configs.Logger != nil {
			n.configs.Logger.Fatalf("nats_:_Connect: error: Publisher %s can't connect: %v, stack: %s\n", n.pubID, err, string(debug.Stack()))
		} else {
			log.Fatalf("nats_:_Connect: error: Publisher %s can't connect: %v, stack: %s\n", n.pubID, err, string(debug.Stack()))
		}
	}
}

func (n *NatsStreamingService) Close() {
	n.conn.Close()
	n.pubMsgChan.Close()
}
