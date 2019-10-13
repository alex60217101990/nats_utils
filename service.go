package nats_utils

import (
	"log"
	"time"

	"github.com/eapache/channels"
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

const (
	DefaultMaxPubAcksInflight = 1000
	DefaultMsgChanSize        = 500
)

type StreamingService interface {
	Connect()
	RunPublisher()
	RunSubscriber(config SubscriberConfig) error
	PublisherPush(subject string, data []byte)
	Close()
}

type StorageSubscribeLast interface {
	SaveLastMessageInfo(key string, lastMsgInfo MsgSubInfo) error
}

type MsgSubInfo struct {
	Subject  string    `json:"subject"`
	Queue    *string   `json:"queue,omitempty"`
	Sequence uint      `json:"sequence"`
	Time     time.Time `json:"time"`
}

type MsgEvent struct {
	Subject string
	Body    []byte
}

type Config struct {
	Async                bool
	ClusterID            *string
	MsgChannelBufferSize *int
	MaxPubAcksInflight   *int
	Compress             bool
	Logger               *log.Logger
	Options              *nats.Options
}

type SubscriberConfig struct {
	Type                         SubscribeType
	StartType                    SubscribeStartType
	QueueName                    *string
	SubMsgChan                   *channels.RingChannel
	UnsubscribeAfterSubscribeEnd bool
	ChannelName                  *string
	MsgHandler                   func(m *stan.Msg)
	LastMsgStorage               StorageSubscribeLast
	MaxInflight                  *int
	AckWaitSeconds               *int
	DurableName                  *string    // only for 'Durable' subscribes or queues
	StartMsgPosition             *int       // only for 'StartAtSequence' type
	StartTime                    *time.Time // only for 'StartAtTime' type
}

type SubscribeType int

const (
	ClassicSubscribe SubscribeType = (1 + iota)
	QueueSubscribe
)

func (s SubscribeType) String() string {
	names := [...]string{
		"Classic NATS subscribe",
		"Queue NATS subscribe",
	}
	if s < ClassicSubscribe || s > QueueSubscribe {
		return "unknown"
	}
	return names[s-1]
}

func (s SubscribeType) Val() int {
	return int(s)
}

type SubscribeStartType int

const (
	StartWithLastReceived SubscribeStartType = (1 + iota) // Последнее полученное сообщение
	DeliverAllAvailable                                   // Начало канала
	StartAtSequence                                       // Конкретное сообщение, индексация начинается с 1
	StartAtTime                                           // Конкретное время, когда сообщение пришло на канал
)

func (t SubscribeStartType) String() string {
	names := [...]string{
		"The last message received",
		"The beginning of the channel",
		"A specific message, indexing starts at 1",
		"A specific time the message arrived in the channel",
	}
	if t < StartWithLastReceived || t > StartAtTime {
		return "unknown"
	}
	return names[t-1]
}

func (t SubscribeStartType) Val() int {
	return int(t)
}
