package nats_utils

import (
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/golang/snappy"
	stan "github.com/nats-io/stan.go"
)

func (n *NatsStreamingService) RunSubscriber(config SubscriberConfig) error {
	var opts []stan.SubscriptionOption
	var sub stan.Subscription
	var err error

	switch config.StartType {
	case StartWithLastReceived:
		opts = append(opts, stan.StartWithLastReceived())
	case DeliverAllAvailable:
		opts = append(opts, stan.DeliverAllAvailable())
	case StartAtSequence:
		opts = append(opts, stan.StartAtSequence(uint64(*config.StartMsgPosition)))
	case StartAtTime:
		opts = append(opts, stan.StartAtTime(*config.StartTime))
	}
	if config.MaxInflight != nil {
		opts = append(opts, stan.MaxInflight(*config.MaxInflight))
	}
	if config.AckWaitSeconds != nil {
		opts = append(opts, stan.AckWait(time.Duration(*config.AckWaitSeconds)*time.Second))
	}
	if config.DurableName != nil {
		opts = append(opts, stan.DurableName(*config.DurableName))
	}
	if config.Type == ClassicSubscribe {
		sub, err = n.conn.Subscribe(*config.ChannelName, func(m *stan.Msg) {
			var err error
			if n.configs.Compress {
				m.Data, err = snappy.Decode(nil, m.Data)
			}
			if err != nil {
				if n.configs.Logger != nil {
					n.configs.Logger.Printf("nats_:_Subscribe_:_[channel: %s] error: %v, stack: %s\n", *config.ChannelName, err, string(debug.Stack()))
				} else {
					log.Printf("nats_:_Subscribe_:_[channel: %s] error: %v, stack: %s\n", *config.ChannelName, err, string(debug.Stack()))
				}
				return
			}
			if config.MsgHandler != nil {
				// client function...
				config.MsgHandler(m)
			}
			if config.LastMsgStorage != nil {
				err = config.LastMsgStorage.SaveLastMessageInfo(*config.ChannelName,
					MsgSubInfo{
						Subject:  *config.ChannelName,
						Sequence: uint(m.Sequence),
						Time:     time.Unix(0, m.Timestamp),
					})
				if err != nil {
					if n.configs.Logger != nil {
						n.configs.Logger.Printf("nats_:_Subscribe_:_[channel: %s] error: %v, stack: %s\n", *config.ChannelName, err, string(debug.Stack()))
					} else {
						log.Printf("nats_:_Subscribe_:_[channel: %s] error: %v, stack: %s\n", *config.ChannelName, err, string(debug.Stack()))
					}
				}
			} else {
				n.cache.Set(*config.ChannelName, MsgSubInfo{
					Subject:  *config.ChannelName,
					Sequence: uint(m.Sequence),
					Time:     time.Unix(0, m.Timestamp),
				}, -1)
			}
			if config.SubMsgChan != nil {
				config.SubMsgChan.In() <- m.Data
			}
		}, opts...)
	} else if config.Type == QueueSubscribe {
		sub, err = n.conn.QueueSubscribe(*config.ChannelName, *config.QueueName, func(m *stan.Msg) {
			var err error
			if n.configs.Compress {
				m.Data, err = snappy.Decode(nil, m.Data)
			}
			if err != nil {
				if n.configs.Logger != nil {
					n.configs.Logger.Printf("nats_:_QueueSubscribe_:_[channel: %s, queue: %s] error: %v, stack: %s\n", *config.ChannelName, *config.QueueName, err, string(debug.Stack()))
				} else {
					log.Printf("nats_:_QueueSubscribe_:_[channel: %s, queue: %s] error: %v, stack: %s\n", *config.ChannelName, *config.QueueName, err, string(debug.Stack()))
				}
				return
			}
			if config.MsgHandler != nil {
				// client function...
				config.MsgHandler(m)
			}
			if config.LastMsgStorage != nil {
				err = config.LastMsgStorage.SaveLastMessageInfo(fmt.Sprintf("%s:%s", *config.ChannelName, *config.QueueName),
					MsgSubInfo{
						Subject:  *config.ChannelName,
						Queue:    config.QueueName,
						Sequence: uint(m.Sequence),
						Time:     time.Unix(0, m.Timestamp),
					})
				if err != nil {
					if n.configs.Logger != nil {
						n.configs.Logger.Printf("nats_:_QueueSubscribe_:_[channel: %s, queue: %s] error: %v, stack: %s\n", *config.ChannelName, *config.QueueName, err, string(debug.Stack()))
					} else {
						log.Printf("nats_:_QueueSubscribe_:_[channel: %s, queue: %s] error: %v, stack: %s\n", *config.ChannelName, *config.QueueName, err, string(debug.Stack()))
					}
				}
			} else {
				n.cache.Set(fmt.Sprintf("%s:%s", *config.ChannelName, *config.QueueName), MsgSubInfo{
					Subject:  *config.ChannelName,
					Queue:    config.QueueName,
					Sequence: uint(m.Sequence),
					Time:     time.Unix(0, m.Timestamp),
				}, -1)
			}
			if config.SubMsgChan != nil {
				config.SubMsgChan.In() <- m.Data
			}
		}, opts...)
	}
	defer func() {
		if config.UnsubscribeAfterSubscribeEnd {
			sub.Unsubscribe()
			config.SubMsgChan.Close()
		}
	}()
	return err
}
