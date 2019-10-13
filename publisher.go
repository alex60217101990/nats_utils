package nats_utils

import (
	"log"
	"runtime/debug"

	"github.com/golang/snappy"
)

func (n *NatsStreamingService) RunPublisher() {
	var event MsgEvent

	if n.configs.Async {
		ackHandler := func(lguid string, err error) {
			if err != nil {
				if n.configs.Logger != nil {
					n.configs.Logger.Printf("nats_:_AckHandler: error: Publisher %v got following error: %v, stack: %s\n", n.pubID, err, string(debug.Stack()))
				} else {
					log.Printf("nats_:_AckHandler: error: Publisher %v got following error: %v, stack: %s\n", n.pubID, err, string(debug.Stack()))
				}
			}
		}
		go func() {
		PubLoop:
			for {
				select {
				case msg, ok := <-n.pubMsgChan.Out():
					if ok {
						if event, ok = msg.(MsgEvent); ok {
							if n.configs.Compress {
								event.Body = snappy.Encode(nil, event.Body)
							}
							_, err := n.conn.PublishAsync(event.Subject, event.Body, ackHandler)
							if err != nil {
								if n.configs.Logger != nil {
									n.configs.Logger.Printf("nats_:_RunPublisher: error: Publisher async %v pushing message error: %v, stack: %s\n", n.pubID, err, string(debug.Stack()))
								} else {
									log.Printf("nats_:_RunPublisher: error: Publisher async %v pushing message error: %v, stack: %s\n", n.pubID, err, string(debug.Stack()))
								}
								break PubLoop
							}
						}
					}
				case <-n.ctx.Done():
					break PubLoop
				}
			}
		}()
	} else {
		go func() {
		PubLoop:
			for {
				select {
				case msg, ok := <-n.pubMsgChan.Out():
					if ok {
						if event, ok = msg.(MsgEvent); ok {
							if n.configs.Compress {
								event.Body = snappy.Encode(nil, event.Body)
							}
							err := n.conn.Publish(event.Subject, event.Body)
							if err != nil {
								if n.configs.Logger != nil {
									n.configs.Logger.Printf("nats_:_RunPublisher: error: Publisher %v pushing message error: %v, stack: %s\n", n.pubID, err, string(debug.Stack()))
								} else {
									log.Printf("nats_:_RunPublisher: error: Publisher %v pushing message error: %v, stack: %s\n", n.pubID, err, string(debug.Stack()))
								}
								break PubLoop
							}
						}
					}
				case <-n.ctx.Done():
					break PubLoop
				}
			}
		}()
	}
}

func (n *NatsStreamingService) PublisherPush(subject string, data []byte) {
	n.pubMsgChan.In() <- MsgEvent{
		Subject: subject,
		Body:    data,
	}
}
