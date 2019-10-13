# nats_utils

## Installation

Run command on you [$GOPATH/src] path:

```bash
go get -u github.com/alex60217101990/nats_utils
```

## Usage

Create service:
```go
	natsServer := NewStreamingService(
		SetContext(ctx),
		SetConfigs(&Config{
			Async:                true,
			ClusterID:            &ClusterID,
			MsgChannelBufferSize: &MsgChannelBufferSize,
			MaxPubAcksInflight:   &MaxPubAcksInflight,
			Compress:             true,
			Logger:               nil,
			Options: &nats_streaming.Options{
				Servers:  servers,
				Secure:   false,
				User:     "some_user",
				Password: "some_password",
			},
		}),
	)
```
Connect with NATS single node or cluster:
```go
    natsServer.Connect()
```
# Publishing:
Run publisher loop:
```go
    go natsServer.RunPublisher() 
```
Publish message to channel:
```go
    natsServer.PublisherPush("test_channel", []byte(fmt.Sprintf("test_message: %v", t)))
```
# Subscribing:
Usage with handler function:
```go
    natsServer.RunSubscriber(SubscriberConfig{
		Type:        ClassicSubscribe,
		StartType:   DeliverAllAvailable,
		SubMsgChan:  subMsgChan,
		ChannelName: &ChannelName,
		MsgHandler: func(m *stan.Msg) {
			//some action with current message...
		},
		MaxInflight:    &MaxPubAcksInflight,
		AckWaitSeconds: &AckWaitSeconds,
		DurableName:    &DurableName,
    })
```
Usage with ring buffering channel:
```go
    subMsgChan := channels.NewRingChannel(channels.BufferCap(500))
    defer subMsgChan.Close()
    natsServer.RunSubscriber(SubscriberConfig{
		Type:        ClassicSubscribe,
		StartType:   DeliverAllAvailable,
		SubMsgChan:  subMsgChan,
		ChannelName: &ChannelName,
		MaxInflight:    &MaxPubAcksInflight,
		AckWaitSeconds: &AckWaitSeconds,
		DurableName:    &DurableName,
    })

    for {
		select {
		case m := <-subMsgChan.Out():
			//some action with current message...
		}
	}
```

## License
[MIT](https://choosealicense.com/licenses/mit/)


