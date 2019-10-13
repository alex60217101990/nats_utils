package nats_utils

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eapache/channels"
	"github.com/fatih/color"
	nats_streaming "github.com/nats-io/nats.go"
)

func TestPubSub(test *testing.T) {
	outY := color.New(color.FgHiYellow, color.Faint)
	outM := color.New(color.FgHiCyan, color.Faint)

	ClusterID := "test-cluster"
	MsgChannelBufferSize := 500
	MaxPubAcksInflight := 1000
	ChannelName := "test_channel"
	AckWaitSeconds := 5
	DurableName := "test_durable"

	d := time.Now().Add(10 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	servers := []string{nats_streaming.DefaultURL}
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
	natsServer.Connect()
	// defer natsServer.Close()
	go natsServer.RunPublisher()
	go func() {
		intervalDump := time.Duration(1) * time.Second
		tickerDump := time.NewTicker(intervalDump)
		defer tickerDump.Stop()
	Exit:
		for {
			select {
			case t := <-tickerDump.C:
				natsServer.PublisherPush("test_channel", []byte(fmt.Sprintf("test_message: %v", t)))
				outY.Println(fmt.Sprintf("test_message: %v", t))
			case <-ctx.Done():
				break Exit
			}
		}
	}()

	subMsgChan := channels.NewRingChannel(channels.BufferCap(500))
	defer subMsgChan.Close()
	natsServer.RunSubscriber(SubscriberConfig{
		Type:        ClassicSubscribe,
		StartType:   DeliverAllAvailable,
		SubMsgChan:  subMsgChan,
		ChannelName: &ChannelName,
		/*MsgHandler: func(m *stan.Msg) {
			custom_logger.CmdError.Println(string(m.Data))
		},*/
		MaxInflight:    &MaxPubAcksInflight,
		AckWaitSeconds: &AckWaitSeconds,
		DurableName:    &DurableName,
	})

	for {
		select {
		case <-ctx.Done():
			return
		case m := <-subMsgChan.Out():
			outM.Println(string(m.([]byte)))
		}
	}
}
