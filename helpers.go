package nats_utils

import (
	"log"
	"runtime/debug"

	"github.com/google/uuid"
	nats "github.com/nats-io/nats.go"
)

func (n *NatsStreamingService) changePubOptions(options *nats.Options) *nats.Options {
	opts := nats.GetDefaultOptions()
	if len(options.Url) > 0 {
		opts.Url = options.Url
	}
	if options.Servers != nil {
		opts.Servers = options.Servers
	}
	opts.NoRandomize = options.NoRandomize
	opts.NoEcho = options.NoEcho
	if len(options.Name) == 0 {
		uuid, err := uuid.NewUUID()
		if err != nil {
			if n.configs.Logger != nil {
				n.configs.Logger.Printf("nats_:_changePubOptions: error: %+v, stack: %s\n", err, string(debug.Stack()))
			} else {
				log.Printf("nats_:_changePubOptions: error: %+v, stack: %s\n", err, string(debug.Stack()))
			}
		}
		opts.Name = uuid.String()
	} else {
		opts.Name = options.Name
	}
	opts.Verbose = options.Verbose
	opts.Pedantic = options.Pedantic
	opts.Secure = options.Secure
	opts.TLSConfig = options.TLSConfig
	if options.MaxReconnect > 0 {
		opts.MaxReconnect = options.MaxReconnect
	}
	if options.ReconnectWait > 0 {
		opts.ReconnectWait = options.ReconnectWait
	}
	if options.Timeout > 0 {
		opts.Timeout = options.Timeout
	}
	if options.PingInterval > 0 {
		opts.PingInterval = options.PingInterval
	}
	if options.MaxPingsOut > 0 {
		opts.MaxPingsOut = options.MaxPingsOut
	}
	if options.SubChanLen > 0 {
		opts.SubChanLen = options.SubChanLen
	}
	if options.ReconnectBufSize > 0 {
		opts.ReconnectBufSize = options.ReconnectBufSize
	}
	if options.DrainTimeout > 0 {
		opts.DrainTimeout = options.DrainTimeout
	}
	if options.AsyncErrorCB != nil {
		opts.AsyncErrorCB = options.AsyncErrorCB
	} else {
		opts.AsyncErrorCB = func(conn *nats.Conn, sub *nats.Subscription, err error) {
			if n.configs.Logger != nil {
				n.configs.Logger.Printf("NATS_:_AsyncErrorCB_: stats: %+v, error: %+v\n", conn.Statistics, err)
			} else {
				log.Printf("NATS_:_AsyncErrorCB_: stats: %+v, error: %+v\n", conn.Statistics, err)
			}
		}
	}
	if options.DisconnectedCB != nil {
		opts.DisconnectedCB = options.DisconnectedCB
	} else {
		opts.DisconnectedCB = func(conn *nats.Conn) {
			if n.configs.Logger != nil {
				n.configs.Logger.Printf("NATS_:_DisconnectedCB: action: client connection disconnect, stats: %+v\n", conn.Statistics)
			} else {
				log.Printf("NATS_:_DisconnectedCB: action: client connection disconnect, stats: %+v\n", conn.Statistics)
			}
		}
	}
	opts.User = options.User
	opts.Password = options.Password
	opts.Token = options.Token
	opts.Nkey = options.Nkey
	opts.SignatureCB = options.SignatureCB
	opts.TokenHandler = options.TokenHandler
	opts.Dialer = options.Dialer
	opts.CustomDialer = options.CustomDialer
	opts.UseOldRequestStyle = options.UseOldRequestStyle
	opts.UserJWT = options.UserJWT
	opts.ClosedCB = options.ClosedCB
	opts.FlusherTimeout = options.FlusherTimeout
	return &opts
}
