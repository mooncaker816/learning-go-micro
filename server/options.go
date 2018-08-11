package server

import (
	"context"
	"time"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/codec"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/server/debug"
	"github.com/micro/go-micro/transport"
)

type Options struct {
	Codecs       map[string]codec.NewCodec
	Broker       broker.Broker
	Registry     registry.Registry
	Transport    transport.Transport
	Metadata     map[string]string
	Name         string
	Address      string
	Advertise    string
	Id           string
	Version      string
	HdlrWrappers []HandlerWrapper
	SubWrappers  []SubscriberWrapper

	RegisterTTL time.Duration

	// Debug Handler which can be set by a user
	DebugHandler debug.DebugHandler

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

// [Min] 新建 server Options，然后用 Option 函数修改 Options，
// [Min] 如果在这之后参数还是没有设置，使用默认值
func newOptions(opt ...Option) Options {
	opts := Options{
		Codecs:   make(map[string]codec.NewCodec),
		Metadata: map[string]string{},
	}

	for _, o := range opt {
		o(&opts)
	}

	if opts.Broker == nil {
		opts.Broker = broker.DefaultBroker
	}

	if opts.Registry == nil {
		opts.Registry = registry.DefaultRegistry
	}

	if opts.Transport == nil {
		opts.Transport = transport.DefaultTransport
	}

	if opts.DebugHandler == nil {
		opts.DebugHandler = debug.DefaultDebugHandler
	}

	if len(opts.Address) == 0 {
		opts.Address = DefaultAddress
	}

	if len(opts.Name) == 0 {
		opts.Name = DefaultName
	}

	if len(opts.Id) == 0 {
		opts.Id = DefaultId
	}

	if len(opts.Version) == 0 {
		opts.Version = DefaultVersion
	}

	return opts
}

// Server name
// [Min] 修改 server 名称的 Option 函数
func Name(n string) Option {
	return func(o *Options) {
		o.Name = n
	}
}

// Unique server id
// [Min] 修改 server Id 的 Option 函数
func Id(id string) Option {
	return func(o *Options) {
		o.Id = id
	}
}

// Version of the service
// [Min] 修改 server 版本的 Option 函数
func Version(v string) Option {
	return func(o *Options) {
		o.Version = v
	}
}

// Address to bind to - host:port
// [Min] 修改 server 地址的 Option 函数
func Address(a string) Option {
	return func(o *Options) {
		o.Address = a
	}
}

// The address to advertise for discovery - host:port
// [Min] 修改 server 广播地址的 Option 函数
func Advertise(a string) Option {
	return func(o *Options) {
		o.Advertise = a
	}
}

// Broker to use for pub/sub
// [Min] 修改 server Broker 的 Option 函数
func Broker(b broker.Broker) Option {
	return func(o *Options) {
		o.Broker = b
	}
}

// Codec to use to encode/decode requests for a given content type
// [Min] 修改 server codec 的 Option 函数
func Codec(contentType string, c codec.NewCodec) Option {
	return func(o *Options) {
		o.Codecs[contentType] = c
	}
}

// Registry used for discovery
// [Min] 修改 server registry 的 Option 函数
func Registry(r registry.Registry) Option {
	return func(o *Options) {
		o.Registry = r
	}
}

// Transport mechanism for communication e.g http, rabbitmq, etc
// [Min] 修改 server Transport 的 Option 函数
func Transport(t transport.Transport) Option {
	return func(o *Options) {
		o.Transport = t
	}
}

// DebugHandler for this server
// [Min] 修改 server DebugHandler 的 Option 函数
func DebugHandler(d debug.DebugHandler) Option {
	return func(o *Options) {
		o.DebugHandler = d
	}
}

// Metadata associated with the server
// [Min] 修改 server Metadata 的 Option 函数
func Metadata(md map[string]string) Option {
	return func(o *Options) {
		o.Metadata = md
	}
}

// Register the service with a TTL
// [Min] 修改 server RegisterTTL 的 Option 函数
func RegisterTTL(t time.Duration) Option {
	return func(o *Options) {
		o.RegisterTTL = t
	}
}

// Wait tells the server to wait for requests to finish before exiting
// [Min] 在 Context 中加入 wait flag，用来表示 server 在退出之前是否要等待完成所有的 request
func Wait(b bool) Option {
	return func(o *Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, "wait", b)
	}
}

// Adds a handler Wrapper to a list of options passed into the server
// [Min] 修改 server HdlrWrappers 的 Option 函数
func WrapHandler(w HandlerWrapper) Option {
	return func(o *Options) {
		o.HdlrWrappers = append(o.HdlrWrappers, w)
	}
}

// Adds a subscriber Wrapper to a list of options passed into the server
// [Min] 修改 server SubWrappers 的 Option 函数
func WrapSubscriber(w SubscriberWrapper) Option {
	return func(o *Options) {
		o.SubWrappers = append(o.SubWrappers, w)
	}
}
