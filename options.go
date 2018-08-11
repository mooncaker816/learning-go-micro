package micro

import (
	"context"
	"time"

	"github.com/micro/cli"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/cmd"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/selector"
	"github.com/micro/go-micro/server"
	"github.com/micro/go-micro/transport"
)

// [Min] Options 是指一个 service 选择的六大类组件，以及相关参数
// [Min] 这些组件均设计成 interface 的模式，从而可以很容易实现插件
type Options struct {
	Broker    broker.Broker
	Cmd       cmd.Cmd
	Client    client.Client
	Server    server.Server
	Registry  registry.Registry
	Transport transport.Transport

	// Register loop interval
	RegisterInterval time.Duration

	// Before and After funcs
	BeforeStart []func() error
	BeforeStop  []func() error
	AfterStart  []func() error
	AfterStop   []func() error

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

// [Min] newOptions 首先会使用六个默认的组件组成 Options，然后按需求修改 Options
func newOptions(opts ...Option) Options {
	opt := Options{
		Broker:    broker.DefaultBroker,
		Cmd:       cmd.DefaultCmd,
		Client:    client.DefaultClient,
		Server:    server.DefaultServer,
		Registry:  registry.DefaultRegistry,
		Transport: transport.DefaultTransport,
		Context:   context.Background(),
	}

	for _, o := range opts {
		o(&opt)
	}

	return opt
}

/* [Min]
// Client Options 中包含的组件插件
	Broker    broker.Broker
	Registry  registry.Registry
	Selector  selector.Selector
	Transport transport.Transport

// Server Options 中包含的组件插件
	Broker       broker.Broker
	Registry     registry.Registry
	Transport    transport.Transport

// Selector Options 中包含的组件插件
	Registry registry.Registry

// Transport Options 中包含的组件插件

// Registry Options 中包含的组件插件

*/

// [Min] 修改 service Broker 的 Option 函数
func Broker(b broker.Broker) Option {
	return func(o *Options) {
		o.Broker = b
		// Update Client and Server
		// [Min] 修改 service  Broker 的同时也要将 client 和 server 的 Broker 修改
		// [Min] 调用 client 和 server 各自的 Broker 生成修改 Broker 的 Option 函数
		// [Min] 再通过 client 和 server 的 Init 方法执行 Option 函数，从而修改 Broker
		o.Client.Init(client.Broker(b))
		o.Server.Init(server.Broker(b))
	}
}

// [Min] 修改 service Cmd 的 Option 函数
func Cmd(c cmd.Cmd) Option {
	return func(o *Options) {
		o.Cmd = c
	}
}

// [Min] 修改 service Client 的 Option 函数
func Client(c client.Client) Option {
	return func(o *Options) {
		o.Client = c
	}
}

// Context specifies a context for the service.
// Can be used to signal shutdown of the service.
// Can be used for extra option values.
// [Min] 修改 service Context 的 Option 函数
func Context(ctx context.Context) Option {
	return func(o *Options) {
		o.Context = ctx
	}
}

// [Min] 修改 service Server 的 Option 函数
func Server(s server.Server) Option {
	return func(o *Options) {
		o.Server = s
	}
}

// Registry sets the registry for the service
// and the underlying components
// [Min] 修改 service Registry 的 Option 函数
func Registry(r registry.Registry) Option {
	return func(o *Options) {
		o.Registry = r
		// Update Client and Server
		// [Min] client 和 server 的 Options 中都有 registry，所以也要同步修改
		o.Client.Init(client.Registry(r))
		o.Server.Init(server.Registry(r))
		// Update Selector
		// [Min] selector Options 中有 registry，需要同时修改
		// [Min] 但是 service 中不含有 selector，selector 是对于 client 而言的
		// [Min] 所以先通过 client 找到对应的 selector，然后再调用 Init 方法修改
		o.Client.Options().Selector.Init(selector.Registry(r))
		// Update Broker
		// [Min] broker  Options 中并没有 registry，
		// [Min] 但是这里的 broker.Registry 会将 registry 通过其 context 传入
		o.Broker.Init(broker.Registry(r))
	}
}

// Selector sets the selector for the service client
// [Min] 修改 service Selector 的 Option 函数，
// [Min] service 并没有直接记录 selector，而是应该修改 client 中的 selector
func Selector(s selector.Selector) Option {
	return func(o *Options) {
		o.Client.Init(client.Selector(s))
	}
}

// Transport sets the transport for the service
// and the underlying components
// [Min] 修改 service Transport 的 Option 函数
func Transport(t transport.Transport) Option {
	return func(o *Options) {
		o.Transport = t
		// Update Client and Server
		// [Min] Trnasport 同时在 client 和 server 的 Options 中，需要同步修改
		o.Client.Init(client.Transport(t))
		o.Server.Init(server.Transport(t))
	}
}

// Convenience options
// [Min] 一些方便的 Option 函数，可以直接在 service level 对 service 的组件的相关属性进行修改

// Name of the service
// [Min] 修改 server 的名称
func Name(n string) Option {
	return func(o *Options) {
		o.Server.Init(server.Name(n))
	}
}

// Version of the service
// [Min] 修改 server 的版本
func Version(v string) Option {
	return func(o *Options) {
		o.Server.Init(server.Version(v))
	}
}

// Metadata associated with the service
// [Min] 修改 server 的 Metadata
func Metadata(md map[string]string) Option {
	return func(o *Options) {
		o.Server.Init(server.Metadata(md))
	}
}

func Flags(flags ...cli.Flag) Option {
	return func(o *Options) {
		o.Cmd.App().Flags = append(o.Cmd.App().Flags, flags...)
	}
}

func Action(a func(*cli.Context)) Option {
	return func(o *Options) {
		o.Cmd.App().Action = a
	}
}

// RegisterTTL specifies the TTL to use when registering the service
// [Min] 修改 server 的 RegisterTTL
func RegisterTTL(t time.Duration) Option {
	return func(o *Options) {
		o.Server.Init(server.RegisterTTL(t))
	}
}

// RegisterInterval specifies the interval on which to re-register
// [Min] 修改 service 的 RegisterInterval
func RegisterInterval(t time.Duration) Option {
	return func(o *Options) {
		o.RegisterInterval = t
	}
}

// WrapClient is a convenience method for wrapping a Client with
// some middleware component. A list of wrappers can be provided.
// [Min] 从 service level 对 client 进行相应的 wrapper 封装
func WrapClient(w ...client.Wrapper) Option {
	return func(o *Options) {
		// apply in reverse
		for i := len(w); i > 0; i-- {
			o.Client = w[i-1](o.Client)
		}
	}
}

// WrapCall is a convenience method for wrapping a Client CallFunc
// [Min] 从 service level 添加 w 到 client Options 中的 CallOptions.CallWrappers
func WrapCall(w ...client.CallWrapper) Option {
	return func(o *Options) {
		o.Client.Init(client.WrapCall(w...))
	}
}

// WrapHandler adds a handler Wrapper to a list of options passed into the server
// [Min] 从 service level 添加 w 到 server Options 中的 HdlrWrappers
func WrapHandler(w ...server.HandlerWrapper) Option {
	return func(o *Options) {
		var wrappers []server.Option

		for _, wrap := range w {
			wrappers = append(wrappers, server.WrapHandler(wrap))
		}

		// Init once
		o.Server.Init(wrappers...)
	}
}

// WrapSubscriber adds a subscriber Wrapper to a list of options passed into the server
// [Min] 从 service level 添加 w 到 server Options 中的 SubWrappers
func WrapSubscriber(w ...server.SubscriberWrapper) Option {
	return func(o *Options) {
		var wrappers []server.Option

		for _, wrap := range w {
			wrappers = append(wrappers, server.WrapSubscriber(wrap))
		}

		// Init once
		o.Server.Init(wrappers...)
	}
}

// Before and Afters
// [Min] 修改 service Options 中的 before & after 函数

func BeforeStart(fn func() error) Option {
	return func(o *Options) {
		o.BeforeStart = append(o.BeforeStart, fn)
	}
}

func BeforeStop(fn func() error) Option {
	return func(o *Options) {
		o.BeforeStop = append(o.BeforeStop, fn)
	}
}

func AfterStart(fn func() error) Option {
	return func(o *Options) {
		o.AfterStart = append(o.AfterStart, fn)
	}
}

func AfterStop(fn func() error) Option {
	return func(o *Options) {
		o.AfterStop = append(o.AfterStop, fn)
	}
}
