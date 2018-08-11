// Package micro is a pluggable RPC framework for microservices
package micro

import (
	"context"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/server"
)

type serviceKey struct{}

// Service is an interface that wraps the lower level libraries
// within go-micro. Its a convenience method for building
// and initialising services.
type Service interface {
	Init(...Option)        // [Min] 初始化 Service 的组件及参数
	Options() Options      // [Min] 返回 server 的组件及参数
	Client() client.Client // [Min] 返回 service 的 client
	Server() server.Server // [Min] 返回 service 的 server
	Run() error            // [Min] 运营服务（start，stop 等完整周期）
	String() string        // [Min] 返回 go-micro
}

// Function is a one time executing Service
// [Min] Function 是只执行依次的服务
type Function interface {
	// Inherits Service interface
	Service
	// Done signals to complete execution
	Done() error
	// Handle registers an RPC handler
	Handle(v interface{}) error
	// Subscribe registers a subscriber
	Subscribe(topic string, v interface{}) error
}

// Publisher is syntactic sugar for publishing
// [Min] 其实就是 client + topic 组成的实例
type Publisher interface {
	Publish(ctx context.Context, msg interface{}, opts ...client.PublishOption) error
}

// [Min] 修改 Options 字段的函数
type Option func(*Options)

var (
	// [Min] clinetWrapper 的 metadata 中的某一 key 的前缀，该 key 用来记录 server name
	HeaderPrefix = "X-Micro-"
)

// NewService creates and returns a new Service based on the packages within.
// [Min] 新建一个 service
func NewService(opts ...Option) Service {
	return newService(opts...)
}

// FromContext retrieves a Service from the Context.
// [Min] 从 context 中获取一个 service
func FromContext(ctx context.Context) (Service, bool) {
	s, ok := ctx.Value(serviceKey{}).(Service)
	return s, ok
}

// NewContext returns a new Context with the Service embedded within it.
// [Min] 将 service 加入到 context 中
func NewContext(ctx context.Context, s Service) context.Context {
	return context.WithValue(ctx, serviceKey{}, s)
}

// NewFunction returns a new Function for a one time executing Service
func NewFunction(opts ...Option) Function {
	return newFunction(opts...)
}

// NewPublisher returns a new Publisher
func NewPublisher(topic string, c client.Client) Publisher {
	if c == nil {
		c = client.NewClient()
	}
	return &publisher{c, topic}
}

// RegisterHandler is syntactic sugar for registering a handler
func RegisterHandler(s server.Server, h interface{}, opts ...server.HandlerOption) error {
	return s.Handle(s.NewHandler(h, opts...))
}

// RegisterSubscriber is syntactic sugar for registering a subscriber
func RegisterSubscriber(topic string, s server.Server, h interface{}, opts ...server.SubscriberOption) error {
	return s.Subscribe(s.NewSubscriber(topic, h, opts...))
}
