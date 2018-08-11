package micro

import (
	"context"
	"time"

	"github.com/micro/go-micro/server"
)

type function struct {
	cancel context.CancelFunc // [Min] service 的 Options 中的 context 对应的 cancelFunc
	Service
}

func fnHandlerWrapper(f Function) server.HandlerWrapper {
	return func(h server.HandlerFunc) server.HandlerFunc {
		return func(ctx context.Context, req server.Request, rsp interface{}) error {
			defer f.Done() // [Min] 执行一次关键
			return h(ctx, req, rsp)
		}
	}
}

func fnSubWrapper(f Function) server.SubscriberWrapper {
	return func(s server.SubscriberFunc) server.SubscriberFunc {
		return func(ctx context.Context, msg server.Message) error {
			defer f.Done() // [Min] 执行一次的关键
			return s(ctx, msg)
		}
	}
}

func newFunction(opts ...Option) Function {
	// [Min] 带 cancel 的 context
	ctx, cancel := context.WithCancel(context.Background())

	// force ttl/interval
	// [Min] hardcode 参数
	fopts := []Option{
		RegisterTTL(time.Minute),
		RegisterInterval(time.Second * 30),
	}

	// prepend to opts
	fopts = append(fopts, opts...)

	// make context the last thing
	fopts = append(fopts, Context(ctx))

	service := newService(fopts...)

	fn := &function{
		cancel:  cancel,
		Service: service,
	}

	service.Server().Init(
		// ensure the service waits for requests to finish
		server.Wait(true),
		// wrap handlers and subscribers to finish execution
		// [Min] 包装 server 的 handler 和 subscriber，
		// [Min] 使得完成一次 request 的处理就执行 cancel 函数，从而达到 service 只执行一次的效果
		server.WrapHandler(fnHandlerWrapper(fn)),
		server.WrapSubscriber(fnSubWrapper(fn)),
	)

	return fn
}

// [Min] 执行 ctx 对应的 cancel 函数
func (f *function) Done() error {
	f.cancel()
	return nil
}

func (f *function) Handle(v interface{}) error {
	return f.Service.Server().Handle(
		f.Service.Server().NewHandler(v),
	)
}

func (f *function) Subscribe(topic string, v interface{}) error {
	return f.Service.Server().Subscribe(
		f.Service.Server().NewSubscriber(topic, v),
	)
}
