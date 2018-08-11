package server

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/codec"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/registry"
)

const (
	subSig = "func(context.Context, interface{}) error"
)

// [Min] handler 就是处理订阅消息的一种具体行为，
// [Min] 其本质可以是一个函数，也可以是一个拥有方法的实例类型的所有可导出的方法
type handler struct {
	method  reflect.Value // [Min] 函数或实例类型的值反射对象
	reqType reflect.Type  // [Min] 函数或者实例类型的方法中作为 request 参数的类型反射
	ctxType reflect.Type  // [Min] 函数或实例类型方法中的 context 的类型反射，可能没有 context
}

// [Min] 和内部的 service 类似的，也是一个用来表示与订阅相关的 handler 的实例类型
type subscriber struct {
	topic      string               // [Min] 该订阅的 topic
	rcvr       reflect.Value        // [Min] sub 的值反射，函数的值反射或者是 receiver 的值反射
	typ        reflect.Type         // [Min] 该订阅的类型反射
	subscriber interface{}          // [Min] newSubscriber 中用来构造 subscriber 的原始输入的 sub
	handlers   []*handler           // [Min] 所有有效方法，函数映射成 handler 之后的集合，也就是对该订阅消息的所有处理手段
	endpoints  []*registry.Endpoint // [Min] 每一个 handler 映射成的 Endpoint
	opts       SubscriberOptions    // [Min] SubscriberOptions
}

// [Min] sub 就是一个订阅的具体实例，
// [Min] 可以是一个具体类型的实例（其方法定义了接收订阅推送后的行为），
// [Min] 也可以是一个拥有函数签名 func(context.Context, interface{}) error 的函数，也描述了如何处理订阅消息
func newSubscriber(topic string, sub interface{}, opts ...SubscriberOption) Subscriber {
	var options SubscriberOptions
	for _, o := range opts {
		o(&options)
	}

	var endpoints []*registry.Endpoint
	var handlers []*handler

	// [Min] 将该订阅消息的所有处理手段，即方法或函数映射成 handler，Endpoint

	// [Min] sub 如果是函数
	if typ := reflect.TypeOf(sub); typ.Kind() == reflect.Func {
		h := &handler{
			method: reflect.ValueOf(sub), // [Min] method 就是函数本身的值反射对象
		}

		switch typ.NumIn() {
		case 1:
			h.reqType = typ.In(0) // [Min] 函数的第一个参数是订阅的消息，事实上这不是一个有效的函数，后续在检查的时候会报错
		case 2:
			h.ctxType = typ.In(0) // [Min] 第一个是 context
			h.reqType = typ.In(1) // [Min] 第二个是输入
		}

		handlers = append(handlers, h)

		endpoints = append(endpoints, &registry.Endpoint{
			Name: "Func", // [Min] Name 无关紧要，因为也可能是一个匿名函数
			// [Min] 将订阅消息的具体类型映射到 registry.Value，作为 Endpoint 的 Request
			Request: extractSubValue(typ),
			Metadata: map[string]string{
				"topic":      topic,
				"subscriber": "true",
			},
		})
	} else {
		// [Min] sub 不是函数，而是一个具体的类型的实例，
		// [Min] 那么就需要将此类型下的所有可导出方法当成一种处理订阅消息的 handler
		hdlr := reflect.ValueOf(sub)                 // [Min] 实例的值反射，作为一个总的 hdlr
		name := reflect.Indirect(hdlr).Type().Name() // [Min] 实例类型的具体名称

		for m := 0; m < typ.NumMethod(); m++ {
			method := typ.Method(m)
			h := &handler{
				method: method.Func, // [Min] 方法本身的值反射对象，就是一个 receiver 作为第一参数的函数
			}

			switch method.Type.NumIn() {
			case 2:
				// [Min] 第一个为 receiver，所以第二个参数的类型就是 reqType
				h.reqType = method.Type.In(1)
			case 3:
				// [Min] 第二个为 context，第三个为输入
				h.ctxType = method.Type.In(1)
				h.reqType = method.Type.In(2)
			}

			handlers = append(handlers, h)

			endpoints = append(endpoints, &registry.Endpoint{
				Name:    name + "." + method.Name,     // [Min] A.Method
				Request: extractSubValue(method.Type), // [Min] 根据方法的类型反射得出 registry.Value
				Metadata: map[string]string{
					"topic":      topic,
					"subscriber": "true",
				},
			})
		}
	}

	// [Min] 构造 subscriber
	return &subscriber{
		rcvr:       reflect.ValueOf(sub), // [Min] sub 的值反射
		typ:        reflect.TypeOf(sub),  // [Min] sub 的类型反射
		topic:      topic,
		subscriber: sub, // [Min] 原始的 sub
		handlers:   handlers,
		endpoints:  endpoints,
		opts:       options,
	}
}

// [Min] 验证一个 subscriber 类型实例是否有效
func validateSubscriber(sub Subscriber) error {
	// [Min] 原始 sub 的类型
	typ := reflect.TypeOf(sub.Subscriber())
	var argType reflect.Type

	if typ.Kind() == reflect.Func {
		// [Min] 如果是函数，必须签名是 func(context.Context, interface{}) error
		name := "Func"
		switch typ.NumIn() {
		case 2:
			argType = typ.In(1)
		default:
			return fmt.Errorf("subscriber %v takes wrong number of args: %v required signature %s", name, typ.NumIn(), subSig)
		}
		// [Min] 作为输入的订阅消息的类型必须可导出
		if !isExportedOrBuiltinType(argType) {
			return fmt.Errorf("subscriber %v argument type not exported: %v", name, argType)
		}
		// [Min] 输出必须只有一个，且是 error
		if typ.NumOut() != 1 {
			return fmt.Errorf("subscriber %v has wrong number of outs: %v require signature %s",
				name, typ.NumOut(), subSig)
		}
		if returnType := typ.Out(0); returnType != typeOfError {
			return fmt.Errorf("subscriber %v returns %v not error", name, returnType.String())
		}
	} else {
		// [Min] 实例类型的情况
		hdlr := reflect.ValueOf(sub.Subscriber())
		name := reflect.Indirect(hdlr).Type().Name()

		for m := 0; m < typ.NumMethod(); m++ {
			method := typ.Method(m)

			// [Min] 方法的等价输入必须是 recevier，context，arg
			switch method.Type.NumIn() {
			case 3:
				argType = method.Type.In(2)
			default:
				return fmt.Errorf("subscriber %v.%v takes wrong number of args: %v required signature %s",
					name, method.Name, method.Type.NumIn(), subSig)
			}

			// [Min] arg 必须可导出
			if !isExportedOrBuiltinType(argType) {
				return fmt.Errorf("%v argument type not exported: %v", name, argType)
			}
			// [Min] 输出必须唯一且是 error
			if method.Type.NumOut() != 1 {
				return fmt.Errorf(
					"subscriber %v.%v has wrong number of outs: %v require signature %s",
					name, method.Name, method.Type.NumOut(), subSig)
			}
			if returnType := method.Type.Out(0); returnType != typeOfError {
				return fmt.Errorf("subscriber %v.%v returns %v not error", name, method.Name, returnType.String())
			}
		}
	}

	return nil
}

// [Min] 由 subscriber 生成 broker.Handler
// [Min] 当调用 s.Options().Broker.Subscribe(topic，broker.Handler) 之后，就完成了订阅
func (s *rpcServer) createSubHandler(sb *subscriber, opts Options) broker.Handler {
	return func(p broker.Publication) error {
		// [Min] 获取消息，编码方式
		msg := p.Message()
		ct := msg.Header["Content-Type"]
		cf, err := s.newCodec(ct)
		if err != nil {
			return err
		}

		hdr := make(map[string]string)
		for k, v := range msg.Header {
			hdr[k] = v
		}
		delete(hdr, "Content-Type")
		// [Min] 将 msg 的 header 去掉 Content-Type 之后存入一个 context 中
		ctx := metadata.NewContext(context.Background(), hdr)

		// [Min] results 用来接收处理订阅消息的返回结果
		// [Min] results 是一个缓存通道，每一个 handler 结束后都不会阻塞返回
		results := make(chan error, len(sb.handlers))

		// [Min] 对每一个 handler 都开启一个 goroutine 对其进行处理
		for i := 0; i < len(sb.handlers); i++ {
			handler := sb.handlers[i]

			var isVal bool
			var req reflect.Value

			// [Min] 如果 reqType 是指针类型，那么构造一个该指针指向类型的零值
			// [Min] 如果不是指针，构造一个该类型的零值
			if handler.reqType.Kind() == reflect.Ptr {
				req = reflect.New(handler.reqType.Elem())
			} else {
				req = reflect.New(handler.reqType)
				isVal = true
			}
			if isVal {
				req = req.Elem()
			}

			b := &buffer{bytes.NewBuffer(msg.Body)}
			co := cf(b)
			defer co.Close()

			if err := co.ReadHeader(&codec.Message{}, codec.Publication); err != nil {
				return err
			}

			// [Min] 读取 msg.Body 到 req
			if err := co.ReadBody(req.Interface()); err != nil {
				return err
			}

			// [Min] 构造一个 SubscriberFunc，用来准备 handler 的参数并执行 handler 对应的函数或方法的函数
			// [Min] 也就是调用这个函数就是正式对订阅消息的处理
			fn := func(ctx context.Context, msg Message) error {
				var vals []reflect.Value
				// [Min] 构造 handler 的参数 receiver，context，req
				// [Min] 如果不是函数，则要先加入 receiver
				if sb.typ.Kind() != reflect.Func {
					vals = append(vals, sb.rcvr)
				}
				// [Min] 如果有 context，加入 context
				if handler.ctxType != nil {
					vals = append(vals, reflect.ValueOf(ctx))
				}

				vals = append(vals, reflect.ValueOf(msg.Payload()))

				// [Min] 调用该 handler，执行函数，方法
				returnValues := handler.method.Call(vals)
				if err := returnValues[0].Interface(); err != nil {
					return err.(error)
				}
				return nil
			}

			// [Min] 加上 wrapper
			for i := len(opts.SubWrappers); i > 0; i-- {
				fn = opts.SubWrappers[i-1](fn)
			}

			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				// [Min] 执行 fn 并返回结果，不会阻塞
				results <- fn(ctx, &rpcMessage{
					topic:       sb.topic,
					contentType: ct,
					payload:     req.Interface(),
				})
			}()
		}

		var errors []string

		// [Min] 接收返回结果
		for i := 0; i < len(sb.handlers); i++ {
			if err := <-results; err != nil {
				errors = append(errors, err.Error())
			}
		}

		if len(errors) > 0 {
			return fmt.Errorf("subscriber error: %s", strings.Join(errors, "\n"))
		}

		return nil
	}
}

// [Min] 返回 topic
func (s *subscriber) Topic() string {
	return s.topic
}

// [Min] 返回构造 subscriber 的原始输入（具体函数或类型实例）
func (s *subscriber) Subscriber() interface{} {
	return s.subscriber
}

// [Min] 返回 endpoints
func (s *subscriber) Endpoints() []*registry.Endpoint {
	return s.endpoints
}

// [Min] 返回 SubscriberOptions
func (s *subscriber) Options() SubscriberOptions {
	return s.opts
}
