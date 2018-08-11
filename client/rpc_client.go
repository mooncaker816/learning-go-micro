package client

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"sync/atomic"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/codec"
	"github.com/micro/go-micro/errors"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/selector"
	"github.com/micro/go-micro/transport"
)

type rpcClient struct {
	once sync.Once
	opts Options
	pool *pool
	seq  uint64
}

// [Min] 新建一个 rpcClient，连接池初始化也在这里完成
func newRpcClient(opt ...Option) Client {
	// [Min] 初始化 Options
	opts := newOptions(opt...)

	rc := &rpcClient{
		once: sync.Once{},
		opts: opts,
		// [Min] 默认的 poolsize 为 1， poolttl 为 1 min
		pool: newPool(opts.PoolSize, opts.PoolTTL),
		seq:  0,
	}

	c := Client(rc)

	// wrap in reverse
	for i := len(opts.Wrappers); i > 0; i-- {
		c = opts.Wrappers[i-1](c)
	}

	return c
}

// [Min] 获取 contentType 对应的 NewCodec，Options 里没有就去 defalutCodecs 中去找
func (r *rpcClient) newCodec(contentType string) (codec.NewCodec, error) {
	if c, ok := r.opts.Codecs[contentType]; ok {
		return c, nil
	}
	if cf, ok := defaultCodecs[contentType]; ok {
		return cf, nil
	}
	return nil, fmt.Errorf("Unsupported Content-Type: %s", contentType)
}

// [Min] 实际调用方法
func (r *rpcClient) call(ctx context.Context, address string, req Request, resp interface{}, opts CallOptions) error {
	msg := &transport.Message{
		Header: make(map[string]string),
	}

	// [Min] 从 context 中获取 metadata，存入 message 的 header 中
	md, ok := metadata.FromContext(ctx)
	if ok {
		for k, v := range md {
			msg.Header[k] = v
		}
	}

	// set timeout in nanoseconds
	// [Min] 按 CallOptions 中的 RequestTimeout 设置超时信息
	// [Min] ctx 中有 timeout 设置时，opts.RequestTimeout 与其保持一致，否则为默认的超时 5s
	msg.Header["Timeout"] = fmt.Sprintf("%d", opts.RequestTimeout)
	// set the content type for the request
	// [Min] 设置 Content-Type，Accept
	msg.Header["Content-Type"] = req.ContentType()
	// set the accept header
	msg.Header["Accept"] = req.ContentType()

	// [Min] 获取该 contentType 对应的 codec 生成函数
	cf, err := r.newCodec(req.ContentType())
	if err != nil {
		return errors.InternalServerError("go.micro.client", err.Error())
	}

	var grr error
	// [Min] 从连接池中获取一个 poolConn，其实就是 transport.Client
	c, err := r.pool.getConn(address, r.opts.Transport, transport.WithTimeout(opts.DialTimeout))
	if err != nil {
		return errors.InternalServerError("go.micro.client", "connection error: %v", err)
	}
	defer func() {
		// defer execution of release
		// [Min] 最后释放该连接
		r.pool.release(address, c, grr)
	}()

	// [Min] seq 用于 protobuf 消息中的 Seq 字段，唯一
	seq := r.seq
	atomic.AddUint64(&r.seq, 1)

	// [Min] 构造 rpcStream，通过 rpcStream.Send 来发送 request 实现调用
	// [Min] rpcStream.Recv 接受 response
	// [Min] 这里虽然写成 stream 的形式，但是 send 和 receive 是在函数内部顺序执行的
	// [Min] 真正的 stream，是在内部只执行 send，将 stream 返回，从而可以通过 receive 在外部接收 response
	stream := &rpcStream{
		context: ctx,
		request: req,
		closed:  make(chan bool), // [Min] 判断 codec 是否已关闭
		// [Min] codec 是一个经过编码封装，http 客户端封装的结合体，
		// [Min] 调用写入相关的方法时，就是发送经过编码后的请求给对端
		// [Min] 调用读取相关的方法时，就是读取解码后的请求的返回
		codec: newRpcPlusCodec(msg, c, cf),
		seq:   seq,
	}
	defer stream.Close() // [Min] 关闭 codec 中的 rwc

	ch := make(chan error, 1)

	// [Min] goroutine 发起请求，并同步等待返回
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// [Min] 防止 panic
				ch <- errors.InternalServerError("go.micro.client", "panic recovered: %v", r)
			}
		}()

		// send request
		// [Min] 调用 codec.WriteRequest 发起请求
		if err := stream.Send(req.Request()); err != nil {
			ch <- err
			return
		}

		// recv request
		// [Min] 同步等待返回
		// [Min] 调用 codec.ReadResponseHeader，codec.ReadResponseBody 读取请求返回
		if err := stream.Recv(resp); err != nil {
			ch <- err
			return
		}

		// success
		ch <- nil
	}()

	select {
	case err := <-ch:
		// [Min] 请求的返回
		grr = err
		return err
	case <-ctx.Done():
		// [Min] 超时
		grr = ctx.Err()
		return errors.New("go.micro.client", fmt.Sprintf("request timeout: %v", ctx.Err()), 408)
	}
}

// [Min] 基本和 call 一样，只不过返回了 rpcStream，可以在外部调用 Recv 方法获得 response
func (r *rpcClient) stream(ctx context.Context, address string, req Request, opts CallOptions) (Stream, error) {
	msg := &transport.Message{
		Header: make(map[string]string),
	}

	md, ok := metadata.FromContext(ctx)
	if ok {
		for k, v := range md {
			msg.Header[k] = v
		}
	}

	// set timeout in nanoseconds
	msg.Header["Timeout"] = fmt.Sprintf("%d", opts.RequestTimeout)
	// set the content type for the request
	msg.Header["Content-Type"] = req.ContentType()
	// set the accept header
	msg.Header["Accept"] = req.ContentType()

	cf, err := r.newCodec(req.ContentType())
	if err != nil {
		return nil, errors.InternalServerError("go.micro.client", err.Error())
	}

	// [Min] Stream 的 dial 选项
	dOpts := []transport.DialOption{
		transport.WithStream(),
	}

	if opts.DialTimeout >= 0 {
		dOpts = append(dOpts, transport.WithTimeout(opts.DialTimeout))
	}

	// [Min] call 是通过 getConn 来获取 conn，stream 需要根据
	c, err := r.opts.Transport.Dial(address, dOpts...)
	if err != nil {
		return nil, errors.InternalServerError("go.micro.client", "connection error: %v", err)
	}

	stream := &rpcStream{
		context: ctx,
		request: req,
		closed:  make(chan bool),
		codec:   newRpcPlusCodec(msg, c, cf),
	}

	ch := make(chan error, 1)

	// [Min] goroutine 发起请求，没有直接调用 stream.Recv，通过返回 stream，使得读取返回可以在外部进行，达到 stream 的效果
	go func() {
		ch <- stream.Send(req.Request())
	}()

	var grr error

	select {
	case err := <-ch:
		grr = err
	case <-ctx.Done():
		grr = errors.New("go.micro.client", fmt.Sprintf("request timeout: %v", ctx.Err()), 408)
	}

	if grr != nil {
		stream.Close()
		return nil, grr
	}

	return stream, nil
}

func (r *rpcClient) Init(opts ...Option) error {
	size := r.opts.PoolSize
	ttl := r.opts.PoolTTL

	for _, o := range opts {
		o(&r.opts)
	}

	// update pool configuration if the options changed
	if size != r.opts.PoolSize || ttl != r.opts.PoolTTL {
		r.pool.Lock()
		r.pool.size = r.opts.PoolSize
		r.pool.ttl = int64(r.opts.PoolTTL.Seconds())
		r.pool.Unlock()
	}

	return nil
}

func (r *rpcClient) Options() Options {
	return r.opts
}

// [Min] 获取能够返回对端服务的一个 node 的函数 next
func (r *rpcClient) next(request Request, opts CallOptions) (selector.Next, error) {
	// return remote address
	// [Min] 如果 CallOptions 中的 address 不为空，就以此为地址构造一个 Node
	if len(opts.Address) > 0 {
		return func() (*registry.Node, error) {
			return &registry.Node{
				Address: opts.Address,
			}, nil
		}, nil
	}

	// get next nodes from the selector
	// [Min] 根据 service name，SelectOptions 获取一个 node
	next, err := r.opts.Selector.Select(request.Service(), opts.SelectOptions...)
	if err != nil && err == selector.ErrNotFound {
		return nil, errors.NotFound("go.micro.client", "service %s: %v", request.Service(), err.Error())
	} else if err != nil {
		return nil, errors.InternalServerError("go.micro.client", "error selecting %s node: %v", request.Service(), err.Error())
	}

	return next, nil
}

// [Min] 发起 rpc 调用
func (r *rpcClient) Call(ctx context.Context, request Request, response interface{}, opts ...CallOption) error {
	// make a copy of call opts
	// [Min] 拷贝之后再修改，修改的内容只对当次调用有效
	callOpts := r.opts.CallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}

	// [Min] next 为获取与该 request 一致的对端服务的 Node 的函数
	next, err := r.next(request, callOpts)
	if err != nil {
		return err
	}

	// check if we already have a deadline
	// [Min] 设置调用 deadline
	d, ok := ctx.Deadline()
	// [Min] 如果 ctx 还没设 deadline，就以 callOpts.RequestTimeout 设置 deadline 即可
	if !ok {
		// no deadline so we create a new one
		ctx, _ = context.WithTimeout(ctx, callOpts.RequestTimeout)
	} else {
		// [Min] 如果 ctx 中已经设置了 deadline，那么我们就以此为标准，修改 callOpts.RequestTimeout
		// got a deadline so no need to setup context
		// but we need to set the timeout we pass along
		opt := WithRequestTimeout(d.Sub(time.Now()))
		opt(&callOpts)
	}

	// should we noop right here?
	select {
	case <-ctx.Done():
		return errors.New("go.micro.client", fmt.Sprintf("%v", ctx.Err()), 408)
	default:
	}

	// make copy of call method
	// [Min] 拷贝 r.call 方法
	rcall := r.call

	// wrap the call in reverse
	// [Min] 对 rcall 添加 wrapper
	for i := len(callOpts.CallWrappers); i > 0; i-- {
		rcall = callOpts.CallWrappers[i-1](rcall)
	}

	// return errors.New("go.micro.client", "request timeout", 408)
	// [Min] i 是 attempts，在 rcall 外面再封装一个按照重试次数递增休息间隔的调用方法
	// [Min] call 里会实际发起调用
	call := func(i int) error {
		// call backoff first. Someone may want an initial start delay
		// [Min] 根据 i 获取 backoff 的时间间隔 t
		t, err := callOpts.Backoff(ctx, request, i)
		if err != nil {
			return errors.InternalServerError("go.micro.client", "backoff error: %v", err.Error())
		}

		// only sleep if greater than 0
		// [Min] 休息 t 时间
		if t.Seconds() > 0 {
			time.Sleep(t)
		}

		// select next node
		// [Min] 获取 node
		node, err := next()
		if err != nil && err == selector.ErrNotFound {
			return errors.NotFound("go.micro.client", "service %s: %v", request.Service(), err.Error())
		} else if err != nil {
			return errors.InternalServerError("go.micro.client", "error getting next %s node: %v", request.Service(), err.Error())
		}

		// set the address
		// [Min] 准备调用地址
		address := node.Address
		if node.Port > 0 {
			address = fmt.Sprintf("%s:%d", address, node.Port)
		}

		// make the call
		// [Min] 调用 rcall，实现可重试封装
		err = rcall(ctx, address, request, response, callOpts)
		// [Min] 调用结束后，根据结果更新节点信息
		r.opts.Selector.Mark(request.Service(), node, err)
		return err
	}

	// [Min] 缓存设为最大重试次数的 error 通道
	ch := make(chan error, callOpts.Retries)
	var gerr error

	// [Min] goroutine 发起调用，如果调用返回 err 不为 nil，在重试次数范围内，并且满足重试条件的情况下，发起重试
	for i := 0; i <= callOpts.Retries; i++ {
		go func() {
			ch <- call(i)
		}()

		select {
		// [Min] 当次调用超时
		case <-ctx.Done():
			return errors.New("go.micro.client", fmt.Sprintf("call timeout: %v", ctx.Err()), 408)
		// [Min] 当次调用完成
		case err := <-ch:
			// if the call succeeded lets bail early
			// [Min] 如果返回 error 为 nil，直接返回
			if err == nil {
				return nil
			}

			// [Min] 调用 Retry 检查设定好的重试发起条件
			retry, rerr := callOpts.Retry(ctx, request, i, err)
			// [Min] 如果检查本身报错了，直接返回检查报的错
			if rerr != nil {
				return rerr
			}

			// [Min] 如果不满足重试条件，返回当次调用的 err
			if !retry {
				return err
			}

			// [Min] 暂存 err，循环发起下次调用
			gerr = err
		}
	}

	return gerr
}

// [Min] 和 Call 相比，一个 request 调用没有了 deadline，也没有 CallWrapper, 其他都类似
func (r *rpcClient) Stream(ctx context.Context, request Request, opts ...CallOption) (Stream, error) {
	// make a copy of call opts
	callOpts := r.opts.CallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}

	next, err := r.next(request, callOpts)
	if err != nil {
		return nil, err
	}

	// should we noop right here?
	select {
	case <-ctx.Done():
		return nil, errors.New("go.micro.client", fmt.Sprintf("%v", ctx.Err()), 408)
	default:
	}

	call := func(i int) (Stream, error) {
		// call backoff first. Someone may want an initial start delay
		t, err := callOpts.Backoff(ctx, request, i)
		if err != nil {
			return nil, errors.InternalServerError("go.micro.client", "backoff error: %v", err.Error())
		}

		// only sleep if greater than 0
		if t.Seconds() > 0 {
			time.Sleep(t)
		}

		node, err := next()
		if err != nil && err == selector.ErrNotFound {
			return nil, errors.NotFound("go.micro.client", "service %s: %v", request.Service(), err.Error())
		} else if err != nil {
			return nil, errors.InternalServerError("go.micro.client", "error getting next %s node: %v", request.Service(), err.Error())
		}

		address := node.Address
		if node.Port > 0 {
			address = fmt.Sprintf("%s:%d", address, node.Port)
		}

		stream, err := r.stream(ctx, address, request, callOpts)
		r.opts.Selector.Mark(request.Service(), node, err)
		return stream, err
	}

	type response struct {
		stream Stream
		err    error
	}

	ch := make(chan response, callOpts.Retries)
	var grr error

	for i := 0; i <= callOpts.Retries; i++ {
		go func() {
			s, err := call(i)
			ch <- response{s, err}
		}()

		select {
		case <-ctx.Done():
			return nil, errors.New("go.micro.client", fmt.Sprintf("call timeout: %v", ctx.Err()), 408)
		case rsp := <-ch:
			// if the call succeeded lets bail early
			if rsp.err == nil {
				return rsp.stream, nil
			}

			retry, rerr := callOpts.Retry(ctx, request, i, rsp.err)
			if rerr != nil {
				return nil, rerr
			}

			if !retry {
				return nil, rsp.err
			}

			grr = rsp.err
		}
	}

	return nil, grr
}

// [Min] 通过 broker 异步推送消息给订阅方
func (r *rpcClient) Publish(ctx context.Context, msg Message, opts ...PublishOption) error {
	// [Min] 从 context 中获取 metadata
	md, ok := metadata.FromContext(ctx)
	if !ok {
		md = make(map[string]string)
	}
	// [Min] 根据 message 设置 content-type
	md["Content-Type"] = msg.ContentType()

	// encode message body
	// [Min] 编码器的生成函数 cf，cf 的输入时一个 rwc
	cf, err := r.newCodec(msg.ContentType())
	if err != nil {
		return errors.InternalServerError("go.micro.client", err.Error())
	}
	// [Min] bytes.Buffer 本身没有实现 Closer，
	// [Min] 所以这里在外面套了一层 buffer 结构，该结构实现了 Closer
	b := &buffer{bytes.NewBuffer(nil)}
	// [Min] 写入 payload 到编码器
	if err := cf(b).Write(&codec.Message{Type: codec.Publication}, msg.Payload()); err != nil {
		return errors.InternalServerError("go.micro.client", err.Error())
	}
	r.once.Do(func() {
		r.opts.Broker.Connect() // [Min] 理论上在Server start 的时候就已经连过 broker 了
	})

	// [Min] 构造 broker.Message，其中 Body 就是 payload，调用 broker 的 Publish 推送消息
	return r.opts.Broker.Publish(msg.Topic(), &broker.Message{
		Header: md,
		Body:   b.Bytes(),
	})
}

// [Min] Message 是异步传输的消息，旧版是 Publication
func (r *rpcClient) NewMessage(topic string, message interface{}, opts ...MessageOption) Message {
	return newMessage(topic, message, r.opts.ContentType, opts...)
}

// [Min] 根据输入构造 client 可以在 Call 方法中发起的请求 Request
func (r *rpcClient) NewRequest(service, method string, request interface{}, reqOpts ...RequestOption) Request {
	return newRequest(service, method, request, r.opts.ContentType, reqOpts...)
}

func (r *rpcClient) String() string {
	return "rpc"
}
