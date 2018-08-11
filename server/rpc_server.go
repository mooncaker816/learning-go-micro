package server

import (
	"context"
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/micro/go-log"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/codec"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/transport"

	"github.com/micro/util/go/lib/addr"
)

// [Min] rpcServer
type rpcServer struct {
	rpc  *server // [Min] 内部的 server
	exit chan chan error

	sync.RWMutex
	opts        Options // [Min] rpcServer 参数
	handlers    map[string]Handler
	subscribers map[*subscriber][]broker.Subscriber
	// used for first registration
	registered bool
	// graceful exit
	wg sync.WaitGroup // [Min] 用来等待每一个 request 的完成
}

// [Min] 初始化 rpcServer
func newRpcServer(opts ...Option) Server {
	// [Min] 初始化 server 的 Options
	options := newOptions(opts...)
	return &rpcServer{
		opts: options,
		// [Min] 初始化内部的 server
		rpc: &server{
			name:         options.Name,
			serviceMap:   make(map[string]*service),
			hdlrWrappers: options.HdlrWrappers,
		},
		handlers:    make(map[string]Handler),
		subscribers: make(map[*subscriber][]broker.Subscriber),
		exit:        make(chan chan error),
	}
}

// [Min] 描述了获取一个 TCPConn（Socket） 之后，对该 TCPConn 如何处理
func (s *rpcServer) accept(sock transport.Socket) {
	defer func() {
		// close socket
		sock.Close()

		if r := recover(); r != nil {
			log.Log("panic recovered: ", r)
			log.Log(string(debug.Stack()))
		}
	}()

	for {
		var msg transport.Message
		// [Min] 读取对端发来的 request，映射并存储到 msg
		if err := sock.Recv(&msg); err != nil {
			return
		}

		// add to wait group
		// [Min] 每读一个 request 就加1
		s.wg.Add(1)

		// we use this Timeout header to set a server deadline
		// [Min] msg 的 Header 就是 request 的 header
		// [Min] 从 request 的 header 中获取 timout 和 content-type 信息
		to := msg.Header["Timeout"]
		// we use this Content-Type header to identify the codec needed
		ct := msg.Header["Content-Type"]

		// [Min] 根据 content-type 获取对应的 codec 生成器
		cf, err := s.newCodec(ct)
		// TODO: needs better error handling
		// [Min] 如果无法获取 codec 生成器，发送错误信息给对端，完成该 request 的处理，返回
		if err != nil {
			sock.Send(&transport.Message{
				Header: map[string]string{
					"Content-Type": "text/plain",
				},
				Body: []byte(err.Error()),
			})
			s.wg.Done()
			return
		}

		// [Min] 获取对 msg 的编码器
		codec := newRpcPlusCodec(&msg, sock, cf)

		// strip our headers
		// [Min] 拷贝一份 msg 的 header，然后从中删除 Content-Type，Timeout
		hdr := make(map[string]string)
		for k, v := range msg.Header {
			hdr[k] = v
		}
		delete(hdr, "Content-Type")
		delete(hdr, "Timeout")

		// [Min] 新建一个 context 将剩余的 header 加入其中
		ctx := metadata.NewContext(context.Background(), hdr)

		// set the timeout if we have it
		// [Min] 通过 context 设置处理该 request 的 timeout 时间
		if len(to) > 0 {
			if n, err := strconv.ParseUint(to, 10, 64); err == nil {
				ctx, _ = context.WithTimeout(ctx, time.Duration(n))
			}
		}

		// TODO: needs better error handling
		// [Min] 处理 request
		if err := s.rpc.serveRequest(ctx, codec, ct); err != nil {
			s.wg.Done()
			log.Logf("Unexpected error serving request, closing socket: %v", err)
			return
		}
		// [Min] 完成 request
		s.wg.Done()
	}
}

// [Min] 根据内容类型，返回对应的编码解码生成器
func (s *rpcServer) newCodec(contentType string) (codec.NewCodec, error) {
	// [Min] 优先从 server 的 Options 中查找
	if cf, ok := s.opts.Codecs[contentType]; ok {
		return cf, nil
	}
	// [Min] 如果 server 的 Options 中没有设置，那么从默认的 defaultCodecs 查找
	if cf, ok := defaultCodecs[contentType]; ok {
		return cf, nil
	}
	return nil, fmt.Errorf("Unsupported Content-Type: %s", contentType)
}

// [Min] 上读锁，获取 server 的 Options
func (s *rpcServer) Options() Options {
	s.RLock()
	opts := s.opts
	s.RUnlock()
	return opts
}

// [Min] 上写锁，按照 Option 函数，对 server 的 Options 进行修改
func (s *rpcServer) Init(opts ...Option) error {
	s.Lock()
	for _, opt := range opts {
		opt(&s.opts)
	}
	// update internal server
	// [Min] 同时按最新的Name，HdlrWrappers 修改内部的 server，serviceMap 不变
	s.rpc = &server{
		name:         s.opts.Name,
		serviceMap:   s.rpc.serviceMap, // [Min] serviceMap 没有变
		hdlrWrappers: s.opts.HdlrWrappers,
	}
	s.Unlock()
	return nil
}

// [Min] 由 Go 的 handler 实例 h，以及 HandlerOption 函数构造 rpcHandler
func (s *rpcServer) NewHandler(h interface{}, opts ...HandlerOption) Handler {
	return newRpcHandler(h, opts...)
}

// [Min] 将 rpcHandler 并更新到 s.handlers 中
func (s *rpcServer) Handle(h Handler) error {
	s.Lock()
	defer s.Unlock()

	// [Min] 将 Go handler 实例类型的方法注册到内部 server 的 serviceMap 中
	// [Min] serviceMap 的 key 为 Go handler 实例类型的名称，value 为内部的 service
	// [Min] 内部 service 相当于该 Go handler 实例类型的所有可导出的方法在 methodType 的映射
	if err := s.rpc.register(h.Handler()); err != nil {
		return err
	}

	// [Min] 注册完成后，将 rpcHandler 添加到 rpcServer 的 handlers 中
	// [Min] key : rpcHandler 对应的 Go handler 的实例类型的名称
	// [Min] value : rpcHandler
	s.handlers[h.Name()] = h

	return nil
}

// [Min] 新建一个 subscriber 类型实例
func (s *rpcServer) NewSubscriber(topic string, sb interface{}, opts ...SubscriberOption) Subscriber {
	return newSubscriber(topic, sb, opts...)
}

// [Min] 将 subscriber 注册到 server 的 subscribers 中，只是占坑（map 的 key），值暂时为 nil
func (s *rpcServer) Subscribe(sb Subscriber) error {
	sub, ok := sb.(*subscriber)
	if !ok {
		return fmt.Errorf("invalid subscriber: expected *subscriber")
	}
	if len(sub.handlers) == 0 {
		return fmt.Errorf("invalid subscriber: no handler functions")
	}

	// [Min] 验证 subscriber 是否有效
	if err := validateSubscriber(sb); err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()
	// [Min] 检查是否已经有了
	_, ok = s.subscribers[sub]
	if ok {
		return fmt.Errorf("subscriber %v already exists", s)
	}
	// [Min] 先占坑，在 server 注册的最后再完成
	// [Min] subscriber -> SubscriberFunc -> broker.Handler
	s.subscribers[sub] = nil
	return nil
}

func (s *rpcServer) Register() error {
	// parse address for host, port
	config := s.Options()
	var advt, host string
	var port int

	// check the advertise address first
	// if it exists then use it, otherwise
	// use the address
	if len(config.Advertise) > 0 {
		advt = config.Advertise
	} else {
		advt = config.Address
	}

	parts := strings.Split(advt, ":")
	if len(parts) > 1 {
		host = strings.Join(parts[:len(parts)-1], ":")
		port, _ = strconv.Atoi(parts[len(parts)-1])
	} else {
		host = parts[0]
	}

	addr, err := addr.Extract(host)
	if err != nil {
		return err
	}

	// register service
	node := &registry.Node{
		Id:       config.Name + "-" + config.Id,
		Address:  addr,
		Port:     port,
		Metadata: config.Metadata,
	}

	node.Metadata["transport"] = config.Transport.String()
	node.Metadata["broker"] = config.Broker.String()
	node.Metadata["server"] = s.String()
	node.Metadata["registry"] = config.Registry.String()

	s.RLock()
	// Maps are ordered randomly, sort the keys for consistency
	var handlerList []string
	for n, e := range s.handlers {
		// Only advertise non internal handlers
		if !e.Options().Internal {
			handlerList = append(handlerList, n)
		}
	}
	sort.Strings(handlerList)

	var subscriberList []*subscriber
	for e := range s.subscribers {
		// Only advertise non internal subscribers
		if !e.Options().Internal {
			subscriberList = append(subscriberList, e)
		}
	}
	sort.Slice(subscriberList, func(i, j int) bool {
		return subscriberList[i].topic > subscriberList[j].topic
	})

	var endpoints []*registry.Endpoint
	for _, n := range handlerList {
		endpoints = append(endpoints, s.handlers[n].Endpoints()...)
	}
	for _, e := range subscriberList {
		endpoints = append(endpoints, e.Endpoints()...)
	}
	s.RUnlock()

	service := &registry.Service{
		Name:      config.Name,
		Version:   config.Version,
		Nodes:     []*registry.Node{node},
		Endpoints: endpoints,
	}

	s.Lock()
	registered := s.registered
	s.Unlock()

	if !registered {
		log.Logf("Registering node: %s", node.Id)
	}

	// create registry options
	rOpts := []registry.RegisterOption{registry.RegisterTTL(config.RegisterTTL)}

	if err := config.Registry.Register(service, rOpts...); err != nil {
		return err
	}

	// already registered? don't need to register subscribers
	if registered {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	s.registered = true

	// [Min] 正式完成之前占完坑的 subscriber 的注册
	for sb, _ := range s.subscribers {
		// [Min] subscriber -> SubscriberFunc -> broker.Handler
		handler := s.createSubHandler(sb, s.opts)
		var opts []broker.SubscribeOption
		// [Min] 更新 broker queue 信息
		if queue := sb.Options().Queue; len(queue) > 0 {
			opts = append(opts, broker.Queue(queue))
		}

		sub, err := config.Broker.Subscribe(sb.Topic(), handler, opts...)
		if err != nil {
			return err
		}
		s.subscribers[sb] = []broker.Subscriber{sub}
	}

	return nil
}

func (s *rpcServer) Deregister() error {
	config := s.Options()
	var advt, host string
	var port int

	// check the advertise address first
	// if it exists then use it, otherwise
	// use the address
	if len(config.Advertise) > 0 {
		advt = config.Advertise
	} else {
		advt = config.Address
	}

	parts := strings.Split(advt, ":")
	if len(parts) > 1 {
		host = strings.Join(parts[:len(parts)-1], ":")
		port, _ = strconv.Atoi(parts[len(parts)-1])
	} else {
		host = parts[0]
	}

	addr, err := addr.Extract(host)
	if err != nil {
		return err
	}

	node := &registry.Node{
		Id:      config.Name + "-" + config.Id,
		Address: addr,
		Port:    port,
	}

	service := &registry.Service{
		Name:    config.Name,
		Version: config.Version,
		Nodes:   []*registry.Node{node},
	}

	log.Logf("Deregistering node: %s", node.Id)
	if err := config.Registry.Deregister(service); err != nil {
		return err
	}

	s.Lock()

	if !s.registered {
		s.Unlock()
		return nil
	}

	s.registered = false

	for sb, subs := range s.subscribers {
		for _, sub := range subs {
			log.Logf("Unsubscribing from topic: %s", sub.Topic())
			sub.Unsubscribe()
		}
		s.subscribers[sb] = nil
	}

	s.Unlock()
	return nil
}

// [Min] 开启 rpcServer，
// [Min] 1. 开启 goroutine 监听并等待 request 的到来进行处理，
// [Min] 2. 同时开启 goroutine 等待 Server 结束处理
// [Min] 3. 连接 Broker
func (s *rpcServer) Start() error {
	registerDebugHandler(s)
	config := s.Options()

	// [Min] 用配置好的 Transport 监听设定的地址
	ts, err := config.Transport.Listen(config.Address)
	if err != nil {
		return err
	}

	log.Logf("Listening on %s", ts.Addr())
	s.Lock()
	// swap address
	addr := s.opts.Address // [Min] 暂存原始的地址
	s.opts.Address = ts.Addr()
	s.Unlock()

	// [Min] 等待连接并进行处理，s.accept 就是对 conn 的处理
	go ts.Accept(s.accept)

	go func() {
		// wait for exit
		// [Min] 执行 rpcServer.Stop 的时候会向 s.exit 发送一个chan error，
		// [Min] 并且发送方会等待，直到从该 chan 中返回数据，表明 server stop 完成
		ch := <-s.exit

		// wait for requests to finish
		// [Min] 如果有 wait flag，需要等待所有当前 request 都完成后再 stop
		if wait(s.opts.Context) {
			s.wg.Wait()
		}

		// close transport listener
		// [Min] 关闭
		ch <- ts.Close()

		// disconnect the broker
		// [Min] 与 Broker 断开连接
		config.Broker.Disconnect()

		s.Lock()
		// swap back address
		s.opts.Address = addr // [Min] 还原原始地址
		s.Unlock()
	}()

	// TODO: subscribe to cruft
	// [Min] 连接 Broker，如果是 httpBroker 其实就是相当于为每一个 server 配另一专门用于代理异步消息发送接收的服务器，让其可以接受消息，并且转发给 subscriber
	// [Min] 其他类型的 broker 则更像是一个公用的消息中转站
	return config.Broker.Connect()
}

func (s *rpcServer) Stop() error {
	ch := make(chan error)
	s.exit <- ch
	return <-ch
}

func (s *rpcServer) String() string {
	return "rpc"
}
