package broker

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/micro/go-log"
	"github.com/micro/go-micro/broker/codec/json"
	merr "github.com/micro/go-micro/errors"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-rcache"
	maddr "github.com/micro/util/go/lib/addr"
	mnet "github.com/micro/util/go/lib/net"
	mls "github.com/micro/util/go/lib/tls"
	"github.com/pborman/uuid"
)

// HTTP Broker is a point to point async broker
// [Min] broker 就是一个中转站，Service 的 Server 与之通过 http 连接通讯，
// [Min] broker 再根据注册的 topic 和对应的 subscriber 进行消息分发
// [Min] 一个 httpBroker 实例代表了一对连接双方
type httpBroker struct {
	id      string  // [Min] broker id
	address string  // [Min] broker 自身服务器监听的地址
	opts    Options // [Min] broker 的参数

	mux *http.ServeMux // [Min]  broker 作为一个 http server 的路由

	c *http.Client      // [Min] 当 broker 分发消息时，对应分发对象来说，broker 是对端的一个 client
	r registry.Registry // [Min] registry

	sync.RWMutex
	subscribers map[string][]*httpSubscriber // [Min] topic 对应的 subscriber
	running     bool                         // [Min] broker 是否在运行
	exit        chan chan error              // [Min] 退出通道的通道
}

// [Min] 订阅方
type httpSubscriber struct {
	opts  SubscribeOptions
	id    string            // [Min] 订阅方 id
	topic string            // [Min] 订阅的 topic
	fn    Handler           // [Min] 对 publication 的处理
	svc   *registry.Service // [Min] 注册的信息
	hb    *httpBroker       // [Min] 对应的 Broker
}

// [Min] broker 需要转发的消息
type httpPublication struct {
	m *Message // [Min] 消息实体
	t string   // [Min] topic
}

var (
	DefaultSubPath   = "/_sub" // [Min] broker 默认路由
	broadcastVersion = "ff.http.broadcast"
	registerTTL      = time.Minute
	registerInterval = time.Second * 30 // [Min] subscriber 心跳
)

func init() {
	rand.Seed(time.Now().Unix())
}

// [Min] broker 作为 client 时的 transport
func newTransport(config *tls.Config) *http.Transport {
	if config == nil {
		config = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     config,
	}
	runtime.SetFinalizer(&t, func(tr **http.Transport) {
		(*tr).CloseIdleConnections()
	})
	return t
}

// [Min] 新建一个 httpBroker
func newHttpBroker(opts ...Option) Broker {
	options := Options{
		Codec:   json.NewCodec(), // [Min] json codec
		Context: context.TODO(),
	}

	for _, o := range opts {
		o(&options)
	}

	// set address
	addr := ":0"
	if len(options.Addrs) > 0 && len(options.Addrs[0]) > 0 {
		addr = options.Addrs[0]
	}

	// get registry
	reg, ok := options.Context.Value(registryKey).(registry.Registry)
	if !ok {
		reg = registry.DefaultRegistry
	}

	h := &httpBroker{
		id:      "broker-" + uuid.NewUUID().String(),
		address: addr,
		opts:    options,
		r:       reg,
		// [Min] broker 作为 client 通过 https 分发消息
		c:           &http.Client{Transport: newTransport(options.TLSConfig)},
		subscribers: make(map[string][]*httpSubscriber),
		exit:        make(chan chan error),
		mux:         http.NewServeMux(),
	}

	// [Min] 默认路由 /_sub
	h.mux.Handle(DefaultSubPath, h)
	return h
}

func (h *httpPublication) Ack() error {
	return nil
}

func (h *httpPublication) Message() *Message {
	return h.m
}

func (h *httpPublication) Topic() string {
	return h.t
}

func (h *httpSubscriber) Options() SubscribeOptions {
	return h.opts
}

func (h *httpSubscriber) Topic() string {
	return h.topic
}

func (h *httpSubscriber) Unsubscribe() error {
	return h.hb.unsubscribe(h)
}

func (h *httpBroker) subscribe(s *httpSubscriber) error {
	h.Lock()
	defer h.Unlock()

	if err := h.r.Register(s.svc, registry.RegisterTTL(registerTTL)); err != nil {
		return err
	}

	h.subscribers[s.topic] = append(h.subscribers[s.topic], s)
	return nil
}

func (h *httpBroker) unsubscribe(s *httpSubscriber) error {
	h.Lock()
	defer h.Unlock()

	var subscribers []*httpSubscriber

	// look for subscriber
	for _, sub := range h.subscribers[s.topic] {
		// deregister and skip forward
		if sub.id == s.id {
			h.r.Deregister(sub.svc)
			continue
		}
		// keep subscriber
		subscribers = append(subscribers, sub)
	}

	// set subscribers
	h.subscribers[s.topic] = subscribers

	return nil
}

// [Min] 周期性注册 subscriber，并等待结束处理
func (h *httpBroker) run(l net.Listener) {
	t := time.NewTicker(registerInterval)
	defer t.Stop()

	for {
		select {
		// heartbeat for each subscriber
		case <-t.C:
			h.RLock()
			for _, subs := range h.subscribers {
				for _, sub := range subs {
					h.r.Register(sub.svc, registry.RegisterTTL(registerTTL))
				}
			}
			h.RUnlock()
		// received exit signal
		// [Min] 收取结束通道，收到了就关闭连接，然后往收到的通道中发送信号
		// [Min] 再注销 subscriber
		case ch := <-h.exit:
			ch <- l.Close()
			h.RLock()
			for _, subs := range h.subscribers {
				for _, sub := range subs {
					h.r.Deregister(sub.svc)
				}
			}
			h.RUnlock()
			return
		}
	}
}

// [Min] serve http 的实现
func (h *httpBroker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// [Min] Broker 只允许 POST 方法
	if req.Method != "POST" {
		err := merr.BadRequest("go.micro.broker", "Method not allowed")
		http.Error(w, err.Error(), http.StatusMethodNotAllowed)
		return
	}
	defer req.Body.Close()

	// [Min] 解析并读取 request body
	req.ParseForm()

	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		errr := merr.InternalServerError("go.micro.broker", "Error reading request body: %v", err)
		w.WriteHeader(500)
		w.Write([]byte(errr.Error()))
		return
	}

	// [Min] 将 request body 通过 codec 反序列化为 message
	var m *Message
	if err = h.opts.Codec.Unmarshal(b, &m); err != nil {
		errr := merr.InternalServerError("go.micro.broker", "Error parsing request body: %v", err)
		w.WriteHeader(500)
		w.Write([]byte(errr.Error()))
		return
	}

	// [Min] 获取 header 中的 topic，key 为:topic，然后从 header 中删除该 topic
	topic := m.Header[":topic"]
	delete(m.Header, ":topic")

	// [Min] Broker 接受的 request header 中必须有 topic
	if len(topic) == 0 {
		errr := merr.InternalServerError("go.micro.broker", "Topic not found")
		w.WriteHeader(500)
		w.Write([]byte(errr.Error()))
		return
	}

	// [Min] 用获取的 message 和 topic 构造 httpPublication
	p := &httpPublication{m: m, t: topic}
	// [Min] 从 request 中获取 id，表示该 topic 的目标 subsriber 的序号
	id := req.Form.Get("id")

	h.RLock()
	// [Min] 通过 topic 来获取所有 httpSubscriber 的列表，
	// [Min] 再通过 id 获取具体的待发送的目标订阅方，
	// [Min]
	for _, subscriber := range h.subscribers[topic] {
		if id == subscriber.id {
			// sub is sync; crufty rate limiting
			// so we don't hose the cpu
			// [Min] 发送 publication
			subscriber.fn(p)
		}
	}
	h.RUnlock()
}

// [Min] 获取 broker 的监听地址
func (h *httpBroker) Address() string {
	h.RLock()
	defer h.RUnlock()
	return h.address
}

// [Min] 开启 httpBroker，相当于开启一个 http server
func (h *httpBroker) Connect() error {

	h.RLock()
	if h.running {
		h.RUnlock()
		return nil
	}
	h.RUnlock()

	h.Lock()
	defer h.Unlock()

	var l net.Listener
	var err error

	// [Min] 监听地址，获得 listener，和 HttpTransport 的监听方式一样
	if h.opts.Secure || h.opts.TLSConfig != nil {
		config := h.opts.TLSConfig

		fn := func(addr string) (net.Listener, error) {
			if config == nil {
				hosts := []string{addr}

				// check if its a valid host:port
				if host, _, err := net.SplitHostPort(addr); err == nil {
					if len(host) == 0 {
						hosts = maddr.IPs()
					} else {
						hosts = []string{host}
					}
				}

				// generate a certificate
				cert, err := mls.Certificate(hosts...)
				if err != nil {
					return nil, err
				}
				config = &tls.Config{Certificates: []tls.Certificate{cert}}
			}
			return tls.Listen("tcp", addr, config)
		}

		l, err = mnet.Listen(h.address, fn)
	} else {
		fn := func(addr string) (net.Listener, error) {
			return net.Listen("tcp", addr)
		}

		l, err = mnet.Listen(h.address, fn)
	}

	if err != nil {
		return err
	}

	log.Logf("Broker Listening on %s", l.Addr().String())
	addr := h.address // [Min] 暂存原始 address，更新当前监听的实际 address
	h.address = l.Addr().String()

	// [Min] serve http
	go http.Serve(l, h.mux)
	go func() {
		// [Min] 1. 为 subscriber 作心跳，按 registerInterval 30s 反复注册
		// [Min] 2. 收取表示退出的结束通道，关闭连接，向结束通道发送信号，通知调用方
		h.run(l)
		// [Min] 此时连接已关闭，还原原始 address
		h.Lock()
		h.address = addr
		h.Unlock()
	}()

	// get registry
	// [Min] 从 context 中获取 registry
	reg, ok := h.opts.Context.Value(registryKey).(registry.Registry)
	// [Min] 没有就使用默认的 registry
	if !ok {
		reg = registry.DefaultRegistry
	}
	// set rcache
	// [Min] 设置 registry 缓存
	h.r = rcache.New(reg)

	// set running
	// [Min] 更新 running 为 true
	h.running = true
	return nil
}

// [Min] 关闭 broker
func (h *httpBroker) Disconnect() error {

	h.RLock()
	if !h.running {
		h.RUnlock()
		return nil
	}
	h.RUnlock()

	h.Lock()
	defer h.Unlock()

	// stop rcache
	// [Min] 暂停 cache
	rc, ok := h.r.(rcache.Cache)
	if ok {
		rc.Stop()
	}

	// exit and return err
	// [Min] 发送退出通道，h.run(l) 的 goroutine 收到后会先关闭连接，然后向退出通道发送信息
	ch := make(chan error)
	h.exit <- ch
	// [Min] 从退出通道中收到信息，表示连接已关闭
	err := <-ch

	// set not running
	// [Min] 设置 running 为 false
	h.running = false
	return err
}

func (h *httpBroker) Init(opts ...Option) error {
	h.RLock()
	if h.running {
		h.RUnlock()
		return errors.New("cannot init while connected")
	}
	h.RUnlock()

	h.Lock()
	defer h.Unlock()

	for _, o := range opts {
		o(&h.opts)
	}

	if len(h.id) == 0 {
		h.id = "broker-" + uuid.NewUUID().String()
	}

	// get registry
	reg, ok := h.opts.Context.Value(registryKey).(registry.Registry)
	if !ok {
		reg = registry.DefaultRegistry
	}

	// get rcache
	if rc, ok := h.r.(rcache.Cache); ok {
		rc.Stop()
	}

	// set registry
	h.r = rcache.New(reg)

	return nil
}

func (h *httpBroker) Options() Options {
	return h.opts
}

// [Min] 推送消息
func (h *httpBroker) Publish(topic string, msg *Message, opts ...PublishOption) error {
	h.RLock()
	// [Min] 从 registry 中获取注册的 topic
	s, err := h.r.GetService("topic:" + topic)
	if err != nil {
		h.RUnlock()
		return err
	}
	h.RUnlock()

	m := &Message{
		Header: make(map[string]string),
		Body:   msg.Body,
	}

	for k, v := range msg.Header {
		m.Header[k] = v
	}

	m.Header[":topic"] = topic

	b, err := h.opts.Codec.Marshal(m) // [Min] json 序列化
	if err != nil {
		return err
	}

	// [Min] 推送函数，node 是订阅方，b 是
	pub := func(node *registry.Node, b []byte) {
		scheme := "http"

		// check if secure is added in metadata
		if node.Metadata["secure"] == "true" {
			scheme = "https"
		}

		vals := url.Values{}
		vals.Add("id", node.Id)

		// [Min] 发起 https 推送消息
		uri := fmt.Sprintf("%s://%s:%d%s?%s", scheme, node.Address, node.Port, DefaultSubPath, vals.Encode())
		r, err := h.c.Post(uri, "application/json", bytes.NewReader(b))
		if err == nil {
			io.Copy(ioutil.Discard, r.Body)
			r.Body.Close()
		}
	}

	for _, service := range s {
		// only process if we have nodes
		if len(service.Nodes) == 0 {
			continue
		}

		switch service.Version {
		// broadcast version means broadcast to all nodes
		case broadcastVersion:
			// [Min] 推送给每一个订阅方
			for _, node := range service.Nodes {
				// publish async
				go pub(node, b)
			}
		default:
			// select node to publish to
			// [Min] 随机推送给一个订阅方
			node := service.Nodes[rand.Int()%len(service.Nodes)]

			// publish async
			go pub(node, b)
		}
	}

	return nil
}

func (h *httpBroker) Subscribe(topic string, handler Handler, opts ...SubscribeOption) (Subscriber, error) {
	options := newSubscribeOptions(opts...)

	// parse address for host, port
	parts := strings.Split(h.Address(), ":")
	host := strings.Join(parts[:len(parts)-1], ":")
	port, _ := strconv.Atoi(parts[len(parts)-1])

	addr, err := maddr.Extract(host)
	if err != nil {
		return nil, err
	}

	// create unique id
	id := h.id + "." + uuid.NewUUID().String()

	var secure bool

	if h.opts.Secure || h.opts.TLSConfig != nil {
		secure = true
	}

	// register service
	node := &registry.Node{
		Id:      id,
		Address: addr,
		Port:    port,
		Metadata: map[string]string{
			"secure": fmt.Sprintf("%t", secure),
		},
	}

	// check for queue group or broadcast queue
	version := options.Queue
	if len(version) == 0 {
		version = broadcastVersion
	}

	service := &registry.Service{
		Name:    "topic:" + topic,
		Version: version,
		Nodes:   []*registry.Node{node},
	}

	// generate subscriber
	subscriber := &httpSubscriber{
		opts:  options,
		hb:    h,
		id:    id,
		topic: topic,
		fn:    handler,
		svc:   service,
	}

	// subscribe now
	if err := h.subscribe(subscriber); err != nil {
		return nil, err
	}

	// return the subscriber
	return subscriber, nil
}

func (h *httpBroker) String() string {
	return "http"
}
