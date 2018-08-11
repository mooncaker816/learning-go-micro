package transport

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/micro/go-log"
	maddr "github.com/micro/util/go/lib/addr"
	mnet "github.com/micro/util/go/lib/net"
	mls "github.com/micro/util/go/lib/tls"
)

/* [Min]
0. newHTTPTransport() -> httpTransport ：得到一些 http 的配置参数

节点作为服务端：

1. httpTransport.Listen() -> httpTransportListener ：获得了一个 tcp listener
2. httpTransportListener.Accept(fn) -> httpTransportSocket ：获得一个tcp conn，并对该 conn 按 fn 进行处理
3. 对 2 中的 conn 进行 bufio.Reader 封装，对端发送的数据从此 buff 中获取
4. 返回信息直接写入 2 中的 conn

节点作为客户端：

1. httpTransport.Dial() -> httpTransportClient ：获得一个 tcp conn
2. 对 1 中的 conn 进行 bufio.Reader 封装，对端发送的数据从此 buff 中获取
3.
*/

type buffer struct {
	io.ReadWriter
}

type httpTransport struct {
	opts Options
}

type httpTransportClient struct {
	ht       *httpTransport
	addr     string
	conn     net.Conn
	dialOpts DialOptions
	once     sync.Once

	sync.Mutex
	r    chan *http.Request
	bl   []*http.Request
	buff *bufio.Reader
}

// [Min] 获取底层 TCPConn 后对 TCPConn 的封装，可以简单理解为服务端的 conn
type httpTransportSocket struct {
	ht   *httpTransport
	r    chan *http.Request
	conn net.Conn  // [Min] TCPConn
	once sync.Once // [Min] 用来控制关闭 TCPConn 后，只对 buff 进行一次清理

	sync.Mutex
	buff *bufio.Reader // [Min] 以 conn 建立的 bufio.Reader
}

// [Min] 服务器端对传统 listener 的封装
type httpTransportListener struct {
	ht       *httpTransport
	listener net.Listener
}

// [Min] 添加一个空的 Close 方法，使得 buffer 实现 io.ReaderCloser
func (b *buffer) Close() error {
	return nil
}

// [Min] 客户端根据 message 得到 header 和 body，发起 http post
func (h *httpTransportClient) Send(m *Message) error {
	header := make(http.Header)

	for k, v := range m.Header {
		header.Set(k, v)
	}

	reqB := bytes.NewBuffer(m.Body)
	defer reqB.Reset()
	buf := &buffer{
		reqB,
	}

	req := &http.Request{
		Method: "POST",
		URL: &url.URL{
			Scheme: "http",
			Host:   h.addr,
		},
		Header:        header,
		Body:          buf,
		ContentLength: int64(reqB.Len()),
		Host:          h.addr,
	}

	h.Lock()
	h.bl = append(h.bl, req)
	select {
	case h.r <- h.bl[0]:
		h.bl = h.bl[1:]
	default:
	}
	h.Unlock()

	// set timeout if its greater than 0
	if h.ht.opts.Timeout > time.Duration(0) {
		h.conn.SetDeadline(time.Now().Add(h.ht.opts.Timeout))
	}

	return req.Write(h.conn)
}

// [Min] 接收对端返回到 message
func (h *httpTransportClient) Recv(m *Message) error {
	if m == nil {
		return errors.New("message passed in is nil")
	}

	var r *http.Request
	// [Min] 如果不是流模式，Recv 紧跟着 Send 之后调用，取出与之对应的 request 后再读取 response
	if !h.dialOpts.Stream {
		rc, ok := <-h.r
		if !ok {
			return io.EOF
		}
		r = rc
	}

	h.Lock()
	defer h.Unlock()
	if h.buff == nil {
		return io.EOF
	}

	// set timeout if its greater than 0
	if h.ht.opts.Timeout > time.Duration(0) {
		h.conn.SetDeadline(time.Now().Add(h.ht.opts.Timeout))
	}

	rsp, err := http.ReadResponse(h.buff, r)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	b, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return err
	}

	if rsp.StatusCode != 200 {
		return errors.New(rsp.Status + ": " + string(b))
	}

	m.Body = b

	if m.Header == nil {
		m.Header = make(map[string]string)
	}

	for k, v := range rsp.Header {
		if len(v) > 0 {
			m.Header[k] = v[0]
		} else {
			m.Header[k] = ""
		}
	}

	return nil
}

func (h *httpTransportClient) Close() error {
	err := h.conn.Close()
	h.once.Do(func() {
		h.Lock()
		h.buff.Reset(nil)
		h.buff = nil
		h.Unlock()
		close(h.r)
	})
	return err
}

// [Min] 从底层连接中获取对端发来的 request 然后映射到 message，
// [Min] 再将 request 发送到 socket 的请求通道
func (h *httpTransportSocket) Recv(m *Message) error {
	if m == nil {
		return errors.New("message passed in is nil")
	}

	// set timeout if its greater than 0
	// [Min] 根据 httpTransport 的 timeout 参数对 TCPConn 设置 deadline
	if h.ht.opts.Timeout > time.Duration(0) {
		h.conn.SetDeadline(time.Now().Add(h.ht.opts.Timeout))
	}

	// [Min] 读取对端发来的 request
	r, err := http.ReadRequest(h.buff)
	if err != nil {
		return err
	}

	// [Min] 读取 request 的 body 并关闭 body
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	r.Body.Close()
	// [Min] 将 body 赋给 message 中的 body 字段
	m.Body = b

	if m.Header == nil {
		m.Header = make(map[string]string)
	}

	// [Min] 将 request 中的 Header (map[string][]string) 存到 message 的 Header 字段，
	// [Min] 但是每一种 Header 只存第一个值
	for k, v := range r.Header {
		if len(v) > 0 {
			m.Header[k] = v[0]
		} else {
			m.Header[k] = ""
		}
	}

	// [Min] 将 r 发给 socket 的请求通道，用作临时存储，待再次需要该 request 的时候从通道中获取
	select {
	case h.r <- r:
	default:
	}

	return nil
}

// [Min] 服务器端将 message 映射成 request 对应的 response，并发送到对端
func (h *httpTransportSocket) Send(m *Message) error {
	b := bytes.NewBuffer(m.Body)
	defer b.Reset()

	// [Min] 从 socket 的请求通道中获取之前临时存储的 request
	r := <-h.r

	// [Min] 构造 response
	rsp := &http.Response{
		Header:        r.Header,   // [Min] header 先照抄 request 的 header
		Body:          &buffer{b}, // [Min] message 的 Body 作为 response 的 body
		Status:        "200 OK",
		StatusCode:    200,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		ContentLength: int64(len(m.Body)),
	}

	// [Min] message 的 header 更新到 response 的 header
	for k, v := range m.Header {
		rsp.Header.Set(k, v)
	}

	// [Min] 将 r 再次发送到 socket 的请求通道中，用作临时存储
	select {
	case h.r <- r:
	default:
	}

	// set timeout if its greater than 0
	// [Min] 根据 httpTransport 的 timeout 参数对 TCPConn 设置 deadline
	if h.ht.opts.Timeout > time.Duration(0) {
		h.conn.SetDeadline(time.Now().Add(h.ht.opts.Timeout))
	}

	// [Min] 将 response 发送给对端
	return rsp.Write(h.conn)
}

// [Min] 服务器端发送 500 错误给对端，header，body 采用 message 中的 header，body
func (h *httpTransportSocket) error(m *Message) error {
	b := bytes.NewBuffer(m.Body)
	defer b.Reset()
	rsp := &http.Response{
		Header:        make(http.Header),
		Body:          &buffer{b},
		Status:        "500 Internal Server Error",
		StatusCode:    500,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		ContentLength: int64(len(m.Body)),
	}

	for k, v := range m.Header {
		rsp.Header.Set(k, v)
	}

	return rsp.Write(h.conn)
}

// [Min] 服务器端关闭 socket
func (h *httpTransportSocket) Close() error {
	// [Min] 关闭底层 TCPConn，并要同时将建立在该 TCPConn 上的 bufio reader 重置为 nil
	err := h.conn.Close()
	h.once.Do(func() {
		h.Lock()
		h.buff.Reset(nil)
		h.buff = nil
		h.Unlock()
	})
	return err
}

func (h *httpTransportListener) Addr() string {
	return h.listener.Addr().String()
}

func (h *httpTransportListener) Close() error {
	return h.listener.Close()
}

// [Min] 从 listener 获取连接，然后开启 goroutine 调用 fn(Socket)，这里的 Socket 就是 httpTransportSocket，
// [Min] 其中包含了 TCPConn，httpTransport 等，相当于传统的 go handle(conn)
func (h *httpTransportListener) Accept(fn func(Socket)) error {
	var tempDelay time.Duration

	for {
		// [Min] 通过 httpTransportListener 中的 listener 来获取连接
		// [Min] 如果获取报错，且为临时错误，那么尝试按一定 delay 来重新获取连接，知道
		c, err := h.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Logf("http: Accept error: %v; retrying in %v\n", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		// [Min] 对底层 TCPConn 的封装，构造 httpTransportSocket
		sock := &httpTransportSocket{
			ht:   h.ht,                        // [Min] httpTransport
			conn: c,                           // [Min] TCPConn
			buff: bufio.NewReader(c),          // [Min] 以 TCPConn 为基础的 bufio reader
			r:    make(chan *http.Request, 1), // [Min] 带有1个缓存的非阻塞请求通道
		}

		go func() {
			// TODO: think of a better error response strategy
			defer func() {
				if r := recover(); r != nil {
					log.Log("panic recovered: ", r)
					sock.Close()
				}
			}()

			fn(sock) // [Min] 处理该连接
		}()
	}
}

func (h *httpTransport) Dial(addr string, opts ...DialOption) (Client, error) {
	dopts := DialOptions{
		Timeout: DefaultDialTimeout,
	}

	for _, opt := range opts {
		opt(&dopts)
	}

	var conn net.Conn
	var err error

	// TODO: support dial option here rather than using internal config
	if h.opts.Secure || h.opts.TLSConfig != nil {
		config := h.opts.TLSConfig
		if config == nil {
			config = &tls.Config{
				InsecureSkipVerify: true,
			}
		}
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: dopts.Timeout}, "tcp", addr, config)
	} else {
		conn, err = net.DialTimeout("tcp", addr, dopts.Timeout)
	}

	if err != nil {
		return nil, err
	}

	return &httpTransportClient{
		ht:       h,
		addr:     addr,
		conn:     conn,
		buff:     bufio.NewReader(conn),
		dialOpts: dopts,
		r:        make(chan *http.Request, 1), // [Min] 用于读取 response 时能找到匹配的 request
	}, nil
}

// [Min] 监听地址，返回 net.listener 和 httpTransport 的组合
func (h *httpTransport) Listen(addr string, opts ...ListenOption) (Listener, error) {
	var options ListenOptions
	for _, o := range opts {
		o(&options)
	}

	var l net.Listener
	var err error

	// TODO: support use of listen options
	// [Min] https
	if h.opts.Secure || h.opts.TLSConfig != nil {
		config := h.opts.TLSConfig

		// [Min] 调用 tls.Listen 获取 net.Listener 的函数
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
				// [Min] 生成自签名证书，因为是内部服务之间的 https 请求，所以可以使用自签名证书
				cert, err := mls.Certificate(hosts...)
				if err != nil {
					return nil, err
				}
				config = &tls.Config{Certificates: []tls.Certificate{cert}}
			}
			return tls.Listen("tcp", addr, config)
		}

		// [Min] mnet.Listen 中会执行 fn，获得 listener
		l, err = mnet.Listen(addr, fn)
	} else {
		// [Min] http
		fn := func(addr string) (net.Listener, error) {
			return net.Listen("tcp", addr)
		}

		// [Min] mnet.Listen 中会执行 fn，获得 listener
		l, err = mnet.Listen(addr, fn)
	}

	if err != nil {
		return nil, err
	}

	// [Min] 返回 transport 和 listener 的组合
	return &httpTransportListener{
		ht:       h,
		listener: l,
	}, nil
}

func (h *httpTransport) String() string {
	return "http"
}

func newHTTPTransport(opts ...Option) *httpTransport {
	var options Options
	for _, o := range opts {
		o(&options)
	}
	return &httpTransport{opts: options}
}
