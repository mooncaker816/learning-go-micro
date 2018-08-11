package client

import (
	"bytes"
	errs "errors"

	"github.com/micro/go-micro/codec"
	"github.com/micro/go-micro/codec/jsonrpc"
	"github.com/micro/go-micro/codec/protorpc"
	"github.com/micro/go-micro/errors"
	"github.com/micro/go-micro/transport"
)

const (
	lastStreamResponseError = "EOS"
)

// serverError represents an error that has been returned from
// the remote side of the RPC connection.
type serverError string

func (e serverError) Error() string {
	return string(e)
}

// errShutdown holds the specific error for closing/closed connections
var (
	errShutdown = errs.New("connection is shut down")
)

type rpcPlusCodec struct {
	client transport.Client
	codec  codec.Codec

	req *transport.Message
	buf *readWriteCloser
}

type readWriteCloser struct {
	wbuf *bytes.Buffer
	rbuf *bytes.Buffer
}

type clientCodec interface {
	WriteRequest(*request, interface{}) error
	ReadResponseHeader(*response) error
	ReadResponseBody(interface{}) error

	Close() error
}

type request struct {
	Service       string
	ServiceMethod string   // format: "Service.Method"
	Seq           uint64   // sequence number chosen by client
	next          *request // for free list in Server
}

type response struct {
	ServiceMethod string    // echoes that of the Request
	Seq           uint64    // echoes that of the request
	Error         string    // error, if any.
	next          *response // for free list in Server
}

var (
	defaultContentType = "application/octet-stream"

	defaultCodecs = map[string]codec.NewCodec{
		"application/json":         jsonrpc.NewCodec,
		"application/json-rpc":     jsonrpc.NewCodec,
		"application/protobuf":     protorpc.NewCodec,
		"application/proto-rpc":    protorpc.NewCodec,
		"application/octet-stream": protorpc.NewCodec,
	}
)

func (rwc *readWriteCloser) Read(p []byte) (n int, err error) {
	return rwc.rbuf.Read(p)
}

func (rwc *readWriteCloser) Write(p []byte) (n int, err error) {
	return rwc.wbuf.Write(p)
}

func (rwc *readWriteCloser) Close() error {
	rwc.rbuf.Reset()
	rwc.wbuf.Reset()
	return nil
}

func newRpcPlusCodec(req *transport.Message, client transport.Client, c codec.NewCodec) *rpcPlusCodec {
	// [Min] 读写分离的用来存储编码后的数据的实例
	rwc := &readWriteCloser{
		wbuf: bytes.NewBuffer(nil),
		rbuf: bytes.NewBuffer(nil),
	}
	r := &rpcPlusCodec{
		buf:    rwc,
		client: client,
		codec:  c(rwc), // [Min] c 是一个生成特定编码的编码器的函数，该编码器的存储空间为 rwc，读取时会将解码后的数据返回，写入时会编码后再写入
		req:    req,
	}
	return r
}

// [Min] request 包含了调用方法，序号等，用于映射成 codec.Message，（统一结构，方便编写代码映射到 protobuf 的 Request/Response ）
// [Min] 再由 Message 映射成 protobuf 的 Request
// [Min] body 是发起该 request 需要用到的输入，一般是由 protoc 生成的代码中的一个结构体实例
// [Min] Request 和 body 组合起来就是 protobuf 发起请求需要的信息，记为 c.req.Body
// [Min] 再拼出 c.req.Header，最后将 c.req 通过 transport.Client 发出去
func (c *rpcPlusCodec) WriteRequest(req *request, body interface{}) error {
	c.buf.wbuf.Reset()
	m := &codec.Message{
		Id:     req.Seq,
		Target: req.Service,
		Method: req.ServiceMethod,
		Type:   codec.Request,
		Header: map[string]string{},
	}
	// [Min] 将 m，body protobuf 序列化后写入 codec 中
	if err := c.codec.Write(m, body); err != nil {
		return errors.InternalServerError("go.micro.client.codec", err.Error())
	}
	// [Min] c.req 是一个 transpot.Message
	// [Min] 刚写入的 protobuf 当作 c.req.Body
	c.req.Body = c.buf.wbuf.Bytes()
	for k, v := range m.Header {
		c.req.Header[k] = v
	}
	// [Min] transPortClinet 的 send 方法发送请求
	if err := c.client.Send(c.req); err != nil {
		return errors.InternalServerError("go.micro.client.transport", err.Error())
	}
	return nil
}

// [Min] 接收对端的 response，并反序列化
func (c *rpcPlusCodec) ReadResponseHeader(r *response) error {
	var m transport.Message
	if err := c.client.Recv(&m); err != nil {
		return errors.InternalServerError("go.micro.client.transport", err.Error())
	}
	c.buf.rbuf.Reset()
	c.buf.rbuf.Write(m.Body) // [Min] 把 Body 写入 rwc 中
	var me codec.Message
	// [Min] 首先从 rwc 中读取 response 的 header，写入 codec.Message
	// [Min] 再由 codec.Message 映射到 response
	err := c.codec.ReadHeader(&me, codec.Response)
	r.ServiceMethod = me.Method
	r.Seq = me.Id
	r.Error = me.Error
	if err != nil {
		return errors.InternalServerError("go.micro.client.codec", err.Error())
	}
	return nil
}

// [Min] 读取 response body
func (c *rpcPlusCodec) ReadResponseBody(b interface{}) error {
	if err := c.codec.ReadBody(b); err != nil {
		return errors.InternalServerError("go.micro.client.codec", err.Error())
	}
	return nil
}

func (c *rpcPlusCodec) Close() error {
	c.buf.Close()
	c.codec.Close()
	if err := c.client.Close(); err != nil {
		return errors.InternalServerError("go.micro.client.transport", err.Error())
	}
	return nil
}
