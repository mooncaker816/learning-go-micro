package server

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Meh, we need to get rid of this shit

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/micro/go-log"
)

var (
	lastStreamResponseError = errors.New("EOS")
	// A value sent as a placeholder for the server's response value when the server
	// receives an invalid request. It is never decoded by the client since the Response
	// contains an error when it is used.
	invalidRequest = struct{}{}

	// Precompute the reflect type for error. Can't use error directly
	// because Typeof takes an empty interface value. This is annoying.
	typeOfError = reflect.TypeOf((*error)(nil)).Elem()
)

type methodType struct {
	sync.Mutex                 // protects counters
	method      reflect.Method // [Min] Go handler 实例类型的反射对象的某一个方法
	ArgType     reflect.Type   // [Min] 该方法的输入参数类型，如果是 stream，则是实现 stream 接口的类型
	ReplyType   reflect.Type   // [Min] 该方法的输出参数类型，如果是 stream，则为 nil
	ContextType reflect.Type   // [Min] 该方法的 context 类型
	stream      bool           // [Min] 是否为 stream
}

// [Min] 对服务器而言的 service，理解为一个 Go handler 提供的各种方法带来的服务
type service struct {
	// [Min]  Go handler 实例类型的名称
	name string // name of service
	// [Min] Go handler 实例的值的反射对象
	rcvr reflect.Value // receiver of methods for the service
	// [Min] Go handler 实例的类型的反射对象
	typ reflect.Type // type of the receiver
	// [Min] 将 Go handler 所有可导出的方法映射到 methodType 后，并以方法名为 key 的一个 map
	method map[string]*methodType // registered methods
}

type request struct {
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

// server represents an RPC Server.
// [Min] rpcServer 对应的内部 server
type server struct {
	name         string
	mu           sync.Mutex // protects the serviceMap
	serviceMap   map[string]*service
	reqLock      sync.Mutex // protects freeReq
	freeReq      *request
	respLock     sync.Mutex // protects freeResp
	freeResp     *response
	hdlrWrappers []HandlerWrapper
}

// Is this an exported - upper case - name?
// [Min] 判断首字符是否为大写
func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

// prepareMethod returns a methodType for the provided method or nil
// in case if the method was unsuitable.
// [Min] 将 Go Method 映射到 methodType，
// [Min] 如果是 stream，ReplyType 为 nil， ArgType 为实现了 stream 接口的类型
// [Min] 如果不是 stream，ReplyType 不为 nil，是正常的返回值类型，ArgType 为输入类型
func prepareMethod(method reflect.Method) *methodType {
	// [Min] mtype 是将 receiver 作为第一个输入参数的函数类型
	mtype := method.Type
	mname := method.Name
	var replyType, argType, contextType reflect.Type
	var stream bool

	// Method must be exported.
	// [Min] 必须可导出
	if method.PkgPath != "" {
		return nil
	}

	// [Min] receiver，context，arg，reply，其中 reply 可选
	switch mtype.NumIn() {
	case 3:
		// assuming streaming
		argType = mtype.In(2) // [Min] 这里起的名字容易引起误解！！！，应该和 extractor.go 是一样的
		contextType = mtype.In(1)
		stream = true
	case 4:
		// method that takes a context
		argType = mtype.In(2)
		replyType = mtype.In(3)
		contextType = mtype.In(1)
	default:
		log.Log("method", mname, "of", mtype, "has wrong number of ins:", mtype.NumIn())
		return nil
	}

	if stream {
		// check stream type
		streamType := reflect.TypeOf((*Stream)(nil)).Elem()
		// [Min] 检查最后一个参数 argType（mtype.In(2)）是否实现了 Stream 接口
		if !argType.Implements(streamType) {
			log.Log(mname, "argument does not implement Stream interface:", argType)
			return nil
		}
	} else {
		// if not stream check the replyType

		// First arg need not be a pointer.
		if !isExportedOrBuiltinType(argType) {
			log.Log(mname, "argument type not exported:", argType)
			return nil
		}

		if replyType.Kind() != reflect.Ptr {
			log.Log("method", mname, "reply type not a pointer:", replyType)
			return nil
		}

		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			log.Log("method", mname, "reply type not exported:", replyType)
			return nil
		}
	}

	// Method needs one out.
	if mtype.NumOut() != 1 {
		log.Log("method", mname, "has wrong number of outs:", mtype.NumOut())
		return nil
	}
	// The return type of the method must be error.
	if returnType := mtype.Out(0); returnType != typeOfError {
		log.Log("method", mname, "returns", returnType.String(), "not error")
		return nil
	}
	return &methodType{method: method, ArgType: argType, ReplyType: replyType, ContextType: contextType, stream: stream}
}

// [Min] 注册 Go 的 handler 的实例（也就是方法的 receiver）到 server.serviceMap
func (server *server) register(rcvr interface{}) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.serviceMap == nil {
		server.serviceMap = make(map[string]*service)
	}
	// [Min] 该 service 是对服务器而言的，可以理解为一个 handler，提供了该 handler
	// [Min] service 包含了该 handler
	s := new(service)
	s.typ = reflect.TypeOf(rcvr)                    // [Min] 实例反射类型
	s.rcvr = reflect.ValueOf(rcvr)                  // [Min] 实例反射值
	sname := reflect.Indirect(s.rcvr).Type().Name() // [Min] 实例类型的名称
	if sname == "" {
		log.Fatal("rpc: no service name for type", s.typ.String())
	}
	// [Min] 必须可导出
	if !isExported(sname) {
		s := "rpc Register: type " + sname + " is not exported"
		log.Log(s)
		return errors.New(s)
	}
	// [Min] 实例类型名称不能重复
	if _, present := server.serviceMap[sname]; present {
		return errors.New("rpc: service already defined: " + sname)
	}
	// [Min] 对内的 service 的名称就是实例类型的名称
	s.name = sname
	// [Min]
	s.method = make(map[string]*methodType)

	// Install the methods
	// [Min] 将方法映射到 methodType，再以方法名为 key 存入 service.method 这个 map 中
	for m := 0; m < s.typ.NumMethod(); m++ {
		method := s.typ.Method(m)
		if mt := prepareMethod(method); mt != nil {
			s.method[method.Name] = mt
		}
	}

	// [Min] 必须有有效的方法
	if len(s.method) == 0 {
		s := "rpc Register: type " + sname + " has no exported methods of suitable type"
		log.Log(s)
		return errors.New(s)
	}
	server.serviceMap[s.name] = s
	return nil
}

func (server *server) sendResponse(sending *sync.Mutex, req *request, reply interface{}, codec serverCodec, errmsg string, last bool) (err error) {
	resp := server.getResponse()
	// Encode the response header
	resp.ServiceMethod = req.ServiceMethod
	if errmsg != "" {
		resp.Error = errmsg
		reply = invalidRequest
	}
	resp.Seq = req.Seq
	sending.Lock()
	err = codec.WriteResponse(resp, reply, last)
	sending.Unlock()
	server.freeResponse(resp)
	return err
}

func (s *service) call(ctx context.Context, server *server, sending *sync.Mutex, mtype *methodType, req *request, argv, replyv reflect.Value, codec serverCodec, ct string) {
	function := mtype.method.Func
	var returnValues []reflect.Value

	r := &rpcRequest{
		service:     server.name,
		contentType: ct,
		method:      req.ServiceMethod,
	}

	if !mtype.stream {
		r.request = argv.Interface()

		fn := func(ctx context.Context, req Request, rsp interface{}) error {
			returnValues = function.Call([]reflect.Value{s.rcvr, mtype.prepareContext(ctx), reflect.ValueOf(req.Request()), reflect.ValueOf(rsp)})

			// The return value for the method is an error.
			if err := returnValues[0].Interface(); err != nil {
				return err.(error)
			}

			return nil
		}

		for i := len(server.hdlrWrappers); i > 0; i-- {
			fn = server.hdlrWrappers[i-1](fn)
		}

		errmsg := ""
		err := fn(ctx, r, replyv.Interface())
		if err != nil {
			errmsg = err.Error()
		}

		err = server.sendResponse(sending, req, replyv.Interface(), codec, errmsg, true)
		if err != nil {
			log.Log("rpc call: unable to send response: ", err)
		}
		server.freeRequest(req)
		return
	}

	// declare a local error to see if we errored out already
	// keep track of the type, to make sure we return
	// the same one consistently
	var lastError error

	stream := &rpcStream{
		context: ctx,
		codec:   codec,
		request: r,
		seq:     req.Seq,
	}

	// Invoke the method, providing a new value for the reply.
	fn := func(ctx context.Context, req Request, stream interface{}) error {
		returnValues = function.Call([]reflect.Value{s.rcvr, mtype.prepareContext(ctx), reflect.ValueOf(stream)})
		if err := returnValues[0].Interface(); err != nil {
			// the function returned an error, we use that
			return err.(error)
		} else if lastError != nil {
			// we had an error inside sendReply, we use that
			return lastError
		} else {
			// no error, we send the special EOS error
			return lastStreamResponseError
		}
	}

	for i := len(server.hdlrWrappers); i > 0; i-- {
		fn = server.hdlrWrappers[i-1](fn)
	}

	// client.Stream request
	r.stream = true

	errmsg := ""
	if err := fn(ctx, r, stream); err != nil {
		errmsg = err.Error()
	}

	// this is the last packet, we don't do anything with
	// the error here (well sendStreamResponse will log it
	// already)
	server.sendResponse(sending, req, nil, codec, errmsg, true)
	server.freeRequest(req)
}

func (m *methodType) prepareContext(ctx context.Context) reflect.Value {
	if contextv := reflect.ValueOf(ctx); contextv.IsValid() {
		return contextv
	}
	return reflect.Zero(m.ContextType)
}

// [Min] 内部 server 处理 request，request 的内容在 codec 中，ct 是 content-type
func (server *server) serveRequest(ctx context.Context, codec serverCodec, ct string) error {
	sending := new(sync.Mutex)
	// [Min] 从 codec 中读取 request
	service, mtype, req, argv, replyv, keepReading, err := server.readRequest(codec)
	if err != nil {
		if !keepReading {
			return err
		}
		// send a response if we actually managed to read a header.
		if req != nil {
			server.sendResponse(sending, req, invalidRequest, codec, err.Error(), true)
			server.freeRequest(req)
		}
		return err
	}
	service.call(ctx, server, sending, mtype, req, argv, replyv, codec, ct)
	return nil
}

// [Min] 更新 server 中的 freeReq，并返回空的 request
func (server *server) getRequest() *request {
	server.reqLock.Lock()
	req := server.freeReq
	if req == nil {
		req = new(request)
	} else {
		server.freeReq = req.next
		*req = request{}
	}
	server.reqLock.Unlock()
	return req
}

func (server *server) freeRequest(req *request) {
	server.reqLock.Lock()
	req.next = server.freeReq
	server.freeReq = req
	server.reqLock.Unlock()
}

func (server *server) getResponse() *response {
	server.respLock.Lock()
	resp := server.freeResp
	if resp == nil {
		resp = new(response)
	} else {
		server.freeResp = resp.next
		*resp = response{}
	}
	server.respLock.Unlock()
	return resp
}

func (server *server) freeResponse(resp *response) {
	server.respLock.Lock()
	resp.next = server.freeResp
	server.freeResp = resp
	server.respLock.Unlock()
}

func (server *server) readRequest(codec serverCodec) (service *service, mtype *methodType, req *request, argv, replyv reflect.Value, keepReading bool, err error) {
	service, mtype, req, keepReading, err = server.readRequestHeader(codec)
	if err != nil {
		if !keepReading {
			return
		}
		// discard body
		codec.ReadRequestBody(nil)
		return
	}
	// is it a streaming request? then we don't read the body
	if mtype.stream {
		codec.ReadRequestBody(nil)
		return
	}

	// Decode the argument value.
	argIsValue := false // if true, need to indirect before calling.
	if mtype.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(mtype.ArgType.Elem())
	} else {
		argv = reflect.New(mtype.ArgType)
		argIsValue = true
	}
	// argv guaranteed to be a pointer now.
	if err = codec.ReadRequestBody(argv.Interface()); err != nil {
		return
	}
	if argIsValue {
		argv = argv.Elem()
	}

	if !mtype.stream {
		replyv = reflect.New(mtype.ReplyType.Elem())
	}
	return
}

// [Min]
func (server *server) readRequestHeader(codec serverCodec) (service *service, mtype *methodType, req *request, keepReading bool, err error) {
	// Grab the request header.
	req = server.getRequest()
	err = codec.ReadRequestHeader(req, true)
	if err != nil {
		req = nil
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return
		}
		err = errors.New("rpc: server cannot decode request: " + err.Error())
		return
	}

	// We read the header successfully. If we see an error now,
	// we can still recover and move on to the next request.
	keepReading = true

	// [Min] 从 request 获得完整的 serviceMethod name，如 类型名.方法名
	serviceMethod := strings.Split(req.ServiceMethod, ".")
	if len(serviceMethod) != 2 {
		err = errors.New("rpc: service/method request ill-formed: " + req.ServiceMethod)
		return
	}
	// Look up the request.
	server.mu.Lock()
	// [Min] 根据类型名获取对应的内部 service
	service = server.serviceMap[serviceMethod[0]]
	server.mu.Unlock()
	if service == nil {
		err = errors.New("rpc: can't find service " + req.ServiceMethod)
		return
	}
	// [Min] 根据方法名获取该方法在内部 service 中对应的 methodType
	mtype = service.method[serviceMethod[1]]
	if mtype == nil {
		err = errors.New("rpc: can't find method " + req.ServiceMethod)
	}
	return
}

type serverCodec interface {
	ReadRequestHeader(*request, bool) error
	ReadRequestBody(interface{}) error
	WriteResponse(*response, interface{}, bool) error

	Close() error
}
