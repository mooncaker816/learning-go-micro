package server

import (
	"reflect"

	"github.com/micro/go-micro/registry"
)

// [Min] rpcHandler，对应于 Go 的一个 handler
type rpcHandler struct {
	name      string
	handler   interface{}
	endpoints []*registry.Endpoint // [Min] handler 每一个可导出方法的映射
	opts      HandlerOptions       // [Min] HandlerOptions 主要是每一个方法对应的 metadata，key 为 A.Method 的形式
}

// [Min] 将 Go handler 转为统一的 rpcHandler，
// [Min] handler 的每一个可导出方法都映射到 rpcHandler 中的一个 endpoint，方便 rpc 以统一形式调用
func newRpcHandler(handler interface{}, opts ...HandlerOption) Handler {
	options := HandlerOptions{
		Metadata: make(map[string]map[string]string),
	}

	// [Min] 按需对 HandlerOptions 进行修改
	for _, o := range opts {
		o(&options)
	}

	// [Min] 获取 handler 的类型，值，名字
	typ := reflect.TypeOf(handler)
	hdlr := reflect.ValueOf(handler)
	name := reflect.Indirect(hdlr).Type().Name()

	var endpoints []*registry.Endpoint

	// [Min] 每一个方法对应一个 Endpoint
	for m := 0; m < typ.NumMethod(); m++ {
		if e := extractEndpoint(typ.Method(m)); e != nil {
			// [Min] 遵循 go 的方法的命名方式 receiver.Method
			e.Name = name + "." + e.Name

			// [Min] 将 HandlerOptions 中的属于该方法的 metadata 添加到 endpoint 的 metadata 中
			for k, v := range options.Metadata[e.Name] {
				e.Metadata[k] = v
			}

			endpoints = append(endpoints, e)
		}
	}

	return &rpcHandler{
		name:      name,      // [Min] handler 的名称
		handler:   handler,   // [Min] handler 本身，实质一般是一个结构体实例，实现了与 protobuf 中该 service 定义一致的方法
		endpoints: endpoints, // [Min] 由 handler 的可导出方法映射出的 endpoints
		opts:      options,   // [Min] handlerOptions，由外界的 Option 函数修改得来，主要是各个方法对应的 metadata，已经更新到 endpoints 中
	}
}

// [Min] rpcHandler 的名称，其实就是 Go handler 实例类型的名称
func (r *rpcHandler) Name() string {
	return r.name
}

// [Min] rpcHandler 对应的 Go handler
func (r *rpcHandler) Handler() interface{} {
	return r.handler
}

// [Min] rpcHandler 的 endpoints
func (r *rpcHandler) Endpoints() []*registry.Endpoint {
	return r.endpoints
}

// [Min] rpcHandler 的 HandlerOptions
func (r *rpcHandler) Options() HandlerOptions {
	return r.opts
}
