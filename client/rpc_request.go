package client

type rpcRequest struct {
	service     string         // [Min] rpc service name 一般就是类型名称
	method      string         // [Min] 该 request 具体调用的方法名
	contentType string         // [Min] 输入数据的 contentType
	request     interface{}    // [Min] 输入数据
	opts        RequestOptions // [Min] RequestOptions
}

// [Min] 新建 rpcRequest，没什么特殊的就是赋值，contentType 优先以 RequestOptions.ContentType 为准
func newRequest(service, method string, request interface{}, contentType string, reqOpts ...RequestOption) Request {
	var opts RequestOptions

	// [Min] 修改 RequestOptions
	for _, o := range reqOpts {
		o(&opts)
	}

	// set the content-type specified
	// [Min] 如果 RequestOptions 中的 ContentType 不为空，那么就以它为准
	if len(opts.ContentType) > 0 {
		contentType = opts.ContentType
	}

	return &rpcRequest{
		service:     service,
		method:      method,
		request:     request,
		contentType: contentType,
		opts:        opts,
	}
}

func (r *rpcRequest) ContentType() string {
	return r.contentType
}

func (r *rpcRequest) Service() string {
	return r.service
}

func (r *rpcRequest) Method() string {
	return r.method
}

// [Min] 返回原始的输入 request
func (r *rpcRequest) Request() interface{} {
	return r.request
}

func (r *rpcRequest) Stream() bool {
	return r.opts.Stream
}
