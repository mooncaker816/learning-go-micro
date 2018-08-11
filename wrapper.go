package micro

import (
	"context"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/metadata"
)

// [Min] clinet 外面套了一个 metadata
type clientWrapper struct {
	client.Client
	headers metadata.Metadata
}

// [Min] 将
func (c *clientWrapper) setHeaders(ctx context.Context) context.Context {
	md := make(metadata.Metadata)

	// [Min] 从 context 中获取现有的 metadata
	if mda, ok := metadata.FromContext(ctx); ok {
		// make copy of metadata
		for k, v := range mda {
			md[k] = v
		}
	}

	// [Min] 将 clientWrapper 中的 metadata 与 context 中的合并（不覆盖）
	for k, v := range c.headers {
		if _, ok := md[k]; !ok {
			md[k] = v
		}
	}

	// [Min] 返回以原 context 为 parent 的新的 context，其中带有合并后的 metadata
	return metadata.NewContext(ctx, md)
}

// [Min] 合并完 metadata 后，以新的 context 调用 Client.Call
func (c *clientWrapper) Call(ctx context.Context, req client.Request, rsp interface{}, opts ...client.CallOption) error {
	ctx = c.setHeaders(ctx)
	return c.Client.Call(ctx, req, rsp, opts...)
}

// [Min] 合并完 metadata 后，以新的 context 调用 Client.Stream
func (c *clientWrapper) Stream(ctx context.Context, req client.Request, opts ...client.CallOption) (client.Stream, error) {
	ctx = c.setHeaders(ctx)
	return c.Client.Stream(ctx, req, opts...)
}

// [Min] 合并完 metadata 后，以新的 context 调用 Client.Publish
func (c *clientWrapper) Publish(ctx context.Context, p client.Message, opts ...client.PublishOption) error {
	ctx = c.setHeaders(ctx)
	return c.Client.Publish(ctx, p, opts...)
}
