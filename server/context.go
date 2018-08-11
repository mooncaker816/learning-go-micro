package server

import (
	"context"
)

type serverKey struct{}

// [Min] 从 context 中获取 wait 的 flag
func wait(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	wait, ok := ctx.Value("wait").(bool)
	if !ok {
		return false
	}
	return wait
}

// [Min] 从 context 中获取 server
func FromContext(ctx context.Context) (Server, bool) {
	c, ok := ctx.Value(serverKey{}).(Server)
	return c, ok
}

// [Min] 将 server 加入到 context 中
func NewContext(ctx context.Context, s Server) context.Context {
	return context.WithValue(ctx, serverKey{}, s)
}
