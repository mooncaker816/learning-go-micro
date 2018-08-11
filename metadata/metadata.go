// Package metadata is a way of defining message headers
package metadata

import (
	"context"
)

type metaKey struct{}

// Metadata is our way of representing request headers internally.
// They're used at the RPC level and translate back and forth
// from Transport headers.
// [Min] metadata， key-value 参数，用于统一内部使用的request header
type Metadata map[string]string

// [Min] 从 context 中获取 metadata
func FromContext(ctx context.Context) (Metadata, bool) {
	md, ok := ctx.Value(metaKey{}).(Metadata)
	return md, ok
}

// [Min] 以原 context 为 parent，加入 metadata 返回一个新的 context
func NewContext(ctx context.Context, md Metadata) context.Context {
	return context.WithValue(ctx, metaKey{}, md)
}
