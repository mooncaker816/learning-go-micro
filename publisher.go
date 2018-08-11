package micro

import (
	"context"

	"github.com/micro/go-micro/client"
)

// [Min] client 外包了一个 topic
type publisher struct {
	c     client.Client
	topic string // [Min] 用于生成 publication
}

// [Min] 通过 p.c.NewMessage(p.topic, msg) 来生成一个 publication，然后调用 clinet 的 Publish 方法
func (p *publisher) Publish(ctx context.Context, msg interface{}, opts ...client.PublishOption) error {
	return p.c.Publish(ctx, p.c.NewMessage(p.topic, msg))
}
