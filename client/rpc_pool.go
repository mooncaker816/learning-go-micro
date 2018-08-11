package client

import (
	"sync"
	"time"

	"github.com/micro/go-micro/transport"
)

// [Min] 连接缓存池
type pool struct {
	size int   // [Min] 缓存池大小
	ttl  int64 // [Min] ttl 时间（秒）

	sync.Mutex
	conns map[string][]*poolConn // [Min] 连接缓存
}

// [Min] 加了连接生成时间的 poolConn
type poolConn struct {
	transport.Client       // [Min] 作为客户端使用的，对底层连接封装后的一个类型实体
	created          int64 // [Min] 创建时间
}

// [Min] 新建一个缓存池，有大小和 ttl 设置
func newPool(size int, ttl time.Duration) *pool {
	return &pool{
		size:  size,
		ttl:   int64(ttl.Seconds()),
		conns: make(map[string][]*poolConn),
	}
}

// NoOp the Close since we manage it
func (p *poolConn) Close() error {
	return nil
}

// [Min] 获取一个有效连接，优先从 pool 中获取
func (p *pool) getConn(addr string, tr transport.Transport, opts ...transport.DialOption) (*poolConn, error) {
	p.Lock()
	// [Min] 获取该地址当前的所有连接
	conns := p.conns[addr]
	now := time.Now().Unix()

	// while we have conns check age and then return one
	// otherwise we'll create a new conn
	// [Min] 如果有连接，倒序取出一个连接，
	// [Min] 如果连接存在的时间超过了 ttl（默认为 1 min），关闭该连接
	// [Min] 如果 ok，那么就返回该连接
	for len(conns) > 0 {
		conn := conns[len(conns)-1]
		conns = conns[:len(conns)-1]
		p.conns[addr] = conns

		// if conn is old kill it and move on
		if d := now - conn.created; d > p.ttl {
			conn.Client.Close()
			continue
		}

		// we got a good conn, lets unlock and return it
		p.Unlock()

		return conn, nil
	}

	p.Unlock()

	// create new conn
	// [Min] 返回的是作为调用方使用的，对底层连接封装后的一个类型实体，如 httpTransportClinet
	c, err := tr.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	// [Min] dial 获的新的连接后包装为 poolConn，并返回
	return &poolConn{c, time.Now().Unix()}, nil
}

// [Min] 调用完成后，释放连接到 pool 中
func (p *pool) release(addr string, conn *poolConn, err error) {
	// don't store the conn if it has errored
	// [Min] 如果调用返回 error 不为 nil，直接关闭该连接，返回
	if err != nil {
		conn.Client.Close()
		return
	}

	// otherwise put it back for reuse
	// [Min] 将该连接重新存入缓存池中（队尾）
	p.Lock()
	conns := p.conns[addr]
	// [Min] 不能超过缓存池预设的大小
	if len(conns) >= p.size {
		p.Unlock()
		conn.Client.Close()
		return
	}
	p.conns[addr] = append(conns, conn)
	p.Unlock()
}
