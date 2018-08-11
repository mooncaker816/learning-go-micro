package protorpc

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/micro/go-micro/codec"
)

type flusher interface {
	Flush() error
}

type protoCodec struct {
	sync.Mutex
	rwc io.ReadWriteCloser // [Min] 由两个分别用于读和写的基本类型的缓存构成的对象，如 bytes.Buffer
	mt  codec.MessageType  // [Min] message 类型：Request Response Publication
	buf *bytes.Buffer      // [Min] publication 缓存
}

func (c *protoCodec) Close() error {
	c.buf.Reset()
	return c.rwc.Close()
}

func (c *protoCodec) String() string {
	return "proto-rpc"
}

// [Min] 按 protobuf 序列化 Message 和 数据 b ，存入 c.rwc
func (c *protoCodec) Write(m *codec.Message, b interface{}) error {
	switch m.Type {
	case codec.Request:
		// [Min] 将 Message 的信息映射到 protobuf 的 Request 中，序列化后写入 c.rwc
		// [Min] 将 b 序列化后写入 c.rwc
		// [Min] Message 用来描述该 Request 的具体执行方法和序号
		// [Min] b 是执行该方法需要用到的输入数据
		c.Lock()
		defer c.Unlock()
		// This is protobuf, of course we copy it.
		// [Min] protobuf 的 request
		pbr := &Request{ServiceMethod: &m.Method, Seq: &m.Id}
		data, err := proto.Marshal(pbr)
		if err != nil {
			return err
		}
		_, err = WriteNetString(c.rwc, data)
		if err != nil {
			return err
		}
		// Of course this is a protobuf! Trust me or detonate the program.
		// [Min] 原始输入是按照 protoc 生成的 Go 的 struct 准备的，
		// [Min] 也就是说肯定实现了 proto.Message 接口，直接序列化即可
		data, err = proto.Marshal(b.(proto.Message))
		if err != nil {
			return err
		}
		_, err = WriteNetString(c.rwc, data)
		if err != nil {
			return err
		}
		if flusher, ok := c.rwc.(flusher); ok {
			err = flusher.Flush()
		}
	case codec.Response:
		// [Min] 将 Message 的信息映射到 protobuf 的 Response 中，序列化后写入 c.rwc
		// [Min] 将 b 序列化后写入 c.rwc
		// [Min] Message 用来描述该 Response 的序号，返回结果，和对应的执行方法
		// [Min] b 是执行该方法的返回数据（micro 应该用不到，需要的返回都是以指针的形式内置在参数中）
		c.Lock()
		defer c.Unlock()
		rtmp := &Response{ServiceMethod: &m.Method, Seq: &m.Id, Error: &m.Error}
		data, err := proto.Marshal(rtmp)
		if err != nil {
			return err
		}
		_, err = WriteNetString(c.rwc, data)
		if err != nil {
			return err
		}
		if pb, ok := b.(proto.Message); ok {
			data, err = proto.Marshal(pb)
			if err != nil {
				return err
			}
		} else {
			data = nil
		}
		_, err = WriteNetString(c.rwc, data)
		if err != nil {
			return err
		}
		if flusher, ok := c.rwc.(flusher); ok {
			err = flusher.Flush()
		}
	case codec.Publication:
		// [Min] Publication 直接将 b 序列化后写入 c.rwc
		data, err := proto.Marshal(b.(proto.Message))
		if err != nil {
			return err
		}
		c.rwc.Write(data)
	default:
		return fmt.Errorf("Unrecognised message type: %v", m.Type)
	}
	return nil
}

// [Min] 从 c.rwc 中读取 Header 反序列化后得到 Message
func (c *protoCodec) ReadHeader(m *codec.Message, mt codec.MessageType) error {
	c.buf.Reset()
	c.mt = mt

	switch mt {
	case codec.Request:
		// [Min] 读取 Request 头
		data, err := ReadNetString(c.rwc)
		if err != nil {
			return err
		}
		rtmp := new(Request)
		// [Min] 反序列化到 rtmp，再映射到 message
		err = proto.Unmarshal(data, rtmp)
		if err != nil {
			return err
		}
		m.Method = rtmp.GetServiceMethod()
		m.Id = rtmp.GetSeq()
	case codec.Response:
		// [Min] 读取 Response 头
		data, err := ReadNetString(c.rwc)
		if err != nil {
			return err
		}
		rtmp := new(Response)
		// [Min] 反序列化到 rtmp，再映射到 message
		err = proto.Unmarshal(data, rtmp)
		if err != nil {
			return err
		}
		m.Method = rtmp.GetServiceMethod()
		m.Id = rtmp.GetSeq()
		m.Error = rtmp.GetError()
	case codec.Publication:
		// [Min] publication 没有头，先临时存入 c.buf，由 ReadBody 从 c.buf 中直接获取
		io.Copy(c.buf, c.rwc)
	default:
		return fmt.Errorf("Unrecognised message type: %v", mt)
	}
	return nil
}

// [Min] 从 c.rwc 中读取 header 后面的实际数据（publication 从 c.buf 读取），
// [Min] 再反序列化后存入 b
func (c *protoCodec) ReadBody(b interface{}) error {
	var data []byte
	switch c.mt {
	case codec.Request, codec.Response:
		var err error
		data, err = ReadNetString(c.rwc)
		if err != nil {
			return err
		}
	case codec.Publication:
		data = c.buf.Bytes()
	default:
		return fmt.Errorf("Unrecognised message type: %v", c.mt)
	}
	if b != nil {
		return proto.Unmarshal(data, b.(proto.Message))
	}
	return nil
}

// [Min] 生成一个 protobuf 的编码解码器，rwc 是一个读写分别存储的实体
func NewCodec(rwc io.ReadWriteCloser) codec.Codec {
	return &protoCodec{
		buf: bytes.NewBuffer(nil), // [Min] buf 用于 publication 的读取
		rwc: rwc,
	}
}
