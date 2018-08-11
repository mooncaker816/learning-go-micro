package protorpc

import (
	"encoding/binary"
	"io"
)

// WriteNetString writes data to a big-endian netstring on a Writer.
// Size is always a 32-bit unsigned int.
// [Min] 将数据 data 按照 bigEndian 写入 w 中，先写入4个字节大小的数据长度，再写入内容
func WriteNetString(w io.Writer, data []byte) (written int, err error) {
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(data)))
	if written, err = w.Write(size); err != nil {
		return
	}
	return w.Write(data)
}

// ReadNetString reads data from a big-endian netstring.
// [Min] 读取数据 r 中开头第一个字段的二进制数据
func ReadNetString(r io.Reader) (data []byte, err error) {
	// [Min] 先读取4个字节的长度信息
	sizeBuf := make([]byte, 4)
	_, err = r.Read(sizeBuf)
	if err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(sizeBuf)
	if size == 0 {
		return nil, nil
	}
	data = make([]byte, size)
	_, err = r.Read(data)
	if err != nil {
		return nil, err
	}
	return
}
