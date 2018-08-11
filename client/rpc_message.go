package client

// [Min] 替代了老的 publication

type message struct {
	topic       string
	contentType string
	payload     interface{}
}

// [Min] 新建 message，contentType 以 MessageOption 修改的为准，如果没有修改，则以输入的 contentType 为准
func newMessage(topic string, payload interface{}, contentType string, opts ...MessageOption) Message {
	var options MessageOptions
	for _, o := range opts {
		o(&options)
	}

	if len(options.ContentType) > 0 {
		contentType = options.ContentType
	}

	return &message{
		payload:     payload,
		topic:       topic,
		contentType: contentType,
	}
}

func (m *message) ContentType() string {
	return m.contentType
}

func (m *message) Topic() string {
	return m.topic
}

func (m *message) Payload() interface{} {
	return m.payload
}
