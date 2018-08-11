package server

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/micro/go-micro/registry"
)

// [Min] 将 Go 的类型映射为 *registry.Value 的形式
func extractValue(v reflect.Type, d int) *registry.Value {
	// [Min] d 表示结构体中字段的层级深度
	if d == 3 {
		return nil
	}
	if v == nil {
		return nil
	}

	// [Min] 指针解引用
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// [Min] 根据类型构造 registry.Value
	arg := &registry.Value{
		Name: v.Name(), // [Min] 该类型的名称，字符串形式
		Type: v.Name(), // [Min] 该类型的名称，字符串形式
	}

	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			val := extractValue(f.Type, d+1)
			if val == nil {
				continue
			}

			// [Min] 更新名称
			// if we can find a json tag use it
			// [Min] 如果字段由 json tag，就用 tag 中的名称作为 Value.Name
			if tags := f.Tag.Get("json"); len(tags) > 0 {
				parts := strings.Split(tags, ",")
				val.Name = parts[0]
			}

			// if there's no name default it
			if len(val.Name) == 0 {
				val.Name = v.Field(i).Name // [Min] 默认名称，字段名
			}

			// [Min] 添加到该结构体的 Values 中
			arg.Values = append(arg.Values, val)
		}
	case reflect.Slice:
		p := v.Elem() // [Min] 切片元素的类型
		if p.Kind() == reflect.Ptr {
			p = p.Elem()
		}
		arg.Type = "[]" + p.Name() // [Min] 类型的字符串形式
		// [Min] 这里把切片也当成是类似结构体的类型处理，第一层为切片的信息，第二层为切片元素的信息
		val := extractValue(v.Elem(), d+1)
		if val != nil {
			arg.Values = append(arg.Values, val)
		}
	}

	return arg
}

// [Min] 将 handler 的方法映射为 Endpoint
func extractEndpoint(method reflect.Method) *registry.Endpoint {
	// [Min] method.PkgPath != "" 表示该方法不可导出，直接返回 nil
	if method.PkgPath != "" {
		return nil
	}

	var rspType, reqType reflect.Type
	var stream bool
	mt := method.Type // [Min] mt 是将 receiver 作为第一个输入参数的函数类型

	// [Min] receiver，context，req，rsp
	// [Min] 该方法的输入参数的个数，支持3个和4个，末尾两个为 req 和 rsp
	// [Min] 获取 reqType 和 rspType
	switch mt.NumIn() {
	case 3:
		reqType = mt.In(1)
		rspType = mt.In(2)
	case 4:
		reqType = mt.In(2)
		rspType = mt.In(3)
	default:
		return nil
	}

	// are we dealing with a stream?
	// [Min] 如果 rspType 不是一个传统意义上的值类型，那么就认为是 stream
	switch rspType.Kind() {
	case reflect.Func, reflect.Interface:
		stream = true
	}

	// [Min] 由 Go 的类型结构转为 registry.Value 结构（其实也是描述结构类型）
	// [Min] 知道了结构就字段的结构类型，就可以解析值
	request := extractValue(reqType, 0)
	response := extractValue(rspType, 0)

	// [Min] 构造该方法对应的 Endpoint
	return &registry.Endpoint{
		Name:     method.Name,
		Request:  request,
		Response: response,
		Metadata: map[string]string{
			"stream": fmt.Sprintf("%v", stream),
		},
	}
}

// [Min] 输入的是一个函数或方法的类型反射，映射出 registry.Value
// [Min] 该函数或方法表示了对订阅消息的一种处理手段
func extractSubValue(typ reflect.Type) *registry.Value {
	var reqType reflect.Type
	// [Min] reqType 为实现了 publication 的实例对象，也就是订阅消息的具体类型
	switch typ.NumIn() {
	case 1:
		reqType = typ.In(0)
	case 2:
		reqType = typ.In(1)
	case 3:
		reqType = typ.In(2)
	default:
		return nil
	}
	// [Min] 根据订阅消息的具体类型，得出 registry.Value
	return extractValue(reqType, 0)
}
