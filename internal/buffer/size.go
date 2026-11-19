package buffer

import (
	"unicode/utf16"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// maxDepth is how deep a message is walked; deeper messages count as one.
const maxDepth = 8

// ApproxSize is the size of a message in the worker's memory, worked out from
// its fields the way the Node SDK works it out: two bytes per UTF-16 unit of
// each text, its type name included, 32 bytes and the length of each byte
// array, eight bytes for each other value and each message not set, and 32
// bytes for the message, each message in it and each list.
func ApproxSize(m proto.Message) int {
	if m == nil {
		return 8
	}
	return messageSize(m.ProtoReflect(), 0)
}

func textSize(s string) int { return 2 * len(utf16.Encode([]rune(s))) }

func messageSize(m protoreflect.Message, depth int) int {
	if !m.IsValid() {
		return 8
	}
	if depth >= maxDepth {
		return 32
	}
	total := 32 + textSize(string(m.Descriptor().FullName()))
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		v := m.Get(f)
		switch {
		case f.IsList():
			total += 32
			list := v.List()
			for j := 0; j < list.Len(); j++ {
				total += valueSize(f, list.Get(j), depth+1)
			}
		case f.IsMap():
			total += 32
			v.Map().Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
				total += valueSize(f.MapKey(), k.Value(), depth+1) + valueSize(f.MapValue(), mv, depth+1)
				return true
			})
		default:
			if f.Message() != nil && !m.Has(f) {
				total += 8
				continue
			}
			total += valueSize(f, v, depth+1)
		}
	}
	return total
}

func valueSize(f protoreflect.FieldDescriptor, v protoreflect.Value, depth int) int {
	switch f.Kind() {
	case protoreflect.StringKind:
		return textSize(v.String())
	case protoreflect.BytesKind:
		return 32 + len(v.Bytes())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return messageSize(v.Message(), depth)
	default:
		return 8
	}
}
