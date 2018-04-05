// File generated by specialize. Do not edit.

// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stats

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func init() {
	beam.RegisterFunction(maxIntFn)
	beam.RegisterFunction(maxInt8Fn)
	beam.RegisterFunction(maxInt16Fn)
	beam.RegisterFunction(maxInt32Fn)
	beam.RegisterFunction(maxInt64Fn)
	beam.RegisterFunction(maxUintFn)
	beam.RegisterFunction(maxUint8Fn)
	beam.RegisterFunction(maxUint16Fn)
	beam.RegisterFunction(maxUint32Fn)
	beam.RegisterFunction(maxUint64Fn)
	beam.RegisterFunction(maxFloat32Fn)
	beam.RegisterFunction(maxFloat64Fn)
}

func findMaxFn(t reflect.Type) interface{} {
	switch t.String() {
	case "int":
		return maxIntFn
	case "int8":
		return maxInt8Fn
	case "int16":
		return maxInt16Fn
	case "int32":
		return maxInt32Fn
	case "int64":
		return maxInt64Fn
	case "uint":
		return maxUintFn
	case "uint8":
		return maxUint8Fn
	case "uint16":
		return maxUint16Fn
	case "uint32":
		return maxUint32Fn
	case "uint64":
		return maxUint64Fn
	case "float32":
		return maxFloat32Fn
	case "float64":
		return maxFloat64Fn
	default:
		panic(fmt.Sprintf("Unexpected number type: %v", t))
	}
}

func maxIntFn(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func maxInt8Fn(x, y int8) int8 {
	if x > y {
		return x
	}
	return y
}

func maxInt16Fn(x, y int16) int16 {
	if x > y {
		return x
	}
	return y
}

func maxInt32Fn(x, y int32) int32 {
	if x > y {
		return x
	}
	return y
}

func maxInt64Fn(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func maxUintFn(x, y uint) uint {
	if x > y {
		return x
	}
	return y
}

func maxUint8Fn(x, y uint8) uint8 {
	if x > y {
		return x
	}
	return y
}

func maxUint16Fn(x, y uint16) uint16 {
	if x > y {
		return x
	}
	return y
}

func maxUint32Fn(x, y uint32) uint32 {
	if x > y {
		return x
	}
	return y
}

func maxUint64Fn(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}

func maxFloat32Fn(x, y float32) float32 {
	if x > y {
		return x
	}
	return y
}

func maxFloat64Fn(x, y float64) float64 {
	if x > y {
		return x
	}
	return y
}
