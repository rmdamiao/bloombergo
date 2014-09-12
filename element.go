package bloombergo

/*
#include <blpapi_event.h>
#include <blpapi_name.h>
#include <blpapi_datetime.h>
#include <stdlib.h>
#cgo CFLAGS: -I../dll
#cgo LDFLAGS: -L. -lblpapi3_64
*/
import "C"
import "unsafe"
import "time"

type Element struct {
	element *C.blpapi_Element_t
}

func (e Element) DataType() int {
	if (e == Element{}) {
		return 0
	} else {
		return int(C.blpapi_Element_datatype(e.element))
	}
}

func (e Element) IsArray() bool {
	if (e == Element{}) {
		return false
	} else {
		return C.blpapi_Element_isArray(e.element) != 0
	}
}

func (e Element) NumValues() int {
	if (e == Element{}) {
		return 0
	} else {
		return int(C.blpapi_Element_numValues(e.element))
	}
}

func (e Element) NumElements() int {
	if (e == Element{}) {
		return 0
	} else {
		return int(C.blpapi_Element_numElements(e.element))
	}
}

func (e Element) IsNull() bool {
	if (e == Element{}) {
		return true
	} else {
		return C.blpapi_Element_isNull(e.element) != 0
	}
}

func (e Element) GetElementAt(position int) Element {
	var response *C.blpapi_Element_t
	if (e != Element{}) && C.blpapi_Element_getElementAt(e.element, &response, C.size_t(position)) == 0 {
		return Element{response}
	} else {
		return Element{}
	}
}

func (e Element) GetElement(value string) Element {
	var nullName *C.blpapi_Name_t
	var response *C.blpapi_Element_t
	cstring := C.CString(value)
	defer C.free(unsafe.Pointer(cstring))
	if (e != Element{}) && C.blpapi_Element_getElement(e.element, &response, cstring, nullName) == 0 {
		return Element{response}
	} else {
		return Element{}
	}
}

func (e Element) HasElement(value string) bool {
	var nullName *C.blpapi_Name_t
	cstring := C.CString(value)
	defer C.free(unsafe.Pointer(cstring))
	if (e != Element{}) && C.blpapi_Element_hasElement(e.element, cstring, nullName) != 0 {
		return true
	} else {
		return false
	}
}

func (e Element) GetValueAsString() string {
	resp := C.CString("")
	defer C.free(unsafe.Pointer(resp))
	if C.blpapi_Element_getValueAsString(e.element, &resp, 0) == 0 {
		return C.GoString(resp)
	} else {
		return ""
	}
}

func (e Element) GetValueAsBool() bool {
	var resp C.blpapi_Bool_t
	if (e != Element{}) && C.blpapi_Element_getValueAsBool(e.element, &resp, C.size_t(0)) == 0 {
		return resp != 0
	} else {
		return false
	}
}

func (e Element) GetValueAsInt32() int32 {
	var resp C.blpapi_Int32_t
	if (e != Element{}) && C.blpapi_Element_getValueAsInt32(e.element, &resp, C.size_t(0)) == 0 {
		return int32(resp)
	} else {
		return int32(0)
	}
}

func (e Element) GetValueAsInt64() int64 {
	var resp C.blpapi_Int64_t
	if (e != Element{}) && C.blpapi_Element_getValueAsInt64(e.element, &resp, C.size_t(0)) == 0 {
		return int64(resp)
	} else {
		return int64(0)
	}
}

func (e Element) GetValueAsFloat32() float32 {
	var resp C.blpapi_Float32_t
	if (e != Element{}) && C.blpapi_Element_getValueAsFloat32(e.element, &resp, C.size_t(0)) == 0 {
		return float32(resp)
	} else {
		return float32(0.0)
	}
}

func (e Element) GetValueAsFloat64() float64 {
	var resp C.blpapi_Float64_t
	if (e != Element{}) && C.blpapi_Element_getValueAsFloat64(e.element, &resp, C.size_t(0)) == 0 {
		return float64(resp)
	} else {
		return float64(0.0)
	}
}

func (e Element) GetValueAsDateTime() time.Time {
	var Datetime C.blpapi_Datetime_t
	var goDatetime time.Time
	if (e != Element{}) && C.blpapi_Element_getValueAsDatetime(e.element, &Datetime, C.size_t(0)) == 0 {
		goDatetime = time.Date(int(Datetime.year), time.Month(Datetime.month), int(Datetime.day), 0, 0, 0, 0, time.UTC)
		return goDatetime
	} else {
		return goDatetime
	}
}

func (e Element) GetValueAsElement(position int) Element {
	var response *C.blpapi_Element_t
	if (e != Element{}) && C.blpapi_Element_getValueAsElement(e.element, &response, C.size_t(position)) == 0 {
		return Element{response}
	} else {
		return Element{}
	}
}

func (e Element) SetValueString(value string) {
	cstring := C.CString(value)
	defer C.free(unsafe.Pointer(cstring))
	C.blpapi_Element_setValueString(e.element, cstring, C.BLPAPI_ELEMENT_INDEX_END)
}

func (e Element) Name() string {
	return C.GoString(C.blpapi_Element_nameString(e.element))
}
