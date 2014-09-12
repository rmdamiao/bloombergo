package bloombergo

/*
#include <blpapi_correlationid.h>
#include <blpapi_element.h>
#include <blpapi_event.h>
#include <blpapi_message.h>
#include <blpapi_request.h>
#include <blpapi_session.h>
#include <blpapi_name.h>
#include <stdlib.h>
#include <string.h>
blpapi_CorrelationId_t* SetCorrelationId(blpapi_UInt64_t v) {
	blpapi_CorrelationId_t* corrId = malloc(sizeof(blpapi_CorrelationId_t));
 	corrId->size = sizeof(blpapi_CorrelationId_t);
 	corrId->valueType = BLPAPI_CORRELATION_TYPE_INT;
 	corrId->value.intValue = v;
 	return corrId;
}
blpapi_UInt64_t GetCorrelationId(blpapi_CorrelationId_t* corrId) {
	blpapi_UInt64_t value;
	value = corrId->value.intValue;
	return value;
}
#cgo CFLAGS: -I./include
#cgo LDFLAGS: -L./lib/
*/
import "C"
import "unsafe"
import "reflect"

type ReferenceDataRequest struct {
	securities []string
	response   interface{}
}

type Session struct {
	RefDataReq chan ReferenceDataRequest
	Destroy    chan bool
}

func CreateSession(host string, port uint16) (Session, bool) {

	// Create abstract session by literal function
	session := func() *C.blpapi_Session_t {
		var nullEventHandler C.blpapi_EventHandler_t
		var nullEventDispatcher *C.blpapi_EventDispatcher_t
		var nullUserdate unsafe.Pointer
		cstring := C.CString(host)
		defer C.free(unsafe.Pointer(cstring))

		sessionOptions := C.blpapi_SessionOptions_create()

		C.blpapi_SessionOptions_setServerHost(sessionOptions, cstring)
		C.blpapi_SessionOptions_setServerPort(sessionOptions, C.ushort(port))
		s := C.blpapi_Session_create(sessionOptions, nullEventHandler, nullEventDispatcher, nullUserdate)
		C.blpapi_SessionOptions_destroy(sessionOptions)
		return s
	}()

	// Start session and open reference data service
	if ok := func() bool {
		cRefData := C.CString("//blp/refdata")
		defer C.free(unsafe.Pointer(cRefData))
		if C.blpapi_Session_start(session) != 0 {
			return false
		}
		if C.blpapi_Session_openService(session, cRefData) != 0 {
			C.blpapi_Session_destroy(session)
			return false
		}
		return true
	}(); !ok {
		return Session{}, false
	}

	// Get reference Data Service handle
	refDataSvc := func() *C.blpapi_Service_t {
		cRefData := C.CString("//blp/refdata")
		defer C.free(unsafe.Pointer(cRefData))
		var r *C.blpapi_Service_t
		C.blpapi_Session_getService(session, &r, cRefData)
		return r
	}()

	// Create state variables
	refDataReqQueue := make(map[uint64]ReferenceDataRequest)
	refDataRespQueue := make(map[uint64]([]Element))

	// Create communication channels
	refDataChan := make(chan ReferenceDataRequest)
	destroyChan := make(chan bool)

	// Set correlation id counter
	corrId := uint64(0)

	// Function to process Reference Data Request
	var Request = func(securities []string, fields []string, id uint64) {
		crefDataRequest := C.CString("ReferenceDataRequest")
		defer C.free(unsafe.Pointer(crefDataRequest))

		var request *C.blpapi_Request_t
		C.blpapi_Service_createRequest(refDataSvc, &request, crefDataRequest)

		elements := Element{C.blpapi_Request_elements(request)}
		securitiesElements := elements.GetElement("securities")
		for _, sec := range securities {
			securitiesElements.SetValueString(sec)
		}

		fieldsElements := elements.GetElement("fields")
		for _, fld := range fields {
			fieldsElements.SetValueString(fld)
		}

		var nullIdentity *C.blpapi_Identity_t
		var nullEventQueue *C.blpapi_EventQueue_t
		var nullChar *C.char

		C.blpapi_Session_sendRequest(session, request, C.SetCorrelationId(C.blpapi_UInt64_t(id)), nullIdentity, nullEventQueue, nullChar, 0)
	}

	// listen to channels
	go func() {
		for loop := true; loop; {
			select {
			case r := <-refDataChan:
				if ChanType := reflect.TypeOf(r.response); ChanType.Kind() == reflect.Chan {
					if MapType := ChanType.Elem(); MapType.Kind() == reflect.Map {
						if MapType.Key().Kind() == reflect.String {
							if StructType := MapType.Elem(); StructType.Kind() == reflect.Struct {
								corrId += 1
								refDataReqQueue[corrId] = r
								fieldArray := make([]string, 0, StructType.NumField())
								for i := 0; i < StructType.NumField(); i += 1 {
									fieldArray = append(fieldArray, StructType.Field(i).Name)
								}
								Request(r.securities, fieldArray, corrId)
							}
						}
					}
				}

			case <-destroyChan:
				loop = false
			}
		}
	}()

	var ParseValue = func(v reflect.Value, e Element, typeName string) {
		if typeName == "Time" {
			v.Set(reflect.ValueOf(e.GetValueAsDateTime()))
		} else {
			switch v.Kind() {
			case reflect.String:
				v.SetString(e.GetValueAsString())
			case reflect.Int32:
				v.SetInt(int64(e.GetValueAsInt32()))
			case reflect.Int64:
				v.SetInt(e.GetValueAsInt64())
			case reflect.Float32:
				v.SetFloat(float64(e.GetValueAsFloat32()))
			case reflect.Float64:
				v.SetFloat(e.GetValueAsFloat64())
			case reflect.Bool:
				v.SetBool(e.GetValueAsBool())
			}
		}
	}

	var handleResponse = func(id uint64) {

		// Get structure of map[security]structure
		StructField := reflect.TypeOf(refDataReqQueue[id].response).Elem().Elem()
		// Get structure field names
		FieldNames := make(map[string]int)
		for i := 0; i < StructField.NumField(); i += 1 {
			FieldNames[StructField.Field(i).Name] = i
		}

		s := reflect.MakeMap(reflect.TypeOf(refDataReqQueue[id].response).Elem())

		for _, referenceDataResponse := range refDataRespQueue[id] {
			securityDataArray := referenceDataResponse.GetElement("securityData")
			numItems := securityDataArray.NumValues()
			for i := 0; i < numItems; i += 1 {
				securityData := securityDataArray.GetValueAsElement(i)
				security := securityData.GetElement("security")
				securityStr := security.GetValueAsString()
				StructNew := reflect.Indirect(reflect.New(StructField))
				fieldDataArray := securityData.GetElement("fieldData")
				numFlds := fieldDataArray.NumElements()
				for j := 0; j < numFlds; j += 1 {
					fieldData := fieldDataArray.GetElementAt(j)
					if index, ok := FieldNames[fieldData.Name()]; ok {
						if fieldData.IsArray() {
							if f, ok := StructField.FieldByName(fieldData.Name()); ok && f.Type.Kind() == reflect.Slice {
								numSubFlds := fieldData.NumValues()
								sf := reflect.MakeSlice(f.Type, 0, numSubFlds)
								for l := 0; l < fieldData.NumValues(); l += 1 {
									subStruct := reflect.Indirect(reflect.New(f.Type.Elem()))
									fieldDataValue := fieldData.GetValueAsElement(l)
									for k := 0; k < subStruct.NumField(); k += 1 {
										ParseValue(subStruct.Field(k), fieldDataValue.GetElementAt(k), f.Type.Elem().Field(k).Type.Name())
									}
									sf = reflect.Append(sf, subStruct)
								}
								StructNew.Field(index).Set(sf)
							}
						} else {
							ParseValue(StructNew.Field(index), fieldData, StructField.Field(index).Type.Name())
						}
					}
				}
				s.SetMapIndex(reflect.ValueOf(securityStr), StructNew)
			}
		}
		reflect.ValueOf(refDataReqQueue[id].response).Send(s)
	}

	var queueRefResponse = func(event *C.blpapi_Event_t, final bool) {
		// mesage iterator (suposedly a single message of name ReferenceDataResponse)
		iter := C.blpapi_MessageIterator_create(event)
		var id uint64 // suosedly there is only one message in the message iterator
		var message *C.blpapi_Message_t
		for C.blpapi_MessageIterator_next(iter, &message) == 0 {
			referenceDataResponse := Element{C.blpapi_Message_elements(message)}
			var corrId C.blpapi_CorrelationId_t
			corrId = C.blpapi_Message_correlationId(message, 0)
			id = uint64(C.GetCorrelationId(&corrId))
			if referenceDataResponse.Name() == "ReferenceDataResponse" {
				if !referenceDataResponse.HasElement("responseError") {
					if _, ok := refDataRespQueue[id]; !ok {
						refDataRespQueue[id] = make([]Element, 0, 0)
					}
					refDataRespQueue[id] = append(refDataRespQueue[id], referenceDataResponse)
				}
			}
		}
		if final {
			handleResponse(id)
		}
	}

	// session message event loop goroutine
	go func() {
		for loop := true; loop; {

			var event *C.blpapi_Event_t
			C.blpapi_Session_nextEvent(session, &event, 0)

			eventType := C.blpapi_Event_eventType(event)
			switch eventType {

			case C.BLPAPI_EVENTTYPE_RESPONSE:
				queueRefResponse(event, true)
			case C.BLPAPI_EVENTTYPE_PARTIAL_RESPONSE:
				queueRefResponse(event, false)
			}
			C.blpapi_Event_release(event)
		}
	}()

	return Session{refDataChan, destroyChan}, true
}
