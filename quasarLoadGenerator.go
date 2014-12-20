package main

import (
	"fmt"
	"math"
	"net"
	"time"
	capnp "github.com/glycerine/go-capnproto"
)

func main() {
	var segment *capnp.Segment = capnp.NewBuffer(nil)
	var request Request = NewRootRequest(segment)

	var insert CmdInsertValues = NewCmdInsertValues(segment)
	insert.SetUuid([]byte("8981b164-87f3-11e4-93fc-00266c632560"))
	insert.SetSync(false)

	// Create a record list containing two data points, and add it
	var time1 int64 = time.Now().UnixNano()
	var value1 float64 = math.Sqrt(float64(time1))
	var record1 Record = NewRecord(segment)
	record1.SetTime(time1)
	record1.SetValue(value1)
	var recordList Record_List = NewRecordList(segment, 2)
	var time2 int64 = time.Now().UnixNano()
	var value2 float64 = math.Sqrt(float64(time2))
	var record2 Record = NewRecord(segment)
	record2.SetTime(time2)
	record2.SetValue(value2)
	var pointerList capnp.PointerList = capnp.PointerList(recordList)
	pointerList.Set(0, capnp.Object(record1))
	pointerList.Set(1, capnp.Object(record2))
	insert.SetValues(recordList)

	request.SetEchoTag(0xAAAAAAAAAAAAAAAA)
	request.SetInsertValues(insert)

	connection, err := net.Dial("tcp", "bunker.cs.berkeley.edu:4410")
	defer connection.Close()
	if err != nil {
		fmt.Printf("Error in connecting: %v", err)
		return
	}

	written, sendErr := segment.WriteTo(connection)
	if sendErr != nil {
		fmt.Printf("Error in sending request: %v\n", sendErr)
		return
	}
	fmt.Printf("Sent %v bytes\n", written)
	
	responseSegment, respErr := capnp.ReadFromStream(connection, nil)
	if respErr != nil {
		fmt.Printf("Error in receiving response: %v\n", err)
		return
	}

	var response Response = ReadRootResponse(responseSegment)
	fmt.Printf("echoTag: %v\n", response.EchoTag())
	fmt.Printf("echoTag should be: %v\n", uint64(0xAAAAAAAAAAAAAAAA))
	var status StatusCode = response.StatusCode()
	fmt.Printf("statusCode: %v\n", status)
	var respType Response_Which = response.Which()
	fmt.Printf("respType: %v\n", respType)
}
