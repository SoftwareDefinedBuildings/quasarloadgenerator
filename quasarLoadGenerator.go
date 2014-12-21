package main

import (
	"fmt"
	"math"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"time"
	capnp "github.com/glycerine/go-capnproto"
)

var pointsInserted uint64
var pointsLock sync.Mutex = sync.Mutex{}

func pushRandomPoints(connection net.Conn, uuid []byte, id uint64, responseChan chan int) {
	var segment *capnp.Segment = capnp.NewBuffer(nil)
	var request Request = NewRootRequest(segment)
	request.SetEchoTag(id)
	var insert CmdInsertValues = NewCmdInsertValues(segment)
	insert.SetUuid(uuid)
	insert.SetSync(false)
	var record1 Record = NewRecord(segment)
	var record2 Record = NewRecord(segment)
	var recordList Record_List = NewRecordList(segment, 2)
	var pointerList capnp.PointerList = capnp.PointerList(recordList)
	var (
		time1 int64
		value1 float64
		time2 int64
		value2 float64
		sendErr error
		responseSegment *capnp.Segment
		respErr error
		response Response
		status StatusCode
	)
	for {
		// Create a record list containing two data points, and add it
		time1 = rand.Int63()
		value1 = math.Sqrt(float64(time1))
		record1.SetTime(time1)
		record1.SetValue(value1)
		time2 = rand.Int63()
		value2 = math.Sqrt(float64(time2))
		record2.SetTime(time2)
		record2.SetValue(value2)
		pointerList.Set(0, capnp.Object(record1))
		pointerList.Set(1, capnp.Object(record2))
		insert.SetValues(recordList)
		request.SetInsertValues(insert)
		
		_, sendErr = segment.WriteTo(connection)
		if sendErr != nil {
			fmt.Printf("Error in sending request: %v\n", sendErr)
			return
		}

		//fmt.Printf("Sent message\n")

		responseSegment, respErr = capnp.ReadFromStream(connection, nil)
		if respErr != nil {
			fmt.Printf("Error in receiving response: %v\n", respErr)
			return
		}

		//fmt.Printf("Received response\n")
		
		response = ReadRootResponse(responseSegment)
		status = response.StatusCode()
		if status == STATUSCODE_OK {
			pointsLock.Lock()
			pointsInserted += 2
			pointsLock.Unlock()
		} else {
			fmt.Printf("Quasar returns status code %s!\n", status)
			responseChan <- 1
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	var (
		connection net.Conn
		err error
	)
	var i int
	numThreads := runtime.NumCPU() << 4
	respChan := make(chan int)
	for i = 0; i < numThreads; i++ {
		connection, err = net.Dial("tcp", "bunker.cs.berkeley.edu:4410")
		if err != nil {
			fmt.Printf("Error in connecting: %v\n", err)
			return
		}
		defer connection.Close()
		go pushRandomPoints(connection, []byte("cd29a8e6-88b5-11e4-81a8-0026b6df9cf2"), uint64(i), respChan)
		fmt.Printf("Started thread %v\n", i)
	}

    pointsLock.Lock()
	fmt.Printf("%v points inserted during initialization\n", pointsInserted)
	pointsInserted = 0
	pointsLock.Unlock()
	go func () { // prints out how many points were inserted in each second
		for {
			time.Sleep(time.Second)
			pointsLock.Lock()
			fmt.Printf("%v points inserted per second\n", pointsInserted);
			pointsInserted = 0
			pointsLock.Unlock()
		}
	}()

	fmt.Println("Started daemon thread")
    
	var threadResponse int
	for threadResponse != 1 {
		threadResponse = <-respChan
	}
}
