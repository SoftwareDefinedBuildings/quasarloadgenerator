package main

import (
	"fmt"
	"math"
	"net"
	"runtime"
	"sync"
	"time"
	capnp "github.com/glycerine/go-capnproto"
)

const (
	TOTAL_RECORDS = -1
	NUM_THREADS = 1
	TCP_CONNECTIONS = 1
	POINTS_PER_MESSAGE = 2
	NANOS_BETWEEN_POINTS = 1
	DB_ADDR = "bunker.cs.berkeley.edu:4410"
)

var (
	FIRST_TIME = time.Now().UnixNano()
	UUID = []byte("7fb5707c-894b-11e4-b60a-0026b6df9cf2")
)

var outstanding map[uint64]int64 = make(map[uint64]int64)
var mapLock sync.Mutex = sync.Mutex{}

var points_sent uint32 = 0
var sentLock sync.Mutex = sync.Mutex{}

var points_received uint32 = 0
var recvLock sync.Mutex = sync.Mutex{}

type MessagePart struct {
	segment *capnp.Segment
	request *Request
	insert *CmdInsertValues
	recordList *Record_List
	pointerList *capnp.PointerList
	record *Record
}

var messagePool sync.Pool = sync.Pool{
	New: func () interface{} {
		var seg *capnp.Segment = capnp.NewBuffer(nil)
		var req Request = NewRootRequest(seg)
		var insert CmdInsertValues = NewCmdInsertValues(seg)
		insert.SetUuid(UUID)
		insert.SetSync(false)
		var recList Record_List = NewRecordList(seg, POINTS_PER_MESSAGE)
		var pointList capnp.PointerList = capnp.PointerList(recList)
		var record Record = NewRecord(seg)
		return MessagePart{
			segment: seg,
			request: &req,
			insert: &insert,
			recordList: &recList,
			pointerList: &pointList,
			record: &record,
		}
	},
}

func get_time_value (time int64) float64 {
	return math.Sin(float64(time))
}

func send_one_message(start int64, connection net.Conn, id uint64, comm chan int, sendLock *sync.Mutex) {
	var mp MessagePart = messagePool.Get().(MessagePart)
	defer messagePool.Put(mp)

	segment := mp.segment
	request := *mp.request
	insert := *mp.insert
	recordList := *mp.recordList
	pointerList := *mp.pointerList
	record := *mp.record

	request.SetEchoTag(id)

	var time int64 = start
	for i := 0; i < POINTS_PER_MESSAGE; i++ {
		record.SetTime(time)
		record.SetValue(get_time_value(time))
		pointerList.Set(i, capnp.Object(record))
		time += NANOS_BETWEEN_POINTS
	}
	insert.SetValues(recordList)
	request.SetInsertValues(insert)
	
	mapLock.Lock()
	outstanding[id] = start
	mapLock.Unlock()

	var sendErr error

	(*sendLock).Lock()
	_, sendErr = segment.WriteTo(connection)
	(*sendLock).Lock()

	if sendErr != nil {
		fmt.Printf("Error in sending request: %v\n", sendErr)
		mapLock.Lock()
		delete(outstanding, id)
		mapLock.Unlock()
		comm <- 1
		return
	}
	sentLock.Lock()
	points_sent += POINTS_PER_MESSAGE
	sentLock.Unlock()
	comm <- 0
}

func receive_one_message(connection net.Conn, recvLock *sync.Mutex) {
	(*recvLock).Lock()
	responseSegment, respErr := capnp.ReadFromStream(connection, nil)
	(*recvLock).Unlock()

	if respErr != nil {
		fmt.Printf("Error in receiving response: %v\n", respErr)
		return
	}
	
	response := ReadRootResponse(responseSegment)
	status := response.StatusCode()
	if status == STATUSCODE_OK {
		id := response.EchoTag()
		delete(outstanding, id)
		recvLock.Lock()
		points_received += POINTS_PER_MESSAGE
		recvLock.Unlock()
	} else {
		fmt.Printf("Quasar returns status code %s!\n", status)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var connections []net.Conn = make([]net.Conn, TCP_CONNECTIONS)
	var sendLocks []*sync.Mutex = make([]*sync.Mutex, TCP_CONNECTIONS)
	var recvLocks []*sync.Mutex = make([]*sync.Mutex, TCP_CONNECTIONS)
	var err error
	fmt.Println("Creating connections...")
	for i := range connections {
		connections[i], err = net.Dial("tcp", DB_ADDR)
		if err == nil {
			fmt.Printf("Created connection %v\n", i)
			defer connections[i].Close()
			sendLocks[i] = &sync.Mutex{}
			recvLocks[i] = &sync.Mutex{}
		} else {
			fmt.Printf("Could not connect to database: %s\n", err)
			return
		}
	}
	fmt.Println("Finished creating connections")

	recv := make(chan int, NUM_THREADS)
	for j := 0; j < NUM_THREADS; j++ {
		recv <- 0
	}

	go func () { // Set up a goroutine to repeatedly print stats
		for {
			time.Sleep(time.Second)
			sentLock.Lock()
			fmt.Printf("Sent %v, ", points_sent)
			points_sent = 0
			sentLock.Unlock()
			recvLock.Lock()
			fmt.Printf("Received %v\n", points_received)
			points_received = 0
			recvLock.Unlock()
		}
	}()

	var sig int
	var connIndex int = 0
	var sendID uint64 = 0
	var pointTime int64 = FIRST_TIME

	for {
		sig = <-recv
		if sig == 0 {
			go send_one_message(pointTime, connections[connIndex], sendID, recv, sendLocks[connIndex])
			go receive_one_message(connections[connIndex], recvLocks[connIndex])
			pointTime += POINTS_PER_MESSAGE * NANOS_BETWEEN_POINTS
			connIndex = (connIndex + 1) % TCP_CONNECTIONS
			sendID++
		} else {
			break
		}
	}
}
