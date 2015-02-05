package main

import (
	"fmt"
	"math"
	"io/ioutil"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	cparse "github.com/SoftwareDefinedBuildings/sync2_quasar/configparser"
	cpint "github.com/SoftwareDefinedBuildings/quasar/cpinterface"
	capnp "github.com/glycerine/go-capnproto"
	uuid "code.google.com/p/go-uuid/uuid"
)

var (
	TOTAL_RECORDS int64
	TCP_CONNECTIONS int
	POINTS_PER_MESSAGE uint32
	NANOS_BETWEEN_POINTS int64
	DB_ADDR string
	NUM_STREAMS int
    FIRST_TIME int64
)

var (
	VERIFY_RESPONSES = false
)

var points_sent uint32 = 0

var points_received uint32 = 0

var points_verified uint32 = 0

type InsertMessagePart struct {
	segment *capnp.Segment
	request *cpint.Request
	insert *cpint.CmdInsertValues
	recordList *cpint.Record_List
	pointerList *capnp.PointerList
	record *cpint.Record
}

var insertPool sync.Pool = sync.Pool{
	New: func () interface{} {
		var seg *capnp.Segment = capnp.NewBuffer(nil)
		var req cpint.Request = cpint.NewRootRequest(seg)
		var insert cpint.CmdInsertValues = cpint.NewCmdInsertValues(seg)
		insert.SetSync(false)
		var recList cpint.Record_List = cpint.NewRecordList(seg, int(POINTS_PER_MESSAGE))
		var pointList capnp.PointerList = capnp.PointerList(recList)
		var record cpint.Record = cpint.NewRecord(seg)
		return InsertMessagePart{
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
	return math.Sin(float64(time) / 1000000000)
}

func min64 (x1 int64, x2 int64) int64 {
	if x1 < x2 {
		return x1
	} else {
		return x2
	}
}

func insert_data(uuid []byte, start int64, connection net.Conn, sendLock *sync.Mutex, connLock *sync.Mutex, connID int, response chan int) {
	var id uint64 = 0
	var time int64 = start
	var endTime int64
	var numPoints uint32 = POINTS_PER_MESSAGE
	if TOTAL_RECORDS < 0 {
		endTime = 0x7FFFFFFFFFFFFFFF
	} else {
		endTime = min64(start + TOTAL_RECORDS * NANOS_BETWEEN_POINTS, 0x7FFFFFFFFFFFFFFF)
	}
	for time < endTime {
		var mp InsertMessagePart = insertPool.Get().(InsertMessagePart)
		
		segment := mp.segment
		request := *mp.request
		insert := *mp.insert
		recordList := *mp.recordList
		pointerList := *mp.pointerList
		record := *mp.record
		
		request.SetEchoTag(id)
		insert.SetUuid(uuid)

		if endTime - time < int64(POINTS_PER_MESSAGE) * NANOS_BETWEEN_POINTS {
			numPoints = uint32((endTime - time) / NANOS_BETWEEN_POINTS)
			recordList = cpint.NewRecordList(segment, int(numPoints))
			pointerList = capnp.PointerList(recordList)
		}

		var i int
		for i = 0; uint32(i) < numPoints; i++ {
			record.SetTime(time)
			record.SetValue(get_time_value(time))
			pointerList.Set(i, capnp.Object(record))
			time += NANOS_BETWEEN_POINTS
		}
		insert.SetValues(recordList)
		request.SetInsertValues(insert)
		
		var sendErr error
		
		(*sendLock).Lock()
		_, sendErr = segment.WriteTo(connection)
		(*sendLock).Unlock()

		insertPool.Put(mp)
		
		if sendErr != nil {
			fmt.Printf("Error in sending request: %v\n", sendErr)
			return
		}
		atomic.AddUint32(&points_sent, uint32(numPoints))

		(*connLock).Lock()
		responseSegment, respErr := capnp.ReadFromStream(connection, nil)
		(*connLock).Unlock()
		
		if respErr != nil {
			fmt.Printf("Error in receiving response: %v\n", respErr)
			response <- -1
			return
		}
		
		response := cpint.ReadRootResponse(responseSegment)
		status := response.StatusCode()
		if status == cpint.STATUSCODE_OK {
			atomic.AddUint32(&points_received, uint32(numPoints))
		} else {
			fmt.Printf("Quasar returns status code %s!\n", status)
		}
		id++
	}
	response <- connID
}

type QueryMessagePart struct {
	segment *capnp.Segment
	request *cpint.Request
	query *cpint.CmdQueryStandardValues
}

var queryPool sync.Pool = sync.Pool{
	New: func () interface{} {
		var seg *capnp.Segment = capnp.NewBuffer(nil)
		var req cpint.Request = cpint.NewRootRequest(seg)
		var query cpint.CmdQueryStandardValues = cpint.NewCmdQueryStandardValues(seg)
		query.SetVersion(0)
		return QueryMessagePart{
			segment: seg,
			request: &req,
			query: &query,
		}
	},
}

func query_data(uuid []byte, start int64, connection net.Conn, sendLock *sync.Mutex, connLock *sync.Mutex, connID int, response chan int) {
	var id uint64 = 0
	var time int64 = start
	var endTime int64
	var numPoints uint32 = POINTS_PER_MESSAGE
	if TOTAL_RECORDS < 0 {
		endTime = 0x7FFFFFFFFFFFFFFF
	} else {
		endTime = min64(start + TOTAL_RECORDS * NANOS_BETWEEN_POINTS, 0x7FFFFFFFFFFFFFFF)
	}
	for time < endTime {
		var mp QueryMessagePart = queryPool.Get().(QueryMessagePart)
		
		segment := mp.segment
		request := *mp.request
		query := *mp.query
		
		request.SetEchoTag(id)
		query.SetUuid(uuid)
		query.SetStartTime(time)
		if endTime - time < int64(POINTS_PER_MESSAGE) * NANOS_BETWEEN_POINTS {
			numPoints = uint32((endTime - time) / NANOS_BETWEEN_POINTS)
		}
		time += NANOS_BETWEEN_POINTS * int64(numPoints)
		query.SetEndTime(time)

		request.SetQueryStandardValues(query);

		var sendErr error
		
		(*sendLock).Lock()
		_, sendErr = segment.WriteTo(connection)
		(*sendLock).Unlock()

		queryPool.Put(mp)
		
		if sendErr != nil {
			fmt.Printf("Error in sending request: %v\n", sendErr)
			return
		}
		atomic.AddUint32(&points_sent, uint32(numPoints))

		(*connLock).Lock()
		responseSegment, respErr := capnp.ReadFromStream(connection, nil)
		(*connLock).Unlock()
		
		if respErr != nil {
			fmt.Printf("Error in receiving response: %v\n", respErr)
			response <- -1
			return
		}
		
		response := cpint.ReadRootResponse(responseSegment)
		status := response.StatusCode()
		if status == cpint.STATUSCODE_OK {
			atomic.AddUint32(&points_received, uint32(numPoints))
		} else {
			fmt.Printf("Quasar returns status code %s!\n", status)
		}

		if VERIFY_RESPONSES {
			records := response.Records().Values()
			var num_records uint32 = uint32(records.Len())
			var expected float64 = 0
			var received float64 = 0
			var recTime int64 = 0
			for m := 0; uint32(m) < num_records; m++ {
				received = records.At(m).Value()
				recTime = records.At(m).Time()
				expected = get_time_value(recTime)
				if received == expected {
					atomic.AddUint32(&points_verified, uint32(1))
				} else {
					fmt.Printf("Expected (%v, %v), got (%v, %v)\n", recTime, expected, recTime, received);
					os.Exit(1)
				}
			}
		}

		id++
	}
	response <- connID
}

func getIntFromConfig(key string, config map[string]interface{}) int64 {
    elem, ok := config[key]
    if !ok {
        fmt.Printf("Could not read %v from config file\n", key)
        os.Exit(1)
    }
    intval, err := strconv.ParseInt(elem.(string), 0, 64)
    if err != nil {
        fmt.Printf("Could not parse %v to an int64: %v\n", elem, err)
        os.Exit(1)
    }
    return intval
}


func main() {
	args := os.Args[1:]
	var send_messages func([]byte, int64, net.Conn, *sync.Mutex, *sync.Mutex, int, chan int)

	if len(args) > 0 && args[0] == "-i" {
		fmt.Println("Insert mode");
		send_messages = insert_data
	} else if len(args) > 0 && args[0] == "-q" {
		fmt.Println("Query mode");
		send_messages = query_data
	} else if len(args) > 0 && args[0] == "-v" {
		fmt.Println("Query mode with verification");
		send_messages = query_data
		VERIFY_RESPONSES = true
	} else {
		fmt.Println("Usage: use -i to insert data and -q to query data. To query data and verify the response, use the -v flag instead of the -q flag.");
		return
	}
	
	/* Read the configuration file. */
	
	configfile, err := ioutil.ReadFile("loadConfig.ini")
	if err != nil {
	    fmt.Printf("Could not read loadConfig.ini: %v\n", err)
	    return
	}
	
	config, isErr := cparse.ParseConfig(string(configfile))
    if isErr {
        fmt.Println("There were errors while parsing loadConfig.ini. See above.")
        return
    }
    
    TOTAL_RECORDS = getIntFromConfig("TOTAL_RECORDS", config)
    TCP_CONNECTIONS = int(getIntFromConfig("TCP_CONNECTIONS", config))
    POINTS_PER_MESSAGE = uint32(getIntFromConfig("POINTS_PER_MESSAGE", config))
    NANOS_BETWEEN_POINTS = getIntFromConfig("NANOS_BETWEEN_POINTS", config)
    NUM_STREAMS = int(getIntFromConfig("NUM_STREAMS", config))
    FIRST_TIME = getIntFromConfig("FIRST_TIME", config)
    
    addr, ok := config["DB_ADDR"]
    if !ok {
        fmt.Println("Could not read DB_ADDR from config file")
        os.Exit(1)
    }
    DB_ADDR = addr.(string)
    
    var uuids [][]byte = make([][]byte, NUM_STREAMS);

	var j int = 0
	for j = 0; j < len(args) - 1; j++ {
		uuids[j] = uuid.Parse(args[j + 1]);
	}
	for j < NUM_STREAMS {
		uuids[j] = []byte(uuid.NewRandom())
		j++
	}
	fmt.Printf("Using UUIDs ")
	for j = 0; j < NUM_STREAMS; j++ {
		fmt.Printf("%s ", uuid.UUID(uuids[j]).String())
	}
	fmt.Printf("\n")
	
	runtime.GOMAXPROCS(runtime.NumCPU())
	var connections []net.Conn = make([]net.Conn, TCP_CONNECTIONS)
	var sendLocks []*sync.Mutex = make([]*sync.Mutex, TCP_CONNECTIONS)
	var recvLocks []*sync.Mutex = make([]*sync.Mutex, TCP_CONNECTIONS)
	
	fmt.Println("Creating connections...")
	for i := range connections {
		connections[i], err = net.Dial("tcp", DB_ADDR)
		if err == nil {
			fmt.Printf("Created connection %v\n", i)
			sendLocks[i] = &sync.Mutex{}
			recvLocks[i] = &sync.Mutex{}
		} else {
			fmt.Printf("Could not connect to database: %s\n", err)
			os.Exit(1);
		}
	}
	fmt.Println("Finished creating connections")

	var connIndex int = 0

	var sig chan int = make(chan int)
	var usingConn []int = make([]int, TCP_CONNECTIONS)
	
	for z := 0; z < NUM_STREAMS; z++ {
		go send_messages(uuids[z], FIRST_TIME, connections[connIndex], sendLocks[connIndex], recvLocks[connIndex], connIndex, sig)
		usingConn[connIndex]++
		connIndex = (connIndex + 1) % TCP_CONNECTIONS
	}

	go func () {
		for {
			time.Sleep(time.Second)
			fmt.Printf("Sent %v, ", points_sent)
			atomic.StoreUint32(&points_sent, 0)
			fmt.Printf("Received %v\n", points_received)
			atomic.StoreUint32(&points_received, 0)
			points_received = 0
		}
	}()

	var response int
	for k := 0; k < NUM_STREAMS; k++ {
		response = <-sig
		if response < 0 {
			for m := 0; m < TCP_CONNECTIONS; m++ {
				if usingConn[m] != 0 {
					connections[m].Close()
				}
			}
			break
		} else {
			usingConn[response]--
			if usingConn[response] == 0 {
				connections[response].Close()
				fmt.Printf("Closed connection %v\n", response)
			}
		}
	}

	for k := NUM_STREAMS; k < TCP_CONNECTIONS; k++ {
		connections[k].Close()
		fmt.Printf("Closed connection %v\n", k)
	}

	fmt.Printf("Sent %v, Received %v\n", points_sent, points_received)
	if (VERIFY_RESPONSES) {
		fmt.Printf("%v points are verified to be correct\n", points_verified);
	}
	
	fmt.Println("Finished")
}
