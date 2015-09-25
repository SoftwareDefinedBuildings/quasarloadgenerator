package main

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	cparse "github.com/SoftwareDefinedBuildings/sync2_quasar/configparser"
	cpint "github.com/SoftwareDefinedBuildings/btrdb/cpinterface"
	capnp "github.com/glycerine/go-capnproto"
	uuid "code.google.com/p/go-uuid/uuid"
)

var (
	TOTAL_RECORDS int64
	TCP_CONNECTIONS int
	POINTS_PER_MESSAGE uint32
	NANOS_BETWEEN_POINTS int64
	NUM_SERVERS int
	NUM_STREAMS int
	FIRST_TIME int64
	RAND_SEED int64
	PERM_SEED int64
	MAX_TIME_RANDOM_OFFSET float64
	DETERMINISTIC_KV bool
	GET_MESSAGE_TIMES bool
	MAX_CONCURRENT_MESSAGES uint64
	STATISTICAL_PW uint8
	
	orderBitlength uint
	orderBitmask uint64
	statistical bool
	statisticalBitmaskLower int64
	statisticalBitmaskUpper int64
)

var (
	VERIFY_RESPONSES = false
	PRINT_ALL = false
)

var points_sent uint32 = 0

var points_received uint32 = 0

var points_verified uint32 = 0

type TransactionData struct {
	sendTime int64
	respTime int64
}

type ConnectionID struct {
	serverIndex int
	connectionIndex int
}

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

var get_time_value func (int64, *rand.Rand) float64

func getRandValue (time int64, randGen *rand.Rand) float64 {
	// We technically don't need time anymore, but if we switch back to a sine wave later it's useful to keep it around as a parameter
	return randGen.NormFloat64()
}

var sines [100]float64

var sinesIndex = 100
func getSinusoidValue (time int64, randGen *rand.Rand) float64 {
	sinesIndex = (sinesIndex + 1) % 100;
	return sines[sinesIndex];
}

func min64 (x1 int64, x2 int64) int64 {
	if x1 < x2 {
		return x1
	} else {
		return x2
	}
}

func insert_data(uuid []byte, start *int64, connection net.Conn, sendLock *sync.Mutex, connID ConnectionID, response chan ConnectionID, streamID int, cont chan uint32, randGen *rand.Rand, permutation []int64, numMessages uint64, history []TransactionData) {
	var currTime int64 = *start
	var j uint64
	var echoTagBase uint64 = uint64(streamID) << orderBitlength
	
	// I used to get from the pool and put it back every iteration. Now I just get it once and keep it.
	var mp InsertMessagePart = insertPool.Get().(InsertMessagePart)
	segment := mp.segment
	request := mp.request
	insert := mp.insert
	recordList := mp.recordList;
	pointerList := mp.pointerList
	record := *mp.record
	
	insert.SetUuid(uuid)
	insert.SetValues(*recordList)
	request.SetInsertValues(*insert)
	for j = 0; j < numMessages; j++ {
		currTime = permutation[j]
		
		request.SetEchoTag(echoTagBase | j)

		var i int
		for i = 0; uint32(i) < POINTS_PER_MESSAGE; i++ {
			if DETERMINISTIC_KV {
				record.SetTime(currTime)
			} else {
				record.SetTime(currTime + int64(randGen.Float64() * MAX_TIME_RANDOM_OFFSET))
			}
			record.SetValue(get_time_value(currTime, randGen))
			pointerList.Set(i, capnp.Object(record))
			currTime += NANOS_BETWEEN_POINTS
		}
		
		cont <- POINTS_PER_MESSAGE // Blocks if we haven't received enough responses
		
		var sendErr error
		
		sendLock.Lock()
		_, sendErr = segment.WriteTo(connection)
		sendLock.Unlock()
		if GET_MESSAGE_TIMES { // write send time to history
			history[j].sendTime = time.Now().UnixNano()
		}
		
		if sendErr != nil {
			fmt.Printf("Error in sending request: %v\n", sendErr)
			return
		}
		atomic.AddUint32(&points_sent, POINTS_PER_MESSAGE)
	}
	
	insertPool.Put(mp)
	
	for j = 0; j < MAX_CONCURRENT_MESSAGES; j++ {
	    // block until everything is fully processed
	    cont <- 0
	}
	response <- connID
}

type QueryMessagePart struct {
	segment *capnp.Segment
	request *cpint.Request
	query *cpint.CmdQueryStandardValues
}

var standQueryPool sync.Pool = sync.Pool{
	New: func () interface{} {
		var seg *capnp.Segment = capnp.NewBuffer(nil)
		var req cpint.Request = cpint.NewRootRequest(seg)
		var query cpint.CmdQueryStandardValues = cpint.NewCmdQueryStandardValues(seg)
		query.SetVersion(0)
		req.SetQueryStandardValues(query)
		return QueryMessagePart{
			segment: seg,
			request: &req,
			query: &query,
		}
	},
}

func query_stand_data(uuid []byte, start *int64, connection net.Conn, sendLock *sync.Mutex, connID ConnectionID, response chan ConnectionID, streamID int, cont chan uint32, randGen *rand.Rand, permutation []int64, numMessages uint64, history []TransactionData) {
	var currTime int64 = *start
	var messageLength int64 = NANOS_BETWEEN_POINTS * int64(POINTS_PER_MESSAGE)
	var j uint64
	var echoTagBase = uint64(streamID) << orderBitlength

	// I used to get from the pool and put it back every iteration. Now I just get it once and keep it.
	var mp QueryMessagePart = standQueryPool.Get().(QueryMessagePart)
	
	segment := mp.segment
	request := mp.request
	query := mp.query

	query.SetUuid(uuid)
	
	for j = 0; j < numMessages; j++ {
		currTime = permutation[j]
	
		request.SetEchoTag(echoTagBase | j)
		query.SetStartTime(currTime)
		query.SetEndTime(currTime + messageLength)

		cont <- POINTS_PER_MESSAGE // Blocks if we haven't received enough responses

		var sendErr error
	
		sendLock.Lock()
		_, sendErr = segment.WriteTo(connection)
		sendLock.Unlock()
		if GET_MESSAGE_TIMES { // write send time to history
			history[j].sendTime = time.Now().UnixNano()
		}
	
		if sendErr != nil {
			fmt.Printf("Error in sending request: %v\n", sendErr)
			os.Exit(1)
		}
		atomic.AddUint32(&points_sent, POINTS_PER_MESSAGE)
	}

	standQueryPool.Put(mp)

	for j = 0; j < MAX_CONCURRENT_MESSAGES; j++ {
		// block until everything is fully processed
		cont <- 0
	}
	response <- connID
}

type StatQueryMessagePart struct {
	segment *capnp.Segment
	request *cpint.Request
	query *cpint.CmdQueryStatisticalValues
}

var statQueryPool sync.Pool = sync.Pool{
	New: func () interface{} {
		var seg *capnp.Segment = capnp.NewBuffer(nil)
		var req cpint.Request = cpint.NewRootRequest(seg)
		var query cpint.CmdQueryStatisticalValues = cpint.NewCmdQueryStatisticalValues(seg)
		query.SetVersion(0)
		query.SetPointWidth(STATISTICAL_PW)
		req.SetQueryStatisticalValues(query)
		return StatQueryMessagePart{
			segment: seg,
			request: &req,
			query: &query,
		}
	},
}

func query_stat_data(uuid []byte, start *int64, connection net.Conn, sendLock *sync.Mutex, connID ConnectionID, response chan ConnectionID, streamID int, cont chan uint32, randGen *rand.Rand, permutation []int64, numMessages uint64, history []TransactionData) {
	var currTime int64 = *start
	var messageLength int64 = NANOS_BETWEEN_POINTS * int64(POINTS_PER_MESSAGE)
	var j uint64
	var echoTagBase = uint64(streamID) << orderBitlength

	// I used to get from the pool and put it back every iteration. Now I just get it once and keep it.
	var mp StatQueryMessagePart = statQueryPool.Get().(StatQueryMessagePart)
	
	segment := mp.segment
	request := mp.request
	query := mp.query

	query.SetUuid(uuid)
	
	var recordsPerMessage uint32 = uint32(messageLength >> STATISTICAL_PW)
	
	for j = 0; j < numMessages; j++ {
		currTime = permutation[j]
	
		request.SetEchoTag(echoTagBase | j)
		query.SetStartTime(currTime)
		query.SetEndTime(currTime + messageLength)

		cont <- recordsPerMessage // Blocks if we haven't received enough responses

		var sendErr error
	
		sendLock.Lock()
		_, sendErr = segment.WriteTo(connection)
		sendLock.Unlock()
		if GET_MESSAGE_TIMES { // write send time to history
			history[j].sendTime = time.Now().UnixNano()
		}
	
		if sendErr != nil {
			fmt.Printf("Error in sending request: %v\n", sendErr)
			os.Exit(1)
		}
		atomic.AddUint32(&points_sent, recordsPerMessage)
	}

	statQueryPool.Put(mp)

	for j = 0; j < MAX_CONCURRENT_MESSAGES; j++ {
		// block until everything is fully processed
		cont <- 0
	}
	response <- connID
}

func getExpTime(currTime int64, randGen *rand.Rand) int64 {
	if DETERMINISTIC_KV {
		return currTime
	} else {
		return currTime + int64(randGen.Float64() * MAX_TIME_RANDOM_OFFSET)
	}
}

func floatEquals(x float64, y float64) bool {
	return math.Abs(x - y) < 1e-14 * math.Max(math.Abs(x), math.Abs(y))
}

func validateResponses(connection net.Conn, connLock *sync.Mutex, idToChannel []chan uint32, randGens []*rand.Rand, times []int64, tempExpTimes []int64, receivedCounts []uint32, pass *bool, numUsing *int, transactionHistories [][]TransactionData) {
	var buf bytes.Buffer // buffer is sized dynamically
	for true {
		/* I've restructured the code so that this is the only goroutine that receives from the connection.
		   So, the locks aren't necessary anymore. But, I've kept the lock around in case we switch to a different
		   design later on. */
		//connLock.Lock()
		responseSegment, respErr := capnp.ReadFromStream(connection, &buf)
		//connLock.Unlock()
		
		if *numUsing == 0 {
			return
		}
	
		if respErr != nil {
			fmt.Printf("Error in receiving response: %v\n", respErr)
			os.Exit(1)
		}
	
		responseSeg := cpint.ReadRootResponse(responseSegment)
		echoTag := responseSeg.EchoTag()
		id := echoTag >> orderBitlength
		var final bool = responseSeg.Final()
		var channel chan uint32 = idToChannel[id]
		
		if responseSeg.StatusCode() != cpint.STATUSCODE_OK {
			fmt.Printf("Quasar returns status code %s!\n", responseSeg.StatusCode())
			os.Exit(1)
		}

		if VERIFY_RESPONSES {
   			var randGen *rand.Rand = randGens[id]
			var currTime int64 = times[id]
			var expTime int64
			var num_records uint32
			var expected float64 = 0
			if !statistical {
				records := responseSeg.Records().Values()
				num_records = uint32(records.Len())
				var received float64 = 0
				var recTime int64 = 0
				if responseSeg.Final() {
					if num_records + receivedCounts[id] != POINTS_PER_MESSAGE {
						fmt.Printf("Expected %v points in query response, but got %v points instead.\n", POINTS_PER_MESSAGE, num_records)
						*pass = false
					}
					receivedCounts[id] = 0
				} else {
					receivedCounts[id] += num_records
				}
				for m := 0; uint32(m) < num_records; m++ {
					received = records.At(m).Value()
					recTime = records.At(m).Time()
					expTime = getExpTime(currTime, randGen)
					expected = get_time_value(recTime, randGens[id])
					if expTime == recTime && received == expected {
						atomic.AddUint32(&points_verified, uint32(1))
						if PRINT_ALL {
							fmt.Printf("Received expected point (%v, %v)\n", recTime, received)
						}
					} else {
						fmt.Printf("Expected (%v, %v), got (%v, %v)\n", expTime, expected, recTime, received)
						*pass = false
					}
					currTime = currTime + NANOS_BETWEEN_POINTS
				}
			} else {
				records := responseSeg.StatisticalRecords().Values()
				num_records = uint32(records.Len())
				var total_count uint64 = 0
				var expMin float64
				var expMean float64
				var expMax float64
				var expRecTime int64
				var expectedEnd int64
				var expRecCount uint64
				expTime = tempExpTimes[id] // we need this early since the pertubation may push it into a different interval
				for m := 0; uint32(m) < num_records; m++ {
					expRecTime = expTime & statisticalBitmaskUpper
					expectedEnd = expRecTime + (1 << uint(STATISTICAL_PW))
					expRecCount = 0
					expMin = math.Inf(1)
					expMean = 0
					expMax = math.Inf(-1)

					for expTime < expectedEnd {
						expected = get_time_value(expTime, randGen)
						expMin = math.Min(expected, expMin)
						expMean += expected
						expMax = math.Max(expected, expMax)
						expRecCount++
						currTime = currTime + NANOS_BETWEEN_POINTS
						expTime = getExpTime(currTime, randGen)
					}
					expMean /= float64(expRecCount)
					record := records.At(m)
					if expRecTime == record.Time() && floatEquals(expMin, record.Min()) && floatEquals(expMean, record.Mean()) && floatEquals(expMax, record.Max()) && expRecCount == record.Count() {
						atomic.AddUint32(&points_verified, uint32(expRecCount))
						if PRINT_ALL {
							fmt.Printf("Received record (time=%v, min=%v, mean=%v, max=%v, count=%v)\n", record.Time(), record.Min(), record.Mean(), record.Max(), record.Count())
						}
					} else {
						fmt.Printf("Expected (time=%v, min=%v, mean=%v, max=%v, count=%v), got (time=%v, min=%v, mean=%v, max=%v, count=%v)\n", expRecTime, expMin, expMean, expMax, expRecCount, record.Time(), record.Min(), record.Mean(), record.Max(), record.Count())
						*pass = false
					}
					total_count += record.Count()
				}
				if responseSeg.Final() {
					if uint32(total_count) + receivedCounts[id] != POINTS_PER_MESSAGE {
						fmt.Printf("Expected %v points in query response, but got %v points instead.\n", POINTS_PER_MESSAGE, total_count)
						*pass = false
					}
					receivedCounts[id] = 0
				} else {
					receivedCounts[id] += uint32(total_count)
				}
				// We still aren't done. We created an extra random number when we found that expTime is out of range, and we need that same expTime next time we receive something (if we generate it again, we will get the wrong result).
				tempExpTimes[id] = expTime
			}
			times[id] = currTime;
		}
		
		if final {
			atomic.AddUint32(&points_received, <-channel)
			if GET_MESSAGE_TIMES {
				transactionHistories[id][echoTag & orderBitmask].respTime = time.Now().UnixNano()
			}
		}
	}
}

type DeleteMessagePart struct {
	segment *capnp.Segment
	request *cpint.Request
	query *cpint.CmdDeleteValues
}

var deletePool sync.Pool = sync.Pool{
	New: func () interface{} {
		var seg *capnp.Segment = capnp.NewBuffer(nil)
		var req cpint.Request = cpint.NewRootRequest(seg)
		var query cpint.CmdDeleteValues = cpint.NewCmdDeleteValues(seg)
		req.SetEchoTag(0)
		return DeleteMessagePart{
			segment: seg,
			request: &req,
			query: &query,
		}
	},
}

func delete_data(uuid []byte, connection net.Conn, sendLock *sync.Mutex, recvLock *sync.Mutex, startTime int64, endTime int64, connID ConnectionID, response chan ConnectionID) {
	var mp DeleteMessagePart = deletePool.Get().(DeleteMessagePart)
	segment := *mp.segment
	request := *mp.request
	query := *mp.query
	query.SetUuid(uuid)
	query.SetStartTime(startTime)
	query.SetEndTime(endTime)
	request.SetDeleteValues(query)
	
	sendLock.Lock()
	_, sendErr := segment.WriteTo(connection)
	sendLock.Unlock()
	
	deletePool.Put(mp)
	
	if sendErr != nil {
		fmt.Printf("Error in sending request: %v\n", sendErr)
		os.Exit(1)
	}
	
	recvLock.Lock()
	responseSegment, respErr := capnp.ReadFromStream(connection, nil)
	recvLock.Unlock()
	
	if respErr != nil {
		fmt.Printf("Error in receiving response: %v\n", respErr)
		os.Exit(1)
	}

	responseSeg := cpint.ReadRootResponse(responseSegment)
	status := responseSeg.StatusCode()
	
	if status != cpint.STATUSCODE_OK {
		fmt.Printf("Quasar returns status code %s!\n", status)
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

func getServer(uuid []byte) int {
	return int(uint(uuid[0]) % uint(NUM_SERVERS))
}

func bitLength(x int64) uint {
	var times uint = 0
	for x != 0 {
		x >>= 1
		times++
	}
	return times
}

func main() {
	args := os.Args[1:]
	var send_messages func([]byte, *int64, net.Conn, *sync.Mutex, ConnectionID, chan ConnectionID, int, chan uint32, *rand.Rand, []int64, uint64, []TransactionData)
	var DELETE_POINTS bool = false
	var queryMode bool = false
	if len(args) > 0 && args[0] == "-i" {
		fmt.Println("Insert mode");
		send_messages = insert_data
	} else if len(args) > 0 && args[0] == "-q" {
		fmt.Println("Query mode");
		queryMode = true
	} else if len(args) > 0 && args[0] == "-v" {
		fmt.Println("Query mode with verification");
		queryMode = true
		VERIFY_RESPONSES = true
	} else if len(args) > 0 && args[0] == "-p" {
		fmt.Println("Query mode with \"print all\" verification");
		queryMode = true
		VERIFY_RESPONSES = true
		PRINT_ALL = true
	} else if len(args) > 0 && args[0] == "-d" {
		fmt.Println("Delete mode")
		DELETE_POINTS = true
	} else {
		fmt.Println("Usage: use -i to insert data and -q to query data. To query data and verify the response, use the -v flag instead of the -q flag. Use the -d flag to delete data. To get a CPU profile, add a file name after -i, -v, or -q.");
		return
	}
	
	/* Check if the user has requested a CPU Profile. */
	if len(args) > 1 {
		f, err := os.Create(args[1])
		if err != nil {
			fmt.Println(err)
			return;
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
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
	NUM_SERVERS = int(getIntFromConfig("NUM_SERVERS", config))
	NUM_STREAMS = int(getIntFromConfig("NUM_STREAMS", config))
	FIRST_TIME = getIntFromConfig("FIRST_TIME", config)
	RAND_SEED = getIntFromConfig("RAND_SEED", config)
	PERM_SEED = getIntFromConfig("PERM_SEED", config)
	var maxConcurrentMessages int64 = getIntFromConfig("MAX_CONCURRENT_MESSAGES", config);
	var timeRandOffset int64 = getIntFromConfig("MAX_TIME_RANDOM_OFFSET", config)
	var pw int64 = getIntFromConfig("STATISTICAL_PW", config);
	if TOTAL_RECORDS <= 0 || TCP_CONNECTIONS <= 0 || POINTS_PER_MESSAGE <= 0 || NANOS_BETWEEN_POINTS <= 0 || NUM_STREAMS <= 0 || maxConcurrentMessages <= 0 {
		fmt.Println("TOTAL_RECORDS, TCP_CONNECTIONS, POINTS_PER_MESSAGE, NANOS_BETWEEN_POINTS, NUM_STREAMS, and MAX_CONCURRENT_MESSAGES must be positive.")
		os.Exit(1)
	}
	if pw < -1 {
		fmt.Println("STATISTICAL_PW cannot be less than -1.")
		os.Exit(1)
	}
	if (TOTAL_RECORDS % int64(POINTS_PER_MESSAGE)) != 0 {
		fmt.Println("TOTAL_RECORDS must be a multiple of POINTS_PER_MESSAGE.")
		os.Exit(1)
	}
	if timeRandOffset >= NANOS_BETWEEN_POINTS {
		fmt.Println("MAX_TIME_RANDOM_OFFSET must be less than NANOS_BETWEEN_POINTS.")
		os.Exit(1)
	}
	if timeRandOffset > (1 << 53) { // must be exactly representable as a float64
		fmt.Println("MAX_TIME_RANDOM_OFFSET is too large: the maximum value is 2 ^ 53.")
		os.Exit(1)
	}
	if timeRandOffset < 0 {
		fmt.Println("MAX_TIME_RANDOM_OFFSET must be nonnegative.")
		os.Exit(1)
	}
	if VERIFY_RESPONSES && maxConcurrentMessages != 1 {
		fmt.Println("WARNING: MAX_CONCURRENT_MESSAGES is always 1 when verifying responses.")
		maxConcurrentMessages = 1;
	}
	if queryMode {
		if pw >= 0 {
			STATISTICAL_PW = uint8(pw)
			statistical = true
			statisticalBitmaskLower = (int64(1) << uint(STATISTICAL_PW)) - 1
			statisticalBitmaskUpper = ^statisticalBitmaskLower
			send_messages = query_stat_data
		} else {
			statistical = false
			send_messages = query_stand_data
		}
	}
	var nanosPerMessage uint64 = uint64(NANOS_BETWEEN_POINTS) * uint64(POINTS_PER_MESSAGE)
	if VERIFY_RESPONSES && statistical && ((nanosPerMessage & uint64(statisticalBitmaskLower)) != 0 || (FIRST_TIME & statisticalBitmaskLower) != 0) {
		fmt.Println("ERROR: When verifying statistical responses, NANOS_BETWEEN_POINTS * POINTS_PER_MESSAGE (the ns in each query) and FIRST_TIME must be multiples of 2 ^ STATISTICAL_PW.")
		return;
	}
	MAX_CONCURRENT_MESSAGES = uint64(maxConcurrentMessages)
	MAX_TIME_RANDOM_OFFSET = float64(timeRandOffset)
	DETERMINISTIC_KV = (config["DETERMINISTIC_KV"].(string) == "true")
	if VERIFY_RESPONSES && PERM_SEED != 0 && !DETERMINISTIC_KV {
		fmt.Println("ERROR: PERM_SEED must be set to 0 when verifying nondeterministic responses.")
		return;
	}
	GET_MESSAGE_TIMES = (config["GET_MESSAGE_TIMES"].(string) == "true")
	if DETERMINISTIC_KV {
		get_time_value = getSinusoidValue;
		for r := 0; r < 100; r++ {
			sines[r] = math.Sin(2 * math.Pi * float64(r) / 100)
		}
	} else {
		get_time_value = getRandValue;
	}
	
	var remainder int64 = 0
	if TOTAL_RECORDS % int64(POINTS_PER_MESSAGE) != 0 {
		remainder = 1
	}
	var perm_size = (TOTAL_RECORDS / int64(POINTS_PER_MESSAGE)) + remainder
	orderBitlength = bitLength(perm_size - 1)
	if orderBitlength + bitLength(int64(NUM_STREAMS - 1)) > 64 {
		fmt.Println("The number of bits required to store (number of messages - 1) plus the number of bits required to store (NUM_STREAMS - 1) cannot exceed 64.")
		os.Exit(1)
	}
	orderBitmask = (1 << orderBitlength) - 1
	
	var seedGen *rand.Rand = rand.New(rand.NewSource(RAND_SEED))
	var permGen *rand.Rand = rand.New(rand.NewSource(PERM_SEED));
	var randGens []*rand.Rand = make([]*rand.Rand, NUM_STREAMS)
	
	var j int
	var ok bool
	var dbAddrStr interface{}
	var dbAddrs []string = make([]string, NUM_SERVERS)
	for j = 0; j < NUM_SERVERS; j++ {
		dbAddrStr, ok = config[fmt.Sprintf("DB_ADDR%v", j + 1)]
		if !ok {
			break
		}
		dbAddrs[j] = dbAddrStr.(string)
	}
	_, ok = config[fmt.Sprintf("DB_ADDR%v", j + 1)]
	if j != NUM_SERVERS || ok {
		fmt.Println("The number of specified DB_ADDRs must equal NUM_SERVERS.")
		os.Exit(1)
	}
	
	var uuids [][]byte = make([][]byte, NUM_STREAMS)
	
	var uuidStr interface{}
	var uuidParsed uuid.UUID
	for j = 0; j < NUM_STREAMS; j++ {
		uuidStr, ok = config[fmt.Sprintf("UUID%v", j + 1)]
		if !ok {
			break
		}
		uuidParsed = uuid.Parse(uuidStr.(string))
		if uuidParsed == nil {
			fmt.Printf("Invalid UUID %v\n", uuidStr)
			os.Exit(1)
		}
		uuids[j] = []byte(uuidParsed)
	}
	_, ok = config[fmt.Sprintf("UUID%v", j + 1)]
	if j != NUM_STREAMS || ok {
		fmt.Println("The number of specified UUIDs must equal NUM_STREAMS.")
		os.Exit(1)
	}
	fmt.Printf("Using UUIDs ")
	for j = 0; j < NUM_STREAMS; j++ {
		fmt.Printf("%s ", uuid.UUID(uuids[j]).String())
	}
	fmt.Printf("\n")
	
	runtime.GOMAXPROCS(runtime.NumCPU())
	var connections [][]net.Conn = make([][]net.Conn, NUM_SERVERS)
	var sendLocks [][]*sync.Mutex = make([][]*sync.Mutex, NUM_SERVERS)
	var recvLocks [][]*sync.Mutex = make([][]*sync.Mutex, NUM_SERVERS)
	
	for s := range dbAddrs {
		fmt.Printf("Creating connections to %v...\n", dbAddrs[s])
		connections[s] = make([]net.Conn, TCP_CONNECTIONS)
		sendLocks[s] = make([]*sync.Mutex, TCP_CONNECTIONS)
		recvLocks[s] = make([]*sync.Mutex, TCP_CONNECTIONS)
		for i := range connections[s] {
			connections[s][i], err = net.Dial("tcp", dbAddrs[s])
			if err == nil {
				fmt.Printf("Created connection %v to %v\n", i, dbAddrs[s])
				sendLocks[s][i] = &sync.Mutex{}
				recvLocks[s][i] = &sync.Mutex{}
			} else {
				fmt.Printf("Could not connect to database: %s\n", err)
				os.Exit(1);
			}
		}
	}
	fmt.Println("Finished creating connections")

	var serverIndex int = 0
	var streamCounts []int = make([]int, NUM_SERVERS)
	var connIndex int

	var sig chan ConnectionID = make(chan ConnectionID)
	var usingConn [][]int = make([][]int, NUM_SERVERS)
	for y := 0; y < NUM_SERVERS; y++ {
		usingConn[y] = make([]int, TCP_CONNECTIONS)
	}
	var idToChannel []chan uint32 = make([]chan uint32, NUM_STREAMS)
	var cont chan uint32
	var randGen *rand.Rand
	var startTimes []int64 = make([]int64, NUM_STREAMS)
	var verification_test_pass bool = true
	var perm [][]int64 = make([][]int64, NUM_STREAMS)
	var pointsReceived []uint32
	var tempExpTimes []int64 = nil
	if VERIFY_RESPONSES {
		pointsReceived = make([]uint32, NUM_STREAMS)
		if statistical {
			tempExpTimes = make([]int64, NUM_STREAMS)
		}
	} else {
		pointsReceived = nil
	}
	
	var transactionHistories [][]TransactionData = make([][]TransactionData, NUM_STREAMS)
	for p := range transactionHistories {
		if GET_MESSAGE_TIMES {
			transactionHistories[p] = make([]TransactionData, perm_size)
		} else {
			transactionHistories[p] = nil
		}
	}	
	
	var f int64
	for e := 0; e < NUM_STREAMS; e++ {
		perm[e] = make([]int64, perm_size)
		if PERM_SEED == 0 {
			for f = 0; f < perm_size; f++ {
				perm[e][f] = FIRST_TIME + NANOS_BETWEEN_POINTS * int64(POINTS_PER_MESSAGE) * f
			}
		} else {
			x := permGen.Perm(int(perm_size))
			for f = 0; f < perm_size; f++ {
				perm[e][f] = FIRST_TIME + NANOS_BETWEEN_POINTS * int64(POINTS_PER_MESSAGE) * int64(x[f])
			}
		}
	}
	fmt.Println("Finished generating insert/query order");
	
	var finished bool = false
	
	var startTime int64 = time.Now().UnixNano()
	if DELETE_POINTS {
		for g := 0; g < NUM_STREAMS; g++ {
			serverIndex = getServer(uuids[g])
			connIndex = streamCounts[serverIndex] % TCP_CONNECTIONS
			go delete_data(uuids[g], connections[serverIndex][connIndex], sendLocks[serverIndex][connIndex], recvLocks[serverIndex][connIndex], FIRST_TIME, FIRST_TIME + NANOS_BETWEEN_POINTS * TOTAL_RECORDS, ConnectionID{serverIndex, connIndex}, sig)
			streamCounts[serverIndex]++
		}
	} else {
		for z := 0; z < NUM_STREAMS; z++ {
			cont = make(chan uint32, maxConcurrentMessages)
			idToChannel[z] = cont
			randGen = rand.New(rand.NewSource(seedGen.Int63()))
			randGens[z] = randGen
			startTimes[z] = FIRST_TIME
			serverIndex = getServer(uuids[z])
			connIndex = streamCounts[serverIndex] % TCP_CONNECTIONS
			go send_messages(uuids[z], &startTimes[z], connections[serverIndex][connIndex], sendLocks[serverIndex][connIndex], ConnectionID{serverIndex, connIndex}, sig, z, cont, randGen, perm[z], uint64(perm_size), transactionHistories[z])
			usingConn[serverIndex][connIndex]++
			streamCounts[serverIndex]++
		}
	
		if VERIFY_RESPONSES && statistical {
			for v := 0; v < NUM_STREAMS; v++ {
				tempExpTimes[v] = getExpTime(startTimes[v], randGens[v])
			}
		}
	
		for serverIndex = 0; serverIndex < NUM_SERVERS; serverIndex++ {
			for connIndex = 0; connIndex < TCP_CONNECTIONS; connIndex++ {
				go validateResponses(connections[serverIndex][connIndex], recvLocks[serverIndex][connIndex], idToChannel, randGens, startTimes, tempExpTimes, pointsReceived, &verification_test_pass, &usingConn[serverIndex][connIndex], transactionHistories)
			}
		}
		
		/* Handle ^C */
		interrupt := make(chan os.Signal)
		signal.Notify(interrupt, os.Interrupt)
		go func() {
			<-interrupt // block until an interrupt happens
			fmt.Println("\nDetected ^C. Abruptly ending program...")
			fmt.Println("The following are the start times of the messages that are currently being inserted/queried:")
			for i := 0; i < NUM_STREAMS; i++ {
				fmt.Printf("%v: %v\n", uuid.UUID(uuids[i]).String(), startTimes[i])
			}
			os.Exit(0)
		}()

		go func () {
			for !finished {
				time.Sleep(time.Second)
				fmt.Printf("Sent %v, ", points_sent)
				atomic.StoreUint32(&points_sent, 0)
				fmt.Printf("Received %v\n", points_received)
				atomic.StoreUint32(&points_received, 0)
				points_received = 0
			}
		}()
	}

	var response ConnectionID
	for k := 0; k < NUM_STREAMS; k++ {
		response = <-sig
		serverIndex = response.serverIndex
		connIndex = response.connectionIndex
		usingConn[serverIndex][connIndex]--
		if usingConn[serverIndex][connIndex] == 0 {
			connections[serverIndex][connIndex].Close()
			fmt.Printf("Closed connection %v to server %v\n", connIndex, dbAddrs[serverIndex])
		}
	}
	
	var deltaT int64 = time.Now().UnixNano() - startTime
	
	// I used to close unused connections here, but now I don't bother
	
	finished = true
	
	if !DELETE_POINTS {
		fmt.Printf("Sent %v, Received %v\n", points_sent, points_received)
	}
	if VERIFY_RESPONSES {
		fmt.Printf("%v points are verified to be correct\n", points_verified);
		if verification_test_pass {
			fmt.Println("All points were verified to be correct. Test PASSes.")
		} else {
			fmt.Println("Some points were found to be incorrect. Test FAILs.")
			os.Exit(1) // terminate with a non-zero exit code
		}
	} else {
		fmt.Println("Finished")
	}
	
	var numResPoints uint64 = uint64(TOTAL_RECORDS) * uint64(NUM_STREAMS)
	fmt.Printf("Total time: %d nanoseconds for %d points\n", deltaT, numResPoints)
	var average uint64 = 0
	if numResPoints != 0 {
		average = uint64(deltaT) / numResPoints
	}
	fmt.Printf("Average: %d nanoseconds per point (floored to integer value)\n", average)
	fmt.Println(deltaT)
	
	if GET_MESSAGE_TIMES {
		file, err := os.Create("stats.json")
		if err != nil {
			fmt.Println("Could not write stats to file")
			os.Exit(1)
		}
		writeSafe(file, "{\n")
		for q := range transactionHistories {
			writeSafe(file, fmt.Sprintf("\"%v\": [\n", uuid.UUID(uuids[q]).String()))
			terminator := ","
			for r := range transactionHistories[q] {
			    if (r == len(transactionHistories[q]) - 1) {
			        terminator = ""
			    }
				writeSafe(file, fmt.Sprintf("[%v,%v]%s\n", transactionHistories[q][r].sendTime, transactionHistories[q][r].respTime, terminator))
			}
			if q == len(transactionHistories) - 1 {
				writeSafe(file, "]\n")
			} else {
				writeSafe(file, "],\n")
			}
		}
		writeSafe(file, "}\n")
	}
}

func writeSafe(file *os.File, str string) {
	written, err := io.WriteString(file, str)
	if written != len(str) || err != nil {
		fmt.Println("Could not write to file")
		os.Exit(1)
	}
}
