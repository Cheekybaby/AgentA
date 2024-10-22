package main

import (
	."encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

var TAG_BUFFER = byte(0)
const (
	DescribeTopicPartitions = 75
)
// Types of requests
type RequestHeader struct {
	RequestAPIKey	    int16    `json:"request_api_key"`
	RequestAPIVersion	int16    `json:"request_api_version"`
	CorrelationId		int32    `json:"correlation_id"`
	ClientIdLength		int16	 `json:"client_id_length"`
	ClientId 			string 	 `json:"client_id"`
	Data 				[]byte   `json:"data"`
}

type DescribeTopicPartitionsRequests struct {
	RequestHeader
	topics []Topic
}

type Topic struct {
	name string
	responsePartitionLimit int32
	cursor int8
}

func main() {
	// Starting a Server
	startServer()
}

func startServer() {
	// Setting up a TCP Listener
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to the port 9092")
		os.Exit(1)
	}

	defer func(listener net.Listener){
		err := listener.Close()
		if err != nil {
			fmt.Println("Failed to Close Listener")
		}
	}(listener)

	// Accepting mulitple incoming connections
	for {
		conn, err := listener.Accept();
		if err != nil {
			fmt.Println("Error in accepting connection: ", err.Error())
			continue
		}
		go func() {
			if err := handleConnection(conn);
			err != nil {
				fmt.Println("Error handling connections: ", err.Error())
			}

			defer func(conn net.Conn) {
				if err := conn.Close();
				err != nil {
					fmt.Printf("Failed to close the connections: %s\n", err.Error())
				}
			} (conn)
		}()
	}
}

func handleConnection (conn net.Conn) error {
	for {
		req, err := NewReqFromConn(conn)
		if err != nil {
			return err
		}

		if err := handleRequest(conn, req);
		err != nil {
			return err
		}
	}
}

func NewReqFromConn (conn net.Conn) (RequestHeader, error) {
	var req RequestHeader
	
	var size uint32
	if err := Read(conn, BigEndian, &size);
	err != nil {
		return req, err
	}

	buff := make([]byte, size)
	if _, err := io.ReadFull(conn, buff);
	err != nil {
		return req, err
	}

	req.RequestAPIKey, buff = readInt16(buff)
	req.RequestAPIVersion, buff = readInt16(buff)
	req.CorrelationId, buff = readInt32(buff)
	req.ClientIdLength, buff = readInt16(buff)
	req.ClientId, buff = readString(buff, int32(req.ClientIdLength))
	_, buff = readTagBuffer(buff)
	req.Data = buff

	return req, nil
}

func handleRequest(conn net.Conn, req RequestHeader) error {
	var message []byte
	if req.RequestAPIKey == DescribeTopicPartitions {
		dtpRequest := readDescribeTopicPartitionsRequest(req)
		message = handleDescribeTopicPartitions(dtpRequest)
	} else {
		/*
			Response structure
			- .MessageLength (4 bytes) (length of ResponseHeader + ResponseBody)
			- .ResponseHeader
			  - .correlation_id (4 bytes)
			- .ResponseBody
			  - .error_code (2 bytes)
			  - .num_api_keys (UNSIGNED_VARINT) (A null array is represented with a length of "0". Therefore, num_api_keys = N + 1)
			  - .ApiKeys (COMPACT_ARRAY)
			    - .api_key (2 bytes)
			    - .min_version (2 bytes)
			    - .max_version (2 bytes)
			    - .TAG_BUFFER (beginning with num_tag_fields of type UNSIGNED_VARINT, set to 0 in this case means having no further data for tag_fields)
			  - .throttle_time_ms (4 bytes)
		*/
		message = appendCorrelationId(message, uint32(req.CorrelationId))
		var errorCode uint16
		if req.RequestAPIVersion < 0 || req.RequestAPIVersion > 4 {
			errorCode = 35
		}
		message = appendErrorCode(message, errorCode)

		if errorCode == 0 {
			message = appendApiKeyInt8Len(message, 2)
			message = appendApiVersionsKey(message)
			message = appendDescribeTopicPartitionsKey(message)
			message = appendThrottleTime(message, 0)
			message = append(message, TAG_BUFFER)
		}	
	}
	if err := sendMessage(conn, message);
	err != nil {
		return fmt.Errorf("sending message failed: %w", err)
	}

	return nil
}

func readDescribeTopicPartitionsRequest(req RequestHeader) DescribeTopicPartitionsRequests {
	topicReq := DescribeTopicPartitionsRequests {
		RequestHeader: req,
		topics: []Topic{},
	}

	data:= req.Data

	nTopics, data := readUVarint(data)

	for i:=uint64(0); i<nTopics; i++ {
		topic := Topic{}
		topicNameLength, data := readUVarint(data)
		topic.name, data = readString(data, int32(topicNameLength))
		_, data = readTagBuffer(data)
		topic.responsePartitionLimit, data = readInt32(data)
		topic.cursor, data = readCursor(data)
		_, data = readTagBuffer(data)
		topicReq.topics = append(topicReq.topics, topic)
	}
	return topicReq
}

func handleDescribeTopicPartitions(req DescribeTopicPartitionsRequests) []byte {
	var message []byte

	message = appendCorrelationId(message, uint32(req.CorrelationId))
	message = append(message, TAG_BUFFER)
	message = appendThrottleTime(message, 0)
	message = appendUvarint(message, 1)
	message = appendErrorCode(message, 3) // Unknown Topic
	message = appendTopicName(message, req.topics[0].name)
	message = append(message, make([]byte, 16)...) // Topic ID: unassigned or null UUID
	message = append(message, byte(0)) // Is Internal (boolean) : 0 is false
	message = appendUvarint(message, 0) // Partitions Array Length
	message = append(message, make([]byte, 4)...) // Topic Authorized Operations
	message = append(message, TAG_BUFFER)
	message = append(message, byte(0xff)) // Next Cursor (pagination): 0xff (indicating a null value)
	message = append(message, TAG_BUFFER)

	return message
}

// Append Functions
func appendTopicName(message []byte, name string) []byte {
	bytes:= []byte(name)
	message = appendUvarint(message, int64(len(bytes)))
	message = append(message, bytes...)
	return message
}

func sendMessage(conn net.Conn, body []byte) error {
	bodyLen := make([]byte, 4)
	BigEndian.PutUint32(bodyLen, uint32(len(body)))
	if _, err:= conn.Write(bodyLen);
	err != nil{
		return err
	}
	if _, err:= conn.Write(body);
	err != nil {
		return err
	}
	return nil
}

func appendApiVersionsKey(message []byte) []byte {
	message = BigEndian.AppendUint16(message, 18) // Api Key
	message = BigEndian.AppendUint16(message, 0) // Min Version
	message = BigEndian.AppendUint16(message, 4) // Max Version
	message = append(message, TAG_BUFFER) // Tag Buffer
	return message
}

func appendDescribeTopicPartitionsKey(message []byte) []byte {
	message = BigEndian.AppendUint16(message, 75) // Topic Partitions Key
	message = BigEndian.AppendUint16(message, 0) // Min Version
	message = BigEndian.AppendUint16(message, 0) // Max Version
	message = append(message, TAG_BUFFER) // TAG BUFFER
	return message
}

func appendCorrelationId(message []byte, val uint32) []byte {
	return BigEndian.AppendUint32(message, val)
}

func appendErrorCode(message []byte, val uint16) []byte {
	return BigEndian.AppendUint16(message, val)
}

func appendApiKeyInt8Len(message []byte, val int8) []byte {
	return append(message, byte(val+1))
}

func appendApiKey(message []byte, val uint16) []byte {
	return BigEndian.AppendUint16(message, val)
}

func appendThrottleTime(message []byte, val uint32) []byte {
	return BigEndian.AppendUint32(message, val)
}

func appendUvarint(message []byte, val int64) []byte {
	return AppendUvarint(message, uint64(val+1))
}

// Read Functions
func readUVarint(source []byte) (uint64, []byte){
	value, n := Uvarint(source)
	return value-1, source[n:]
}
func readInt16(source []byte) (int16, []byte){
	return int16(BigEndian.Uint16(source)), source[2:]
}
func readInt32(source []byte) (int32, []byte){
	return int32(BigEndian.Uint32(source)), source[4:]
}
func readString(source []byte, length int32) (string, []byte){
	return string(source[:length]), source[length:] 
}
func readCursor(source []byte) (int8, []byte) {
	return int8(source[0]), source[1:]
}
func readTagBuffer(source []byte) ([]byte, []byte) {
	return source[:1], source[1:]
}