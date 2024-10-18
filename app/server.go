package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

var TAG_BUFFER = byte(0)

// Types of requests
type RequestHeader struct {
	RequestAPIKey	    int16    `json:"request_api_key"`
	RequestAPIVersion	int16    `json:"request_api_version"`
	CorrelationId		int32    `json:"correlation_id"`
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
	if err := binary.Read(conn, binary.BigEndian, &size);
	err != nil {
		return req, err
	}

	buff := make([]byte, size)
	if _, err := io.ReadFull(conn, buff);
	err != nil {
		return req, err
	}

	req.RequestAPIKey = int16(binary.BigEndian.Uint16(buff[0:2]))
	req.RequestAPIVersion = int16(binary.BigEndian.Uint16(buff[2:4]))
	req.CorrelationId = int32(binary.BigEndian.Uint32(buff[4:8]))

	return req, nil
}

func handleRequest(conn net.Conn, req RequestHeader) error {
	var message []byte

	// Correlation Id
	message = binary.BigEndian.AppendUint32(message, uint32(req.CorrelationId)) // Correlation Id
	
	// Error Code 
	if req.RequestAPIVersion < 0 || req.RequestAPIVersion > 4 {
		message = binary.BigEndian.AppendUint16(message, 35) // Error in API version
	} else {
		message = binary.BigEndian.AppendUint16(message, 0) // No Error in API version

		// Num of API Keys
		message = append(message, byte(3))
		// Api Versions Key
		message = appendApiVersionsKey(message)
		// Describe Topic Partitions Key
		message = appendDescribeTopicPartitionsKey(message)
		// Throttle Time
		message = binary.BigEndian.AppendUint32(message, 0)
		// Tag Buffer
		message = append(message, TAG_BUFFER)
	}
	
	// Message Length
	messageLen := make([]byte, 4)
	binary.BigEndian.PutUint32(messageLen, uint32(len(message)))

	if _, err := conn.Write(messageLen);
	err != nil {
		fmt.Println("Error Sending Data: ", err.Error())
	}
	if _, err := conn.Write(message);
	err != nil {
		fmt.Println("Error sending data", err.Error())
	}
	return nil
}

func appendApiVersionsKey(message []byte) []byte {
	message = binary.BigEndian.AppendUint16(message, 18) // Api Key
	message = binary.BigEndian.AppendUint16(message, 0) // Min Version
	message = binary.BigEndian.AppendUint16(message, 4) // Max Version
	message = append(message, TAG_BUFFER) // Tag Buffer
	return message
}

func appendDescribeTopicPartitionsKey(message []byte) []byte {
	message = binary.BigEndian.AppendUint16(message, 75) // Topic Partitions Key
	message = binary.BigEndian.AppendUint16(message, 0) // Min Version
	message = binary.BigEndian.AppendUint16(message, 0) // Max Version
	message = append(message, TAG_BUFFER) // TAG BUFFER
	return message
}