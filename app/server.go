package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"net"
)

// Types of requests
type RequestHeader struct {
	RequestAPIKey	    int16    `json:"request_api_key"`
	RequestAPIVersion	int16    `json:"request_api_version"`
	CorrelationId		int32    `json:"correlation_id"`
	TaggedFields		[]string `json:"tagged_fields"`
}

func main() {
	// Setting up a TCP listener
	l, err := net.Listen("tcp", "0.0.0.0.9092")
	if err != nil {
		fmt.Println("Failed to bind to the port")
		os.Exit(1)
	}

	// Accepting incoming connections
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	defer conn.Close() // Closes the connection after the function finishes running

	// Handling the connection and taking the data
	var buff = make([]byte, 1024)
	conn.Read(buff)

	// Checking version
	var version_err []byte
	ver := binary.BigEndian.Uint16(buff[4:8])

	switch ver {
		case 0, 1, 2, 3, 4:
			version_err = []byte{0,0}
		default:
			version_err = []byte{0,35}
	}

	// Correlation Id
	// corr := binary.BigEndian.Uint32(buff[8:12])
	
	// Response
	resp := make([]byte, 8)
	copy(resp, []byte{0,0,0,0})
	copy(resp[4:], buff[8:12]) // This adds the correlation id to the response

	resp = append(resp, version_err...)
	
	// The final response
	conn.Write(resp)
}