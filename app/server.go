package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

var TAG_BUFFER = []byte{0x00}

// Types of requests
type RequestHeader struct {
	RequestAPIKey	    int16    `json:"request_api_key"`
	RequestAPIVersion	int16    `json:"request_api_version"`
	CorrelationId		int32    `json:"correlation_id"`
	ClientId   			string	 `json:"client_id"`
	TaggedFields		string   `json:"tagged_fields"`
}

func main() {
	// Setting up a TCP listener
	startServer()
}

func startServer() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to the port 9092")
		os.Exit(1)
	}

	// Accepting incoming connections
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close() // Closes the connection after the function finishes running
	for {
		req, err := NewReqFromConn(conn)
		if err != nil {
			log.Fatal(err)
		}

		res := bytes.NewBuffer([]byte{})
		binary.Write(res, binary.BigEndian, uint32(req.CorrelationId))
		var errorCode = uint16(0)
		if int(req.RequestAPIVersion) < 0 || int(req.RequestAPIVersion) > 4{
			errorCode = 35
		}

		binary.Write(res, binary.BigEndian, uint16(errorCode))
		binary.Write(res, binary.BigEndian, byte(2))
		binary.Write(res, binary.BigEndian, uint16(18))
		binary.Write(res, binary.BigEndian, uint16(3))
		binary.Write(res, binary.BigEndian, uint16(4))

		res.Write(TAG_BUFFER)
		binary.Write(res, binary.BigEndian, uint32(0))
		res.Write(TAG_BUFFER)

		binary.Write(conn, binary.BigEndian, uint32(res.Len()))

		io.Copy(conn, res)
	}
}

func NewReqFromConn (conn net.Conn) (RequestHeader, error) {
	var size uint32
	err := binary.Read(conn, binary.BigEndian, &size)
	if err != nil {
		return RequestHeader{}, err
	}

	req := RequestHeader{}
	binary.Read(conn, binary.BigEndian, &req.RequestAPIKey)
	binary.Read(conn, binary.BigEndian, &req.RequestAPIVersion)
	binary.Read(conn, binary.BigEndian, &req.CorrelationId)

	conn.SetReadDeadline(time.Now().Add(5*time.Second))
	conn.Read(make([]byte, 1024))

	return req, nil
}

func (r *RequestHeader) Reader() (uint32, io.Reader) {
	var buff bytes.Buffer
	binary.Write(&buff, binary.BigEndian, &r.RequestAPIKey)
	binary.Write(&buff, binary.BigEndian, &r.RequestAPIVersion)
	binary.Write(&buff, binary.BigEndian, &r.CorrelationId)

	return uint32(buff.Len()), &buff
}