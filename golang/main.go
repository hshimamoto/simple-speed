// MIT License Copyright (C) 2022 Hiroshi Shimamoto
// vim: set sw=4 sts=4:
package main

import (
    "encoding/binary"
    "fmt"
    "log"
    "net"
    "os"
    "strconv"
    "time"
)

func readnbytes(conn net.Conn, buf []byte) error {
    nlen := len(buf)
    i := 0
    for i < nlen {
	n, err := conn.Read(buf[i:])
	if n <= 0 {
	    if err != nil {
		return err
	    }
	    return fmt.Errorf("Read Failure")
	}
	i += n
    }
    return nil
}

func senddummy(conn net.Conn, nlen int64) error {
    buf := make([]byte, 65536)
    rest := nlen
    for rest > 0 {
	wlen := int64(65536)
	if rest < wlen {
	    wlen = rest
	}
	n, err := conn.Write(buf[:wlen])
	if n <= 0 {
	    if err != nil {
		return err
	    }
	    return fmt.Errorf("Write error")
	}
	rest -= int64(n)
    }
    return nil
}

func recvdummy(conn net.Conn, nlen int64) error {
    buf := make([]byte, 65536)
    rest := nlen
    for rest > 0 {
	rlen := int64(65536)
	if rest < rlen {
	    rlen = rest
	}
	n, err := conn.Read(buf[:rlen])
	if n <= 0 {
	    if err != nil {
		return err
	    }
	    return fmt.Errorf("Read error")
	}
	rest -= int64(n)
    }
    return nil
}

func server_work(conn net.Conn) {
    defer conn.Close()

    // REQTCPDL|Length(8byte)
    request := make([]byte, 16)
    if err := readnbytes(conn, request); err != nil {
	log.Printf("Bad Request: %v", err)
	return
    }
    request_type := string(request[:6])
    request_dir := string(request[6:8])
    request_len := binary.LittleEndian.Uint64(request[8:16])
    log.Printf("%s %s %d", request_type, request_dir, request_len)
    if request_type != "REQTCP" {
	log.Printf("Unknown type %s", request_type)
	return
    }
    switch request_dir {
    case "UL", "DL":
    default:
	log.Printf("Unknown dir: %s", request_dir)
	return
    }

    // wait ST
    st := make([]byte, 2)
    if err := readnbytes(conn, st); err != nil {
	log.Printf("Start marker error: %v", err)
	return
    }
    if string(st) != "ST" {
	log.Printf("Bad Start marker: %s", string(st))
	return
    }

    log.Printf("DATA START")
    var errdummy error = nil
    if request_dir == "DL" {
	errdummy = senddummy(conn, int64(request_len))
    } else {
	errdummy = recvdummy(conn, int64(request_len))
    }
    if errdummy != nil {
	log.Printf("%s: %v", request_dir, errdummy)
	return
    }
    log.Printf("DATA END")

    // finally get end marker EN
    msg := make([]byte, 10)
    if err := readnbytes(conn, msg); err != nil {
	log.Printf("End marker error: %v", err)
	return
    }
    ts_end := binary.LittleEndian.Uint64(msg[2:])
    log.Printf("finish in %d usec", ts_end)
    resp := []byte("RESPONSETTTTTTTT")
    binary.LittleEndian.PutUint64(resp[8:], ts_end)
    conn.Write(resp)
}

func server(laddr string) {
    // listen in TCP
    l, err := net.Listen("tcp", laddr)
    if err != nil {
	log.Printf("Listen: %v", err)
	return
    }
    for {
	conn, err := l.Accept()
	if err != nil {
	    log.Printf("Accept: %v", err)
	    continue
	}
	go server_work(conn)
    }
}

func client(raddr, dir, length string) {
    if dir != "DL" && dir != "UL" {
	log.Printf("Bad direction: %s", dir)
	return
    }
    nlen, err := strconv.ParseUint(length, 10, 64)
    if err != nil {
	log.Printf("Bad Length: %v", err)
	return
    }
    if nlen > 4*1024*1024*1024 {
	log.Printf("Bad Length: too long")
	return
    }
    // dial
    conn, err := net.Dial("tcp", raddr)
    if err != nil {
	log.Printf("Dial: %v", err)
	return
    }
    // dial ok
    defer conn.Close()

    proto := "TCP"
    request := []byte(fmt.Sprintf("REQ%s%sLLLLLLLL", proto, dir))
    binary.LittleEndian.PutUint64(request[8:], uint64(nlen))
    conn.Write(request)

    tm_start := time.Now()
    // send Start marker
    conn.Write([]byte("ST"))

    log.Printf("DATA START")
    var errdummy error = nil
    if dir == "DL" {
	errdummy = recvdummy(conn, int64(nlen))
    } else {
	errdummy = senddummy(conn, int64(nlen))
    }
    if errdummy != nil {
	log.Printf("%s: %v", dir, errdummy)
	return
    }
    log.Printf("DATA END")

    // send End marker and duration
    msg := []byte("ENTTTTTTTT")
    dur := time.Since(tm_start)
    binary.LittleEndian.PutUint64(msg[2:], uint64(dur.Microseconds()))
    conn.Write(msg)

    resp := make([]byte, 16)
    if err := readnbytes(conn, resp); err != nil {
	log.Printf("Response error: %v", err)
	return
    }
    if string(resp[:8]) != "RESPONSE" {
	log.Printf("Bad response: %s", string(resp[:8]))
	return
    }
    usec := binary.LittleEndian.Uint64(resp[8:])
    bpusec := float64(nlen) / float64(usec)
    bpsec := bpusec * 1000 * 1000
    log.Printf("throughput %f MiB/sec", bpsec / 1024 / 1024)
}

func main() {
    if len(os.Args) <= 2 {
	return
    }
    cmd := os.Args[1]
    switch cmd {
    case "server":
	server(os.Args[2])
    case "client":
	if len(os.Args) <= 4 {
	    log.Printf("not enough args")
	    return
	}
	client(os.Args[2], os.Args[3], os.Args[4])
    default:
	return
    }
}
