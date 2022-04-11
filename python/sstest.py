#!/usr/bin/env python3
# MIT License Copyright (C) 2022 Hiroshi Shimamoto
# vim: set sw=4 sts=4:

import sys
from socket import socket, AF_INET, SOCK_STREAM
from struct import pack, unpack
from time import time

def client(req_addr, req_dir, req_length):
    # hostname:port
    addr = req_addr.split(':')
    if len(addr) != 2:
        print(f"Bad addr {req_addr}")
        return
    if req_dir != "DL" and req_dir != "UL":
        print(f"Bad direction {req_dir}")
        return
    try:
        req_length = int(req_length)
        s = socket(AF_INET, SOCK_STREAM)
    except Exception as e:
        print(f"{e}")
        return
    # okay socket is ready
    try:
        s.connect((addr[0], int(addr[1])))
        # create REQ packet
        req = f"REQTCP{req_dir}".encode() + pack("<Q", req_length)
        s.sendall(req)
        tm_start = time()
        s.sendall(b"ST")
        rest = req_length
        print(f"DATA {req_dir} START")
        if req_dir == "DL":
            #print("downloading")
            while rest > 0:
                rlen = 65536
                if rest < rlen:
                    rlen = rest
                msg = s.recv(rlen)
                if len(msg) <= 0:
                    raise Exception("unexpected close")
                rest -= len(msg)
        elif req_dir == "UL":
            #print("uploading")
            msg = b"\0" * 65536
            while rest > 0:
                slen = 65536
                if rest < slen:
                    slen = rest
                sent = s.send(msg[0:slen])
                if sent <= 0:
                    raise Exception("unexpected stop")
                rest -= sent
        print(f"DATA {req_dir} END")
        duration = time() - tm_start
        usec = int(duration * 1000 * 1000)
        #print(f"DONE in {usec} usec")
        en = b"EN" + pack("<Q", usec)
        s.sendall(en)
        resp = s.recv(16)
        # get usec from RESPONSE
        vals = unpack("<Q", resp[8:16])
        usec = vals[0]
        bpusec = req_length / usec
        bpsec = bpusec * 1000 * 1000
        print(f"throughput {bpsec / 1024 / 1024} MiB/sec")
    except Exception as e:
        print(f"{e}")
    s.close()

def main():
    if len(sys.argv) <= 4:
        print(f"{sys.argv[0]} client <server:port> <DL|UL> <bytes>")
        return
    if sys.argv[1] == "client":
        client(sys.argv[2], sys.argv[3], sys.argv[4])

if __name__ == "__main__":
    main()
