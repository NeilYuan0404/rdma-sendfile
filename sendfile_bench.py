#!/usr/bin/env python3
"""TCP sendfile 对照：口径尽量贴近 rdma-client / rdma-server。

发送端用 os.sendfile 零拷贝把文件推进 socket；接收端 recv + write。
计时不含 listen/connect，只统计 payload。

  python3 sendfile_bench.py recv 192.168.8.146 2001 out.bin
  python3 sendfile_bench.py send 192.168.8.146 2001 in.bin
"""

from __future__ import annotations

import argparse
import os
import socket
import time

RECV_CHUNK = 64 * 1024


def print_throughput(side: str, nbytes: int, elapsed: float) -> None:
    if elapsed < 1e-9:
        elapsed = 1e-9
    mib = nbytes / (1024.0 * 1024.0)
    mib_s = mib / elapsed
    print(
        f"{side}: {nbytes} bytes in {elapsed:.4f} s, "
        f"{mib_s:.2f} MiB/s ({mib_s * 8.0:.2f} Mbit/s)",
        flush=True,
    )


def cmd_recv(ip: str, port: int, outfile: str) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((ip, port))
    srv.listen(1)
    print(f"Listening on {ip}:{port}, writing {outfile}", flush=True)

    conn, addr = srv.accept()
    print(f"Connection from {addr[0]}:{addr[1]}", flush=True)

    started = False
    t0 = 0.0
    written = 0
    with conn, open(outfile, "wb") as out:
        while True:
            data = conn.recv(RECV_CHUNK)
            if not data:
                break
            if not started:
                t0 = time.monotonic()
                started = True
            out.write(data)
            written += len(data)
        out.flush()
        os.fsync(out.fileno())

    if started:
        print_throughput("tcp-recv", written, time.monotonic() - t0)
    print(f"Peer closed, wrote {written} bytes to {outfile}", flush=True)
    srv.close()


def cmd_send(ip: str, port: int, infile: str) -> None:
    size = os.path.getsize(infile)
    print(f"Sending {infile} ({size} bytes) via sendfile", flush=True)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    offset = 0
    t0 = time.monotonic()
    with open(infile, "rb") as src, sock:
        fd = src.fileno()
        while offset < size:
            sent = os.sendfile(sock.fileno(), fd, offset, size - offset)
            if sent <= 0:
                raise RuntimeError("sendfile returned %s" % sent)
            offset += sent

    print_throughput("tcp-sendfile", offset, time.monotonic() - t0)
    print("Transfer finished", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="TCP sendfile benchmark vs RDMA")
    sub = parser.add_subparsers(dest="cmd", required=True)

    recv_p = sub.add_parser("recv", help="listen and write incoming bytes")
    recv_p.add_argument("ip")
    recv_p.add_argument("port", type=int)
    recv_p.add_argument("outfile")

    send_p = sub.add_parser("send", help="connect and os.sendfile a file")
    send_p.add_argument("ip")
    send_p.add_argument("port", type=int)
    send_p.add_argument("infile")

    args = parser.parse_args()
    if args.cmd == "recv":
        cmd_recv(args.ip, args.port, args.outfile)
    else:
        cmd_send(args.ip, args.port, args.infile)


if __name__ == "__main__":
    main()
