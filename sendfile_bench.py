#!/usr/bin/env python3
"""TCP sendfile 对照：与 RDMA 相同约定——server 发送，client 接收。

  python3 sendfile_bench.py server 192.168.8.146 2001 in.bin
  python3 sendfile_bench.py client 192.168.8.146 2001 out.bin
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


def cmd_server(ip: str, port: int, infile: str) -> None:
    size = os.path.getsize(infile)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((ip, port))
    srv.listen(1)
    print(f"Listening on {ip}:{port}, sending {infile} ({size} bytes)", flush=True)

    conn, addr = srv.accept()
    print(f"Connection from {addr[0]}:{addr[1]}", flush=True)
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    offset = 0
    t0 = time.monotonic()
    with open(infile, "rb") as src, conn:
        fd = src.fileno()
        while offset < size:
            sent = os.sendfile(conn.fileno(), fd, offset, size - offset)
            if sent <= 0:
                raise RuntimeError("sendfile returned %s" % sent)
            offset += sent

    print_throughput("tcp-server", offset, time.monotonic() - t0)
    print("Transfer finished", flush=True)
    srv.close()


def cmd_client(ip: str, port: int, outfile: str) -> None:
    print(f"Connecting to {ip}:{port}, writing {outfile}", flush=True)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))

    started = False
    t0 = 0.0
    chunks: list[bytes] = []
    written = 0
    with sock:
        while True:
            data = sock.recv(RECV_CHUNK)
            if not data:
                break
            if not started:
                t0 = time.monotonic()
                started = True
            chunks.append(data)
            written += len(data)

    t_net = time.monotonic()
    if started:
        print_throughput("tcp-network", written, t_net - t0)

    payload = b"".join(chunks)
    with open(outfile, "wb") as out:
        out.write(payload)
        out.flush()
        os.fsync(out.fileno())

    if started:
        print_throughput("tcp-client", written, time.monotonic() - t0)
    print(f"Peer closed, wrote {written} bytes to {outfile}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="TCP sendfile benchmark vs RDMA")
    sub = parser.add_subparsers(dest="cmd", required=True)

    server_p = sub.add_parser("server", help="listen and os.sendfile a file")
    server_p.add_argument("ip")
    server_p.add_argument("port", type=int)
    server_p.add_argument("infile")

    client_p = sub.add_parser("client", help="connect, buffer in memory, then write")
    client_p.add_argument("ip")
    client_p.add_argument("port", type=int)
    client_p.add_argument("outfile")

    args = parser.parse_args()
    if args.cmd == "server":
        cmd_server(args.ip, args.port, args.infile)
    else:
        cmd_client(args.ip, args.port, args.outfile)


if __name__ == "__main__":
    main()
