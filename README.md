# Rdma

SEND/RECV file transfer over `rdma_cm` + `ibverbs` (Soft-iWARP `siw` or a real RNIC).

## setup
```
$ sudo modprobe siw
$ sudo rdma link add siw0 type siw netdev eth1
$ ibv_devices
```

Bind to the IP of the netdev that `siw` is attached to, not `127.0.0.1`.

## compile
```
$ gcc -o rdma-client rdma-client.c -libverbs -lrdmacm -lpthread
$ gcc -o rdma-server rdma-server.c -libverbs -lrdmacm -lpthread
```

## run
```
# server
$ ./rdma-server 192.168.8.146 2000 output.mp4

# client
$ ./rdma-client 192.168.8.146 2000 meeting_01.mp4
```

Client sends the file with `IBV_WR_SEND`. Server writes the path given on the command line. Each chunk is ACKed before the next send so Soft-iWARP does not drop the connection.

Both sides print elapsed time and throughput (MiB/s) for the payload, excluding connection setup.

## TCP sendfile 对比
用 Linux `os.sendfile` 走普通 TCP（零拷贝发送，接收端仍是 `recv`+`write`）。端口不要和 RDMA 冲突。

```
# receiver
$ python3 sendfile_bench.py recv 192.168.8.146 2001 output.sendfile

# sender
$ python3 sendfile_bench.py send 192.168.8.146 2001 meeting_01.mp4
```

吞吐打印格式与 RDMA 相同（bytes / s / MiB/s / Mbit/s），计时同样不含建连。
