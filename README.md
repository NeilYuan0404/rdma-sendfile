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
Put `meeting_01.mp4` in the client working directory.

```
# server
$ ./rdma-server 192.168.8.146 2000

# client
$ ./rdma-client 192.168.8.146 2000
```

Client sends the file with `IBV_WR_SEND`. Server writes `output.mp4`. Each chunk is ACKed before the next send so Soft-iWARP does not drop the connection.

Both sides print elapsed time and throughput (MiB/s) for the payload, excluding connection setup.
