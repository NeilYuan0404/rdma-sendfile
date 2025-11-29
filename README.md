# Rdma

## run
```
$ sudo modprobe siw
$ sudo rdma link add siw0 type siw netdev eth1
$ ibv_devices
```

## compile
```
$ gcc -o rdma-client rdma-client.c -libverbs -lrdmacm
$ gcc -o rdma-server rdma-server.c -libverbs -lrdmacm

```

