# Rdma

约定：**server 发送，client 接收。**

控制面双边 SEND/RECV（`ready` / `size` / `rkey` / `done` / `ack`），两端先 `post_recv` 再 SEND，用 WC 推进。数据面 RDMA WRITE。server 先把文件读进源 MR，再 WRITE；client 热路径不写盘，`done` 之后再一次性落盘。

## setup
```
$ sudo modprobe siw
$ sudo rdma link add siw0 type siw netdev eth1
$ ibv_devices
```

Bind to the IP of the netdev that `siw` is attached to, not `127.0.0.1`。

## compile
```
$ gcc -o rdma-server rdma-server.c -libverbs -lrdmacm -lpthread
$ gcc -o rdma-client rdma-client.c -libverbs -lrdmacm -lpthread
```

## run
先起发送端，再连接收端：

```
# server：监听，发送 infile
$ ./rdma-server 192.168.8.146 2000 meeting_01.mp4

# client：连接，写入 outfile
$ ./rdma-client 192.168.8.146 2000 output.mp4
```

打印两行吞吐（均不含建连）：

- `network`：已注册内存上的 WRITE（不含预读）
- `server` / `client`：含等 ACK 或最后一次落盘

公平对比看 **`client` vs `tcp-client`**（都含落盘）。同一文件可用 `cmp` 校验。

## TCP sendfile 对比
同样是 server 发送、client 接收。端口不要和 RDMA 冲突。

```
# server
$ python3 sendfile_bench.py server 192.168.8.146 2001 meeting_01.mp4

# client
$ python3 sendfile_bench.py client 192.168.8.146 2001 output.sendfile
```
