/*
 * client：连接并接收文件。控制面线性 5 步：
 * post_recv → SEND READY → 等 SIZE → 回 RKEY → 等 DONE → 回 ACK。
 * payload 由 server RDMA WRITE 进来；done 后再一次性写入 outfile。
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include <pthread.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "rdma.h"

static int disconnected = 0;
static const char *g_outfile;
static size_t bytes_written = 0;

static int write_all(int fd, const char *data, size_t length) {
    size_t off = 0;
    while (off < length) {
        ssize_t n = write(fd, data + off, length - off);
        if (n < 0) {
            perror("write");
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

static void recv_file(struct rdma_cm_id *cm_id) {
    printf("Connection established with %s\n", get_inet_addr_str(cm_id));
    fflush(stdout);

    conn_manger_t *conn = (conn_manger_t *)cm_id->context;

    if (post_recv(conn) != 0) {
        rdma_disconnect(cm_id);
        return;
    }
    fill_ctrl(conn->send_buffer, CTRL_READY, 0, 0, 0);
    if (post_send(conn, sizeof(ctrl_msg_t)) != 0) {
        rdma_disconnect(cm_id);
        return;
    }

    ctrl_msg_t size_msg;
    if (expect_ctrl(conn, CTRL_SIZE, &size_msg) != 0) {
        rdma_disconnect(cm_id);
        return;
    }
    size_t size = (size_t)size_msg.size;
    if (size == 0 || alloc_dest_mr(conn, size) != 0) {
        fprintf(stderr, "alloc dest failed size=%zu\n", size);
        rdma_disconnect(cm_id);
        return;
    }

    struct timespec net_t0, net_t1;
    clock_gettime(CLOCK_MONOTONIC, &net_t0);

    if (post_recv(conn) != 0) {
        rdma_disconnect(cm_id);
        return;
    }
    fill_ctrl(conn->send_buffer, CTRL_RKEY, conn->dest_mr->rkey,
              (uint64_t)(uintptr_t)conn->dest_buf, size);
    if (post_send(conn, sizeof(ctrl_msg_t)) != 0) {
        rdma_disconnect(cm_id);
        return;
    }
    printf("Ready for RDMA WRITE, dest %zu bytes rkey=%u\n",
           size, conn->dest_mr->rkey);
    fflush(stdout);

    if (expect_ctrl(conn, CTRL_DONE, NULL) != 0) {
        rdma_disconnect(cm_id);
        return;
    }

    clock_gettime(CLOCK_MONOTONIC, &net_t1);
    print_throughput("network", size, &net_t0, &net_t1);

    int fd = open(g_outfile, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        perror(g_outfile);
        rdma_disconnect(cm_id);
        return;
    }
    if (write_all(fd, conn->dest_buf, size) == 0) {
        fsync(fd);
        bytes_written = size;
    }
    close(fd);

    struct timespec t2;
    clock_gettime(CLOCK_MONOTONIC, &t2);
    print_throughput("client", bytes_written, &net_t0, &t2);

    fill_ctrl(conn->send_buffer, CTRL_ACK, 0, 0, bytes_written);
    if (post_send(conn, sizeof(ctrl_msg_t)) != 0) {
        rdma_disconnect(cm_id);
        return;
    }
}

static void *client_recv_thread(void *arg) {
    recv_file((struct rdma_cm_id *)arg);
    return NULL;
}

int main(int argc, char *argv[]) {

    if (argc != 4) {
        printf("Usage: %s <server_ip> <server_port> <outfile>\n", argv[0]);
        return -1;
    }
    g_outfile = argv[3];

    struct rdma_event_channel *eventchannel = rdma_create_event_channel();
    struct rdma_cm_id *cm_id;
    if (rdma_create_id(eventchannel, &cm_id, NULL, RDMA_PS_TCP)) {
        perror("rdma_create_id");
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(atoi(argv[2]));
    server_addr.sin_addr.s_addr = inet_addr(argv[1]);

    if (rdma_resolve_addr(cm_id, NULL, (struct sockaddr *)&server_addr, 2000)) {
        perror("rdma_resolve_addr");
        return -1;
    }

    struct rdma_cm_event *event;
    while (rdma_get_cm_event(eventchannel, &event) == 0) {
        enum rdma_cm_event_type ev = event->event;
        struct rdma_cm_id *id = event->id;

        switch (ev) {
            case RDMA_CM_EVENT_ADDR_RESOLVED:
                rdma_ack_cm_event(event);
                if (rdma_resolve_route(cm_id, 2000)) {
                    perror("rdma_resolve_route");
                    return -1;
                }
                break;
            case RDMA_CM_EVENT_ROUTE_RESOLVED:
                rdma_ack_cm_event(event);
                initialize_connection(cm_id);
                {
                    struct rdma_conn_param conn_param;
                    memset(&conn_param, 0, sizeof(conn_param));
                    conn_param.responder_resources = 128;
                    conn_param.initiator_depth = 128;
                    conn_param.retry_count = 7;
                    conn_param.rnr_retry_count = 7;
                    if (rdma_connect(cm_id, &conn_param)) {
                        perror("rdma_connect");
                        return -1;
                    }
                }
                break;
            case RDMA_CM_EVENT_ESTABLISHED: {
                rdma_ack_cm_event(event);
                pthread_t th;
                pthread_create(&th, NULL, client_recv_thread, id);
                pthread_detach(th);
                break;
            }
            case RDMA_CM_EVENT_DISCONNECTED:
                disconnected = 1;
                rdma_ack_cm_event(event);
                destory_connection(id);
                printf("Disconnected, wrote %zu bytes to %s\n",
                       bytes_written, g_outfile);
                fflush(stdout);
                return 0;
            default:
                printf("Unhandled event: %s\n", rdma_event_str(ev));
                rdma_ack_cm_event(event);
                break;
        }
    }

    return 0;
}
