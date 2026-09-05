/*
 * server：监听并发送文件。控制面线性 5 步：
 * post_recv → 等 READY → 发 SIZE → 等 RKEY → WRITE → 发 DONE → 等 ACK。
 * 数据面 RDMA WRITE 泵（窗口 WR_WINDOW，块 CHUNK_SIZE）。
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
static const char *g_infile;

static int read_all(int fd, char *buf, size_t length) {
    size_t off = 0;
    while (off < length) {
        ssize_t n = read(fd, buf + off, length - off);
        if (n <= 0) {
            perror("read");
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

static void send_file(struct rdma_cm_id *cm_id) {
    printf("Connection established with %s\n", get_inet_addr_str(cm_id));
    fflush(stdout);

    conn_manger_t *conn = (conn_manger_t *)cm_id->context;

    if (post_recv(conn) != 0) {
        rdma_disconnect(cm_id);
        return;
    }

    int fd = open(g_infile, O_RDONLY);
    if (fd < 0) {
        perror(g_infile);
        rdma_disconnect(cm_id);
        return;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        perror("fstat");
        close(fd);
        rdma_disconnect(cm_id);
        return;
    }
    size_t file_size = (size_t)st.st_size;
    if (file_size == 0) {
        fprintf(stderr, "empty file\n");
        close(fd);
        rdma_disconnect(cm_id);
        return;
    }

    if (alloc_src_mr(conn, file_size) != 0 ||
        read_all(fd, conn->src_buf, file_size) != 0) {
        close(fd);
        rdma_disconnect(cm_id);
        return;
    }
    close(fd);

    printf("Sending %s (%zu bytes) via RDMA WRITE chunk=%d window=%d\n",
           g_infile, file_size, CHUNK_SIZE, WR_WINDOW);
    fflush(stdout);

    if (expect_ctrl(conn, CTRL_READY, NULL) != 0) {
        rdma_disconnect(cm_id);
        return;
    }

    if (post_recv(conn) != 0) {
        rdma_disconnect(cm_id);
        return;
    }
    fill_ctrl(conn->send_buffer, CTRL_SIZE, 0, 0, file_size);
    if (post_send(conn, sizeof(ctrl_msg_t)) != 0) {
        rdma_disconnect(cm_id);
        return;
    }

    ctrl_msg_t rkey_msg;
    if (expect_ctrl(conn, CTRL_RKEY, &rkey_msg) != 0) {
        rdma_disconnect(cm_id);
        return;
    }
    conn->remote_addr = rkey_msg.addr;
    conn->remote_rkey = rkey_msg.rkey;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    if (post_rdma_file(conn) != 0) {
        rdma_disconnect(cm_id);
        return;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    print_throughput("network", file_size, &t0, &t1);

    if (post_recv(conn) != 0) {
        rdma_disconnect(cm_id);
        return;
    }
    fill_ctrl(conn->send_buffer, CTRL_DONE, 0, 0, file_size);
    if (post_send(conn, sizeof(ctrl_msg_t)) != 0) {
        rdma_disconnect(cm_id);
        return;
    }
    if (expect_ctrl(conn, CTRL_ACK, NULL) != 0) {
        rdma_disconnect(cm_id);
        return;
    }

    struct timespec t2;
    clock_gettime(CLOCK_MONOTONIC, &t2);
    print_throughput("server", file_size, &t0, &t2);

    printf("Transfer finished, disconnecting\n");
    rdma_disconnect(cm_id);
}

static void *server_send_thread(void *arg) {
    send_file((struct rdma_cm_id *)arg);
    return NULL;
}

static void on_connection_request(struct rdma_cm_id *cm_id) {
    initialize_connection(cm_id);

    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));
    conn_param.responder_resources = 128;
    conn_param.initiator_depth = 128;
    conn_param.retry_count = 7;
    conn_param.rnr_retry_count = 7;

    if (rdma_accept(cm_id, &conn_param)) {
        perror("rdma_accept");
        exit(-1);
    }
}

int main(int argc, char *argv[]) {

    if (argc != 4) {
        printf("Usage: %s <bind_ip> <port> <infile>\n", argv[0]);
        return -1;
    }
    g_infile = argv[3];

    struct rdma_event_channel *eventchannel = rdma_create_event_channel();
    struct rdma_cm_id *cm_id;

    if (rdma_create_id(eventchannel, &cm_id, NULL, RDMA_PS_TCP)) {
        perror("rdma_create_id");
        return -1;
    }

    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons(atoi(argv[2]));
    bind_addr.sin_addr.s_addr = inet_addr(argv[1]);

    if (rdma_bind_addr(cm_id, (struct sockaddr *)&bind_addr)) {
        perror("rdma_bind_addr");
        return -1;
    }

    if (rdma_listen(cm_id, 10)) {
        perror("rdma_listen");
        return -1;
    }

    printf("Listening on %s:%s, sending %s\n", argv[1], argv[2], g_infile);
    fflush(stdout);

    struct rdma_cm_event *event;
    while (rdma_get_cm_event(eventchannel, &event) == 0) {
        enum rdma_cm_event_type ev = event->event;
        struct rdma_cm_id *id = event->id;
        rdma_ack_cm_event(event);

        if (ev == RDMA_CM_EVENT_CONNECT_REQUEST) {
            on_connection_request(id);
        } else if (ev == RDMA_CM_EVENT_ESTABLISHED) {
            pthread_t th;
            pthread_create(&th, NULL, server_send_thread, id);
            pthread_detach(th);
        } else if (ev == RDMA_CM_EVENT_DISCONNECTED) {
            disconnected = 1;
            destory_connection(id);
            printf("Client disconnected.\n");
            fflush(stdout);
        } else {
            printf("Unhandled event: %s\n", rdma_event_str(ev));
        }
    }

    return 0;
}
