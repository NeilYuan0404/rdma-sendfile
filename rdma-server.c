/*
 * 接收端：listen 后按块 RECV，写入 output.mp4，再回 1 字节 ACK。
 * 先写盘、再 post_recv、再 ACK，保证客户端下一包到达时 recv WQE 已挂好。
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "rdma.h"

static int disconnected = 0;

int image_fd = -1;
static size_t bytes_written = 0;

static int write_file(const char *filename, char *data, int length) {
    if (image_fd < 0) {
        image_fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0666);
        if (image_fd < 0) {
            printf("open failed : %s\n", filename);
            exit(1);
        }
    }

    /* write() 可能短写，必须循环直到 length 字节都落盘。 */
    int off = 0;
    while (off < length) {
        ssize_t n = write(image_fd, data + off, length - off);
        if (n < 0) {
            perror("write");
            return -1;
        }
        off += n;
    }
    bytes_written += (size_t)length;
    return length;
}

static void on_completion_server(struct ibv_wc *wc) {

    if (wc->status != IBV_WC_SUCCESS) {
        /* destroy_qp 会把未完成的 recv flush 掉，断连时是正常现象。 */
        if (disconnected || wc->status == IBV_WC_WR_FLUSH_ERR)
            return;
        fprintf(stderr, "Work completion error: %s (opcode=%d byte_len=%u)\n",
                ibv_wc_status_str(wc->status), wc->opcode, wc->byte_len);
        return;
    }

    conn_manger_t *conn_manger = (conn_manger_t *)wc->wr_id;
    if (wc->opcode == IBV_WC_RECV) {
        if (wc->wc_flags & IBV_WC_WITH_IMM) {
            /* 预留：带 immediate 的结束标记。当前客户端用断连当 EOF，不会走到这里。 */
            if (image_fd >= 0) {
                fsync(image_fd);
                close(image_fd);
                image_fd = -1;
            }
            printf("Transfer complete, wrote %zu bytes\n", bytes_written);
            fflush(stdout);
            return;
        }
        /* 按本块实际到达长度写，不是固定 BUFFER_SIZE。 */
        write_file("output.mp4", conn_manger->recv_buffer,
                   wc->byte_len > BUFFER_SIZE ? BUFFER_SIZE : (int)wc->byte_len);
        /* 先挂下一 recv 再 ACK，避免客户端马上发下一块时 RNR。 */
        post_recv(conn_manger);
        conn_manger->send_buffer[0] = 1;
        post_send_nowait(conn_manger, 1);
    } else if (wc->opcode == IBV_WC_SEND) {
        /* ACK 的 SEND 完成，无需再处理 */
    }
}

static void on_connection_request(struct rdma_cm_id *cm_id) {
    initialize_connection(cm_id, on_completion_server);

    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));
    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;
    conn_param.retry_count = 7;
    conn_param.rnr_retry_count = 7;  /* 配合 ACK 乒乓；siw 不能单靠这个扛 RNR */

    if (rdma_accept(cm_id, &conn_param)) {
        perror("rdma_accept");
        exit(-1);
    }
}

static void on_connect_established(struct rdma_cm_id *cm_id) {
    printf("Connection established with %s\n", get_inet_addr_str(cm_id));
    fflush(stdout);
    /* QP 进入 RTS 后再 post_recv。siw 在 RTS 之前挂的 recv 可能被 flush。 */
    post_recv((conn_manger_t *)cm_id->context);
}

int main(int argc, char *argv[]) {

    if (argc != 3) {
        printf("Usage: %s <server_ip> <server_port>\n", argv[0]);
        return -1;
    }

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

    if (rdma_bind_addr(cm_id, (struct sockaddr *)&server_addr)) {
        perror("rdma_bind_addr");
        return -1;
    }

    if (rdma_listen(cm_id, 10)) {
        perror("rdma_listen");
        return -1;
    }

    printf("Listening on %s:%s\n", argv[1], argv[2]);
    fflush(stdout);

    struct rdma_cm_event *event;
    while (rdma_get_cm_event(eventchannel, &event) == 0) {
        enum rdma_cm_event_type ev = event->event;
        struct rdma_cm_id *id = event->id;
        rdma_ack_cm_event(event);

        if (ev == RDMA_CM_EVENT_CONNECT_REQUEST) {
            on_connection_request(id);
        } else if (ev == RDMA_CM_EVENT_ESTABLISHED) {
            on_connect_established(id);
        } else if (ev == RDMA_CM_EVENT_DISCONNECTED) {
            disconnected = 1;
            /* 客户端 disconnect 即 EOF，把已写入的文件刷盘。 */
            if (image_fd >= 0) {
                fsync(image_fd);
                close(image_fd);
                image_fd = -1;
            }
            destory_connection(id);
            printf("Client disconnected, wrote %zu bytes to output.mp4\n", bytes_written);
            fflush(stdout);
        } else {
            printf("Unhandled event: %s\n", rdma_event_str(ev));
        }
    }

    return 0;
}
