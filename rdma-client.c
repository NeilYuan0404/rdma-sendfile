/*
 * 发送端：读 meeting_01.mp4，按 BUFFER_SIZE 切块 IBV_WR_SEND。
 * 每块等对端 1 字节 ACK 再发下一块；传完 rdma_disconnect，用断连当 EOF。
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

#define FILENAME        "meeting_01.mp4"

static int disconnected = 0;

/* CQ 线程回调。真正叫醒业务线程的是 cq_poller 里对 send/recv_outstanding 的清零。 */
static void on_completion_client(struct ibv_wc *wc) {

    if (wc->status != IBV_WC_SUCCESS) {
        /* 拆 QP 时未完成的 WQE 会被 flush，不算传输失败。 */
        if (disconnected || wc->status == IBV_WC_WR_FLUSH_ERR)
            return;
        fprintf(stderr, "Work completion error: %s (opcode=%d)\n",
                ibv_wc_status_str(wc->status), wc->opcode);
        return;
    }

    if (wc->opcode == IBV_WC_RECV) {
        /* 服务端 ACK；wait_recv() 在 cq_poller 里被叫醒 */
    } else if (wc->opcode == IBV_WC_SEND) {
        /* post_send() 在等这次完成 */
    }
}

static void on_connect_established(struct rdma_cm_id *cm_id) {
    printf("Connection established with %s\n", get_inet_addr_str(cm_id));
    fflush(stdout);

    conn_manger_t *conn_manger = (conn_manger_t *)cm_id->context;
    char *sbuffer = conn_manger->send_buffer;

    int fd = open(FILENAME, O_RDONLY);
    if (fd < 0) {
        perror("open " FILENAME);
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
    size_t idx = 0;

    printf("Sending %s (%zu bytes)\n", FILENAME, file_size);
    fflush(stdout);

    /* 先挂 recv 再发数据，才能接到服务端的 ACK。 */
    if (post_recv(conn_manger) != 0) {
        close(fd);
        rdma_disconnect(cm_id);
        return;
    }

    /* 单飞：SEND 数据 → 等 ACK → 再挂下一次 recv。siw 上不能连续 SEND 不等人。 */
    while (idx < file_size) {
        size_t remain = file_size - idx;
        size_t count = remain < BUFFER_SIZE ? remain : BUFFER_SIZE;

        ssize_t ret = read(fd, sbuffer, count);
        if (ret <= 0) {
            perror("read");
            break;
        }

        /* post_send 带真实读到的长度，最后一块不会按 64KB 补零。 */
        if (post_send(conn_manger, (uint32_t)ret) != 0)
            break;
        if (wait_recv(conn_manger) != 0)
            break;

        idx += (size_t)ret;
        if (idx == file_size || idx % (1024 * 1024) == 0)
            printf("sent %zu / %zu\n", idx, file_size);
        fflush(stdout);

        if (idx < file_size && post_recv(conn_manger) != 0)
            break;
    }

    close(fd);
    /* 没有单独的结束报文，对端把 DISCONNECTED 当作文件收完。 */
    printf("Transfer finished, disconnecting\n");
    rdma_disconnect(cm_id);
}

/* 发送不能堵在 CM 线程里，否则收不到 DISCONNECTED。 */
static void *client_send_thread(void *arg) {
    on_connect_established((struct rdma_cm_id *)arg);
    return NULL;
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

    if (rdma_resolve_addr(cm_id, NULL, (struct sockaddr *)&server_addr, 2000)) {
        perror("rdma_resolve_addr");
        return -1;
    }

    struct rdma_cm_event *event;
    while (rdma_get_cm_event(eventchannel, &event) == 0) {
        enum rdma_cm_event_type ev = event->event;
        struct rdma_cm_id *id = event->id;

        /* 每个 CM 事件都必须 ack，否则连接管理器会卡住。 */
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
                /* 此时才有 cm_id->verbs，才能建 QP / 注册内存。 */
                initialize_connection(cm_id, on_completion_client);
                {
                    struct rdma_conn_param conn_param;
                    memset(&conn_param, 0, sizeof(conn_param));
                    conn_param.responder_resources = 1;
                    conn_param.initiator_depth = 1;
                    conn_param.retry_count = 7;
                    conn_param.rnr_retry_count = 7;  /* 7 在 IB 上表示无限 RNR 重试；siw 上仍不可靠 */
                    if (rdma_connect(cm_id, &conn_param)) {
                        perror("rdma_connect");
                        return -1;
                    }
                }
                break;
            case RDMA_CM_EVENT_ESTABLISHED: {
                rdma_ack_cm_event(event);
                pthread_t th;
                pthread_create(&th, NULL, client_send_thread, id);
                pthread_detach(th);
                break;
            }
            case RDMA_CM_EVENT_DISCONNECTED:
                disconnected = 1;
                rdma_ack_cm_event(event);
                destory_connection(id);
                printf("Disconnected.\n");
                return 0;
            default:
                printf("Unhandled event: %s\n", rdma_event_str(ev));
                rdma_ack_cm_event(event);
                break;
        }
    }

    return 0;
}
