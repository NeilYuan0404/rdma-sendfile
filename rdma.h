/*
 * 控制面：双边 SEND/RECV（ready / size / rkey / done / ack）。
 * 两端同一条路径：post_recv → post_send / expect_ctrl，CQ 上回收 WC。
 * 数据面：RDMA WRITE 泵（源已在 MR 中）。
 *
 * 约定：server 发送文件，client 接收。
 */
#ifndef __RDMA_H__
#define __RDMA_H__

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <time.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#define CTRL_BUF_SIZE  4096
#define CHUNK_SIZE     (8 * 1024 * 1024)
#define WR_WINDOW      128
#define POLL_BATCH     16
#define CONN_MAGIC     0x52444d41u

#define CTRL_SIZE    1
#define CTRL_RKEY    2
#define CTRL_DONE    3
#define CTRL_ACK     4
#define CTRL_READY   5

typedef struct ctrl_msg {
    uint32_t type;
    uint32_t rkey;
    uint64_t addr;
    uint64_t size;
} __attribute__((packed)) ctrl_msg_t;

static double timespec_to_sec(const struct timespec *t) {
    return (double)t->tv_sec + (double)t->tv_nsec / 1e9;
}

static void print_throughput(const char *side, size_t bytes,
                             const struct timespec *t0,
                             const struct timespec *t1) {
    double sec = timespec_to_sec(t1) - timespec_to_sec(t0);
    if (sec < 1e-9)
        sec = 1e-9;
    double mib = (double)bytes / (1024.0 * 1024.0);
    double mib_s = mib / sec;
    printf("%s: %zu bytes in %.4f s, %.2f MiB/s (%.2f Mbit/s)\n",
           side, bytes, sec, mib_s, mib_s * 8.0);
    fflush(stdout);
}

typedef struct conn_manger {
    uint32_t magic;
    char *recv_buffer;
    char *send_buffer;
    struct ibv_mr *recv_mr;
    struct ibv_mr *send_mr;
    char *src_buf;
    struct ibv_mr *src_mr;
    size_t src_size;
    char *dest_buf;
    struct ibv_mr *dest_mr;
    size_t dest_size;
    struct ibv_pd *pd;
    struct ibv_qp *qp;
    struct ibv_cq *cq;
    struct ibv_comp_channel *channel;
    uint64_t remote_addr;
    uint32_t remote_rkey;
    int send_outstanding;
    int recv_outstanding;
    int write_inflight;
    int shutdown;
} conn_manger_t;

static char *get_inet_addr_str(struct rdma_cm_id *cm_id) {
    struct sockaddr *addr = rdma_get_peer_addr(cm_id);
    if (!addr || addr->sa_family != AF_INET)
        return "unknown";
    return inet_ntoa(((struct sockaddr_in *)addr)->sin_addr);
}

static int poll_cq_batch(conn_manger_t *conn) {
    struct ibv_wc wcs[POLL_BATCH];
    int n = ibv_poll_cq(conn->cq, POLL_BATCH, wcs);
    if (n < 0) {
        perror("ibv_poll_cq");
        return -1;
    }

    for (int i = 0; i < n; i++) {
        struct ibv_wc *wc = &wcs[i];
        if (wc->status != IBV_WC_SUCCESS) {
            if (!conn->shutdown && wc->status != IBV_WC_WR_FLUSH_ERR)
                fprintf(stderr, "Work completion error: %s (opcode=%d)\n",
                        ibv_wc_status_str(wc->status), wc->opcode);
            conn->send_outstanding = 0;
            conn->recv_outstanding = 0;
            conn->write_inflight = 0;
            continue;
        }

        if (wc->opcode == IBV_WC_RDMA_WRITE) {
            int credits = (int)wc->wr_id;
            if (credits <= 0)
                credits = 1;
            conn->write_inflight -= credits;
            if (conn->write_inflight < 0)
                conn->write_inflight = 0;
        } else if (wc->opcode == IBV_WC_SEND) {
            conn->send_outstanding = 0;
        } else if (wc->opcode & IBV_WC_RECV) {
            conn->recv_outstanding = 0;
        }
    }
    return n;
}

/* 有完成就收；没有就在 channel 上阻塞，避免空转抢 siw。 */
static int cq_wait_progress(conn_manger_t *conn) {
    int n = poll_cq_batch(conn);
    if (n != 0)
        return n;
    if (!conn->channel)
        return 0;
    if (ibv_req_notify_cq(conn->cq, 0)) {
        perror("ibv_req_notify_cq");
        return -1;
    }
    n = poll_cq_batch(conn);
    if (n != 0)
        return n;

    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    if (ibv_get_cq_event(conn->channel, &ev_cq, &ev_ctx)) {
        perror("ibv_get_cq_event");
        return -1;
    }
    ibv_ack_cq_events(ev_cq, 1);
    return poll_cq_batch(conn);
}

static int wait_send_done(conn_manger_t *conn) {
    while (conn->send_outstanding) {
        if (cq_wait_progress(conn) < 0)
            return -1;
    }
    return 0;
}

static void destory_connection(struct rdma_cm_id *cm_id) {
    conn_manger_t *conn = (conn_manger_t *)cm_id->context;
    if (!conn)
        return;

    conn->shutdown = 1;
    rdma_destroy_qp(cm_id);
    conn->qp = NULL;

    if (conn->src_mr)
        ibv_dereg_mr(conn->src_mr);
    if (conn->dest_mr)
        ibv_dereg_mr(conn->dest_mr);
    if (conn->recv_mr)
        ibv_dereg_mr(conn->recv_mr);
    if (conn->send_mr)
        ibv_dereg_mr(conn->send_mr);

    if (conn->cq)
        ibv_destroy_cq(conn->cq);
    if (conn->channel)
        ibv_destroy_comp_channel(conn->channel);

    free(conn->src_buf);
    free(conn->dest_buf);
    free(conn->recv_buffer);
    free(conn->send_buffer);
    conn->magic = 0;
    free(conn);
    cm_id->context = NULL;

    rdma_destroy_id(cm_id);
}

static void initialize_connection(struct rdma_cm_id *cm_id) {
    struct ibv_pd *pd = ibv_alloc_pd(cm_id->verbs);
    if (!pd) {
        perror("ibv_alloc_pd");
        exit(-1);
    }

    struct ibv_comp_channel *channel = ibv_create_comp_channel(cm_id->verbs);
    if (!channel) {
        perror("ibv_create_comp_channel");
        exit(-1);
    }

    struct ibv_cq *cq = ibv_create_cq(cm_id->verbs, 1024, NULL, channel, 0);
    if (!cq) {
        perror("ibv_create_cq");
        exit(-1);
    }

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 1024;
    qp_attr.cap.max_recv_wr = 1024;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    if (rdma_create_qp(cm_id, pd, &qp_attr)) {
        perror("rdma_create_qp");
        exit(-1);
    }

    conn_manger_t *conn = (conn_manger_t *)calloc(1, sizeof(*conn));
    if (!conn) {
        perror("malloc conn_manger");
        exit(-1);
    }
    conn->magic = CONN_MAGIC;
    conn->pd = pd;
    conn->qp = cm_id->qp;
    conn->cq = cq;
    conn->channel = channel;

    if (posix_memalign((void **)&conn->recv_buffer, 4096, CTRL_BUF_SIZE) ||
        posix_memalign((void **)&conn->send_buffer, 4096, CTRL_BUF_SIZE)) {
        perror("posix_memalign");
        exit(-1);
    }

    conn->recv_mr = ibv_reg_mr(pd, conn->recv_buffer, CTRL_BUF_SIZE,
                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    conn->send_mr = ibv_reg_mr(pd, conn->send_buffer, CTRL_BUF_SIZE,
                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!conn->recv_mr || !conn->send_mr) {
        perror("ibv_reg_mr");
        exit(-1);
    }

    cm_id->context = conn;
}

static void fill_ctrl(char *buf, uint32_t type, uint32_t rkey, uint64_t addr, uint64_t size) {
    ctrl_msg_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = type;
    msg.rkey = rkey;
    msg.addr = addr;
    msg.size = size;
    memcpy(buf, &msg, sizeof(msg));
}

static ctrl_msg_t parse_ctrl(const char *buf) {
    ctrl_msg_t msg;
    memcpy(&msg, buf, sizeof(msg));
    return msg;
}

static int expect_ctrl(conn_manger_t *conn, uint32_t type, ctrl_msg_t *out) {
    while (conn->recv_outstanding) {
        if (cq_wait_progress(conn) < 0)
            return -1;
    }
    ctrl_msg_t msg = parse_ctrl(conn->recv_buffer);
    if (msg.type != type) {
        fprintf(stderr, "unexpected ctrl type %u (want %u)\n", msg.type, type);
        return -1;
    }
    if (out)
        *out = msg;
    return 0;
}

static int post_send(conn_manger_t *conn, uint32_t length) {
    if (length == 0 || length > CTRL_BUF_SIZE)
        return -1;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)conn->send_buffer;
    sge.length = length;
    sge.lkey = conn->send_mr->lkey;

    struct ibv_send_wr send_wr, *bad_send_wr = NULL;
    memset(&send_wr, 0, sizeof(send_wr));
    send_wr.wr_id = (uintptr_t)conn;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;

    conn->send_outstanding = 1;
    if (ibv_post_send(conn->qp, &send_wr, &bad_send_wr)) {
        conn->send_outstanding = 0;
        perror("ibv_post_send");
        return -1;
    }
    return wait_send_done(conn);
}

static int post_rdma_write(conn_manger_t *conn, uint64_t local_addr, uint32_t lkey,
                           uint64_t offset, uint32_t length) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = local_addr;
    sge.length = length;
    sge.lkey = lkey;

    struct ibv_send_wr send_wr, *bad_send_wr = NULL;
    memset(&send_wr, 0, sizeof(send_wr));
    send_wr.wr_id = 1;
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.wr.rdma.remote_addr = conn->remote_addr + offset;
    send_wr.wr.rdma.rkey = conn->remote_rkey;

    conn->write_inflight++;
    if (ibv_post_send(conn->qp, &send_wr, &bad_send_wr)) {
        conn->write_inflight--;
        perror("ibv_post_send rdma_write");
        return -1;
    }
    return 0;
}

static int post_rdma_file(conn_manger_t *conn) {
    size_t offset = 0;
    size_t size = conn->src_size;
    uint32_t lkey = conn->src_mr->lkey;

    while (offset < size) {
        while (conn->write_inflight >= WR_WINDOW) {
            if (cq_wait_progress(conn) < 0)
                return -1;
        }
        size_t remain = size - offset;
        uint32_t len = remain > (size_t)CHUNK_SIZE ? (uint32_t)CHUNK_SIZE : (uint32_t)remain;
        if (post_rdma_write(conn, (uint64_t)(uintptr_t)(conn->src_buf + offset),
                            lkey, (uint64_t)offset, len) != 0)
            return -1;
        offset += len;
    }
    while (conn->write_inflight > 0) {
        if (cq_wait_progress(conn) < 0)
            return -1;
    }
    return 0;
}

static int post_recv(conn_manger_t *conn) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)conn->recv_buffer;
    sge.length = CTRL_BUF_SIZE;
    sge.lkey = conn->recv_mr->lkey;

    struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
    memset(&recv_wr, 0, sizeof(recv_wr));
    recv_wr.wr_id = (uintptr_t)conn;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;

    conn->recv_outstanding = 1;
    if (ibv_post_recv(conn->qp, &recv_wr, &bad_recv_wr)) {
        conn->recv_outstanding = 0;
        perror("ibv_post_recv");
        return -1;
    }
    return 0;
}

static int alloc_src_mr(conn_manger_t *conn, size_t size) {
    if (posix_memalign((void **)&conn->src_buf, 4096, size)) {
        perror("posix_memalign src");
        return -1;
    }
    conn->src_size = size;
    conn->src_mr = ibv_reg_mr(conn->pd, conn->src_buf, size, IBV_ACCESS_LOCAL_WRITE);
    if (!conn->src_mr) {
        perror("ibv_reg_mr src");
        return -1;
    }
    return 0;
}

static int alloc_dest_mr(conn_manger_t *conn, size_t size) {
    if (posix_memalign((void **)&conn->dest_buf, 4096, size)) {
        perror("posix_memalign dest");
        return -1;
    }
    memset(conn->dest_buf, 0, size);
    conn->dest_size = size;
    conn->dest_mr = ibv_reg_mr(conn->pd, conn->dest_buf, size,
                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!conn->dest_mr) {
        perror("ibv_reg_mr dest");
        return -1;
    }
    return 0;
}

#endif
