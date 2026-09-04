/*
 * 双边 SEND/RECV 文件传输的公共层（不是 RDMA WRITE）。
 *
 * 网卡只能 DMA 已 ibv_reg_mr 的内存；ibv_post_send 只表示请求进了发送队列，
 * 真正完成要等 CQ。对端必须先 post_recv，否则 RC SEND 会 RNR。
 *
 * 数据面一次只飞一块：发送端等 ACK 再发下一块，避免 Soft-iWARP 上流控空窗
 * 把 QP 打进错误态。
 */
#ifndef __RDMA_H__
#define __RDMA_H__

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#define BUFFER_SIZE  (64 * 1024)
#define CONN_MAGIC   0x52444d41u  /* 校验 wr_id 是否仍指向有效 conn */

static double timespec_to_sec(const struct timespec *t) {
    return (double)t->tv_sec + (double)t->tv_nsec / 1e9;
}

/* 不含建连，只统计 payload 字节 / 墙钟时间。 */
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
    struct ibv_qp *qp;
    pthread_mutex_t lock;
    pthread_cond_t cv;
    int send_outstanding;  /* post_send 阻塞，直到 SEND CQE */
    int recv_outstanding;  /* wait_recv 阻塞，直到 RECV CQE（ACK） */
    int in_handler;        /* CQ 回调正在跑，拆连接时要等它结束 */
    int shutdown;
} conn_manger_t;

typedef void (*on_completion_t)(struct ibv_wc *wc);

typedef struct cq_params {
    struct ibv_comp_channel *channel;
    on_completion_t on_complete;
} cq_params_t;

static char *get_inet_addr_str(struct rdma_cm_id *cm_id) {
    struct sockaddr *addr = rdma_get_peer_addr(cm_id);
    if (!addr || addr->sa_family != AF_INET)
        return "unknown";
    return inet_ntoa(((struct sockaddr_in *)addr)->sin_addr);
}

static void finish_send_locked(conn_manger_t *conn) {
    conn->send_outstanding = 0;
    pthread_cond_signal(&conn->cv);
}

static void finish_recv_locked(conn_manger_t *conn) {
    conn->recv_outstanding = 0;
    pthread_cond_signal(&conn->cv);
}

/* CQ 完成在独立线程：业务线程 post 之后用 condvar 等这里叫醒。
 * params 必须堆分配，不能把栈上结构传给这个线程。 */
static void *cq_poller(void *arg) {
    struct ibv_wc wc;
    struct ibv_cq *cq;
    void *ctx = NULL;
    cq_params_t *params = (cq_params_t *)arg;
    struct ibv_comp_channel *channel = params->channel;
    on_completion_t on_complete = params->on_complete;

    while (1) {
        if (ibv_get_cq_event(channel, &cq, &ctx)) {
            perror("ibv_get_cq_event");
            break;
        }
        ibv_ack_cq_events(cq, 1);
        ibv_req_notify_cq(cq, 0);

        /* 一次只 poll 1 个 CQE：num_entries 必须和 wc 数组长度一致。 */
        while (1) {
            int n = ibv_poll_cq(cq, 1, &wc);
            if (n < 0) {
                perror("ibv_poll_cq");
                break;
            }
            if (n == 0)
                break;

            /* post_* 时把 wr_id 设成 conn 指针，完成事件才能找回连接。 */
            conn_manger_t *conn = (conn_manger_t *)wc.wr_id;
            if (!conn || conn->magic != CONN_MAGIC) {
                fprintf(stderr, "cq_poller: invalid wr_id %p\n", (void *)wc.wr_id);
                continue;
            }

            pthread_mutex_lock(&conn->lock);
            if (conn->shutdown) {
                finish_send_locked(conn);
                finish_recv_locked(conn);
                pthread_mutex_unlock(&conn->lock);
                continue;
            }
            conn->in_handler = 1;
            pthread_mutex_unlock(&conn->lock);

            on_complete(&wc);

            pthread_mutex_lock(&conn->lock);
            conn->in_handler = 0;
            /* IBV_WC_RECV 的值是 1<<7，用位与判断 RECV 类完成。 */
            if (wc.opcode == IBV_WC_SEND || wc.status != IBV_WC_SUCCESS)
                finish_send_locked(conn);
            if ((wc.opcode & IBV_WC_RECV) || wc.status != IBV_WC_SUCCESS)
                finish_recv_locked(conn);
            pthread_cond_signal(&conn->cv);
            pthread_mutex_unlock(&conn->lock);
        }
    }

    return NULL;
}

static void destory_connection(struct rdma_cm_id *cm_id) {
    conn_manger_t *conn = (conn_manger_t *)cm_id->context;
    if (!conn)
        return;

    /* 等 CQ 回调退出后再 destroy_qp，否则回调里还在用 recv_buffer。 */
    pthread_mutex_lock(&conn->lock);
    conn->shutdown = 1;
    while (conn->in_handler)
        pthread_cond_wait(&conn->cv, &conn->lock);
    pthread_mutex_unlock(&conn->lock);

    rdma_destroy_qp(cm_id);

    if (conn->recv_mr)
        ibv_dereg_mr(conn->recv_mr);
    if (conn->send_mr)
        ibv_dereg_mr(conn->send_mr);

    free(conn->recv_buffer);
    free(conn->send_buffer);
    conn->recv_buffer = NULL;
    conn->send_buffer = NULL;
    conn->magic = 0;
    /* conn 本身不 free：CQ 线程可能还堵在 get_cq_event 上。 */

    rdma_destroy_id(cm_id);
}

static void initialize_connection(struct rdma_cm_id *cm_id, on_completion_t on_complete) {
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

    if (ibv_req_notify_cq(cq, 0)) {
        perror("ibv_req_notify_cq");
        exit(-1);
    }

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.qp_type = IBV_QPT_RC;  /* 可靠连接，SEND 有序、可重传 */
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
    conn->qp = cm_id->qp;
    pthread_mutex_init(&conn->lock, NULL);
    pthread_cond_init(&conn->cv, NULL);

    /* 按页对齐，方便 MR 钉页；siw 的 page_size_cap 是 4K。 */
    if (posix_memalign((void **)&conn->recv_buffer, 4096, BUFFER_SIZE) ||
        posix_memalign((void **)&conn->send_buffer, 4096, BUFFER_SIZE)) {
        perror("posix_memalign");
        exit(-1);
    }

    /* recv 必须能被网卡写入；send 是本地读出发。 */
    conn->recv_mr = ibv_reg_mr(pd, conn->recv_buffer, BUFFER_SIZE,
                                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    conn->send_mr = ibv_reg_mr(pd, conn->send_buffer, BUFFER_SIZE,
                                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!conn->recv_mr || !conn->send_mr) {
        perror("ibv_reg_mr");
        exit(-1);
    }

    cm_id->context = conn;

    cq_params_t *params = (cq_params_t *)malloc(sizeof(*params));
    if (!params) {
        perror("malloc cq_params");
        exit(-1);
    }
    params->channel = channel;
    params->on_complete = on_complete;

    /* params 在堆上，poller 线程会一直持有 channel 指针。 */
    pthread_t cq_poller_thread;
    if (pthread_create(&cq_poller_thread, NULL, cq_poller, params) != 0) {
        perror("pthread_create");
        exit(-1);
    }
    pthread_detach(cq_poller_thread);
}

static int wait_flag(conn_manger_t *conn, int *flag) {
    pthread_mutex_lock(&conn->lock);
    while (*flag)
        pthread_cond_wait(&conn->cv, &conn->lock);
    pthread_mutex_unlock(&conn->lock);
    return 0;
}

/* 把 send_buffer 前 length 字节 SEND 出去，并等到本地 SEND CQE。
 * length 必须是本块真实大小，最后一块不能填满 BUFFER_SIZE。 */
static int post_send(conn_manger_t *conn, uint32_t length) {
    if (length == 0 || length > BUFFER_SIZE)
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
    send_wr.send_flags = IBV_SEND_SIGNALED;  /* 否则没有 SEND CQE，wait 会永远卡住 */
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;

    pthread_mutex_lock(&conn->lock);
    conn->send_outstanding = 1;
    pthread_mutex_unlock(&conn->lock);

    if (ibv_post_send(conn->qp, &send_wr, &bad_send_wr)) {
        pthread_mutex_lock(&conn->lock);
        conn->send_outstanding = 0;
        pthread_mutex_unlock(&conn->lock);
        perror("ibv_post_send");
        return -1;
    }

    return wait_flag(conn, &conn->send_outstanding);
}

/* 非阻塞 SEND。CQ 线程里发 ACK 时不能调用会 wait 的 post_send，否则自死锁。 */
static int post_send_nowait(conn_manger_t *conn, uint32_t length) {
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

    if (ibv_post_send(conn->qp, &send_wr, &bad_send_wr)) {
        perror("ibv_post_send nowait");
        return -1;
    }
    return 0;
}

/* 挂一条 recv WQE。对端 SEND 到达前必须已经 post，否则 RNR。 */
static int post_recv(conn_manger_t *conn) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)conn->recv_buffer;
    sge.length = BUFFER_SIZE;
    sge.lkey = conn->recv_mr->lkey;

    struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
    memset(&recv_wr, 0, sizeof(recv_wr));
    recv_wr.wr_id = (uintptr_t)conn;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;

    pthread_mutex_lock(&conn->lock);
    conn->recv_outstanding = 1;
    pthread_mutex_unlock(&conn->lock);

    if (ibv_post_recv(conn->qp, &recv_wr, &bad_recv_wr)) {
        pthread_mutex_lock(&conn->lock);
        conn->recv_outstanding = 0;
        pthread_mutex_unlock(&conn->lock);
        perror("ibv_post_recv");
        return -1;
    }
    return 0;
}

/* 等到对端 ACK 的 RECV CQE。必须在 post_recv 之后调用。 */
static int wait_recv(conn_manger_t *conn) {
    return wait_flag(conn, &conn->recv_outstanding);
}

#endif
