

#ifndef __RDMA_H__
#define __RDMA_H__

#define BUFFER_SIZE 1024

typedef struct conn_manger {
    char *recv_buffer;
    char *send_buffer;
    struct ibv_mr *recv_mr;
    struct ibv_mr *send_mr;
    struct ibv_qp *qp;
} conn_manger_t;

static char *get_inet_addr_str(struct rdma_cm_id *cm_id) {
    
    struct sockaddr_in *addr_in = (struct sockaddr_in *)rdma_get_peer_addr(cm_id);
    return inet_ntoa(addr_in->sin_addr);
}

// wc --> 
static void *cq_poller(void *arg) {

    struct ibv_wc wc;
    struct ibv_cq *cq;
    void *ctx = NULL;
    struct ibv_comp_channel *channel = (struct ibv_comp_channel*)arg;

    while (1) {
        printf("before ibv_get_cq_event\n");
        ibv_get_cq_event(channel, &cq, &ctx);
        printf(" ibv_get_cq_event\n");
        ibv_ack_cq_events(cq, 1);
        printf(" ibv_ack_cq_events\n");
        ibv_req_notify_cq(cq, 0);
        printf(" ibv_req_notify_cq\n");
        while (ibv_poll_cq(cq, 10, &wc)) {
            // Here we would normally handle work completions
            // For brevity, we skip that.
            if (wc.status != IBV_WC_SUCCESS) {
                fprintf(stderr, "Work completion error: %s\n", ibv_wc_status_str(wc.status));
                continue;
            }

            //wr.wr_id  -->  wc.wr_id;
            conn_manger_t *conn_manger = (conn_manger_t*)wc.wr_id;
            if (wc.opcode == IBV_WC_RECV) {
                printf("Received : %s\n", conn_manger->recv_buffer);
            } else if (wc.opcode == IBV_WC_SEND) {
                printf("Send : %s\n", conn_manger->send_buffer);
            }

        }

    }

}

static void initialize_connection(struct rdma_cm_id *cm_id) {
    // Placeholder for connection initialization logic
    printf("Initializing connection for %s\n", get_inet_addr_str(cm_id));

    struct ibv_pd *pd = ibv_alloc_pd(cm_id->verbs);
    if (!pd) {
        perror("ibv_alloc_pd failed\n");    
        exit(-1);   
    }

    struct ibv_comp_channel *channel = ibv_create_comp_channel(cm_id->verbs);
    if (!channel) {
        perror("ibv_create_comp_channel failed\n");   
        exit(-1);   
    }

    struct ibv_cq *cq = ibv_create_cq(cm_id->verbs, 10, NULL, channel, 0);
    if (!cq) {
        perror("ibv_create_cq failed\n");    
        exit(-1);   
    }

    if ( 0 != ibv_req_notify_cq(cq, 0)) {
        perror("ibv_req_notify_cq failed\n");    
        exit(-1);   
    }

    pthread_t cq_poller_thread;
    pthread_create(&cq_poller_thread, NULL, cq_poller, channel);

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 10;
    qp_attr.cap.max_recv_wr = 10;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    if (0 != rdma_create_qp(cm_id, pd, &qp_attr)) {
        perror("rdma_create_qp failed\n");    
        exit(-1);   
    }

    conn_manger_t *conn_manger = (conn_manger_t *)malloc(sizeof(conn_manger_t));
    //cm->qp = 

    conn_manger->recv_buffer = (char *)malloc(BUFFER_SIZE * sizeof(char));
    conn_manger->send_buffer = (char *)malloc(BUFFER_SIZE * sizeof(char));

    conn_manger->recv_mr = ibv_reg_mr(pd, conn_manger->recv_buffer, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    conn_manger->send_mr = ibv_reg_mr(pd, conn_manger->send_buffer, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);

    cm_id->context = conn_manger;

    return ;
}


#endif


