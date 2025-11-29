
// 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>

#include "rdma.h"

// ibverbs 
// rdmacm

static void on_connection_request(struct rdma_cm_id *cm_id) {
    printf("Connection request from %s\n", get_inet_addr_str(cm_id));
    initialize_connection(cm_id);

    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));

    if (0 != rdma_accept(cm_id, &conn_param)) {
        perror("rdma_accept failed\n");
        exit(-1);
    }
}

static void on_connect_established(struct rdma_cm_id *cm_id) {
    printf("Connection established with %s\n", get_inet_addr_str(cm_id));

    char rbuffer[BUFFER_SIZE] = {0};

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)rbuffer;
    sge.length = BUFFER_SIZE;
    sge.lkey = 0; // Assume lkey is set appropriately

    struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
    memset(&recv_wr, 0, sizeof(recv_wr));
    recv_wr.wr_id = 2;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;    

    ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr);

}

// cm --> connection manager
// ./rdma-server 192.168.68.3 2000
int main(int argc, char *argv[]) {

    if (argc != 3) {
        printf("Usage: %s <server_ip> <server_port>\n", argv[0]);
        return -1;
    }

    struct rdma_event_channel *eventchannel = rdma_create_event_channel();
    struct rdma_cm_id *cm_id; 

    if (0 != rdma_create_id(eventchannel, &cm_id, NULL, RDMA_PS_TCP)) {
        perror("rdma_create_id failed\n");
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(atoi(argv[2]));
    server_addr.sin_addr.s_addr = inet_addr(argv[1]);

    if (0 != rdma_bind_addr(cm_id, (struct sockaddr *)&server_addr)) {
        perror("rdma_bind_addr failed\n");
        return -1;
    }

    if (0 != rdma_listen(cm_id, 10)) {
        perror("rdma_listen failed\n");
        return -1;
    }

    struct rdma_cm_event *event;
    while (0 == rdma_get_cm_event(eventchannel, &event)) {

        if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            printf("Received connection request.\n");
            on_connection_request(event->id);
        } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
            printf("Connection established.\n");
            on_connect_established(event->id);
        } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
            printf("Client disconnected.\n");
        }


        rdma_ack_cm_event(event);
    }

    getchar();

    return 0;
}

