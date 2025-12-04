
// 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "rdma.h"

// ibverbs 
// rdmacm
// other thread

static int disconnected = 0;

int image_fd = 0;
int write_file(const char *filename, char *data, int length) {

    if (image_fd == 0) {
        image_fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0776);
        if (image_fd <= 0) {
            printf("open failed : %s\n", filename);
            exit(1);
        }
    }

    return write(image_fd, data, length);
}

static void on_completion_server(struct ibv_wc *wc) {

    if (wc->status != IBV_WC_SUCCESS) {
        if (disconnected) {
            printf("Client disconnected.\n");
        } else {
            fprintf(stderr, "Work completion error: %s\n", ibv_wc_status_str(wc->status));
        }
        
        return ;
    }

    //wr.wr_id  -->  wc.wr_id;
    conn_manger_t *conn_manger = (conn_manger_t*)wc->wr_id;
    if (wc->opcode == IBV_WC_RECV) {
        //printf("Received : %s\n", conn_manger->recv_buffer);
        write_file("output.mp4", conn_manger->recv_buffer, BUFFER_SIZE);
#if 0
        memcpy(conn_manger->send_buffer, conn_manger->recv_buffer, BUFFER_SIZE);
        post_send(conn_manger);
#else 
        post_recv(conn_manger);
#endif 
    } else if (wc->opcode == IBV_WC_SEND) {
        printf("Send : %s\n", conn_manger->send_buffer); //

        //post_recv(conn_manger);

    }

}


static void on_connection_request(struct rdma_cm_id *cm_id) {
    initialize_connection(cm_id, on_completion_server);

    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));

    if (0 != rdma_accept(cm_id, &conn_param)) {
        perror("rdma_accept failed\n");
        exit(-1);
    }
}

static void on_connect_established(struct rdma_cm_id *cm_id) {

    conn_manger_t *conn_manger = cm_id->context;

    post_recv(conn_manger);

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
            on_connection_request(event->id);
        } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
            on_connect_established(event->id);
        } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
            disconnected = 1;
            destory_connection(event->id);
        }


        rdma_ack_cm_event(event);
    }

    getchar();

    return 0;
}

