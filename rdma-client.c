
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>

#include "rdma.h"


static void on_connect_established(struct rdma_cm_id *cm_id) {
    printf("Connection established with %s\n", get_inet_addr_str(cm_id));

    char sbuffer[BUFFER_SIZE] = {0};
    char rbuffer[BUFFER_SIZE] = {0};

    while (1) {

        memset(sbuffer, 0, BUFFER_SIZE);

        printf(">>>>> ");
        scanf("%s", sbuffer);
        printf("\n");

        // Post send
        struct ibv_sge sge;
        memset(&sge, 0, sizeof(sge));
        sge.addr = (uintptr_t)sbuffer;
        sge.length = BUFFER_SIZE;
        sge.lkey = 0; // Assume lkey is set appropriately

        struct ibv_send_wr send_wr, *bad_send_wr = NULL;
        memset(&send_wr, 0, sizeof(send_wr));
        send_wr.wr_id = 1;
        send_wr.opcode = IBV_WR_SEND;
        send_wr.send_flags = IBV_SEND_SIGNALED;
        send_wr.sg_list = &sge;
        send_wr.num_sge = 1;    

        ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr);

        // Post receive
        struct ibv_sge rge;
        memset(&rge, 0, sizeof(rge));
        rge.addr = (uintptr_t)rbuffer;
        rge.length = BUFFER_SIZE;
        rge.lkey = 0; // Assume lkey is set appropriately   

        struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
        memset(&recv_wr, 0, sizeof(recv_wr));
        recv_wr.wr_id = 2;
        recv_wr.sg_list = &rge;
        recv_wr.num_sge = 1;

        ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr);

    }
}


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

    struct sockaddr_in client_addr;
    memset(&client_addr, 0, sizeof(client_addr));
    client_addr.sin_family = AF_INET;
    client_addr.sin_port = htons(atoi(argv[2]));
    client_addr.sin_addr.s_addr = inet_addr(argv[1]);

    // resolve --> resolve address
    if (0 != rdma_resolve_addr(cm_id, NULL, (struct sockaddr *)&client_addr, 2000)) {
        perror("rdma_resolve_addr failed\n");
        return -1;
    }

    struct rdma_cm_event *event;
    while (rdma_get_cm_event(eventchannel, &event) == 0) {
        
        switch (event->event) {
            case RDMA_CM_EVENT_ADDR_RESOLVED:
                printf("Address resolved.\n");
                // Proceed to resolve route
                if (0 != rdma_resolve_route(cm_id, 2000)) {
                    perror("rdma_resolve_route failed\n");
                    return -1;
                }
                break;
            case RDMA_CM_EVENT_ROUTE_RESOLVED:
                printf("Route resolved.\n");

                initialize_connection(cm_id);
                // Now we can connect
                if (0 != rdma_connect(cm_id, NULL)) {
                    perror("rdma_connect failed\n");
                    return -1;
                }
                break;
            case RDMA_CM_EVENT_ESTABLISHED:
                printf("Connection established.\n");
                on_connect_established(event->id);
                break;
            case RDMA_CM_EVENT_DISCONNECTED:
                printf("Disconnected from server.\n");
                break;
            default:
                printf("Unhandled event: %d\n", event->event);
                break;
        }

    }


    return 0;
}



