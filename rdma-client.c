
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

static void on_completion_client(struct ibv_wc *wc) {

    if (wc->status != IBV_WC_SUCCESS) {
        if (disconnected) {
            printf("Disconnected from server.\n");
        } else {
            fprintf(stderr, "Work completion error: %s\n", ibv_wc_status_str(wc->status));
        }
        
        return ;
    }

    //wr.wr_id  -->  wc.wr_id;
    conn_manger_t *conn_manger = (conn_manger_t*)wc->wr_id;
    if (wc->opcode == IBV_WC_RECV) {
        printf("Received : %s\n", conn_manger->recv_buffer);

    } else if (wc->opcode == IBV_WC_SEND) {
        //printf("Send : %s\n", conn_manger->send_buffer);

    }
}

static void on_connect_established(struct rdma_cm_id *cm_id) {
    printf("Connection established with %s\n", get_inet_addr_str(cm_id));

    conn_manger_t *conn_manger = (conn_manger_t*)cm_id->context;

    char *sbuffer = conn_manger->send_buffer;
    char *rbuffer = conn_manger->recv_buffer;

    int fd = open(FILENAME, O_RDONLY);
    if (fd <= 0) return ;

    struct stat st;
    if (-1 == fstat(fd, &st)) {
        close(fd);
        return ;
    }
    size_t file_size = st.st_size;
    size_t idx = 0, count = 0;

    printf("enter loop\n");
    while (1) {

        memset(sbuffer, 0, BUFFER_SIZE);
#if 1
        if (idx + BUFFER_SIZE <= file_size) {
            count = BUFFER_SIZE;
        } else if (idx < file_size) {
            count = file_size - idx;
        } else {
            close(fd);
            break;
        }
        int ret = read(fd, sbuffer, BUFFER_SIZE);
        idx += ret;
        printf("idx: %ld", idx);

        //getchar();

#elif 0
        sprintf(sbuffer, "abcdefghijklmnopqrstuvwxyz");
        getchar();
#else 
        printf(">>>>> ");
        scanf("%s", sbuffer);
        printf("\n");
#endif
        post_send(conn_manger);

        // Post receive
#if 0        
        memset(rbuffer, 0, BUFFER_SIZE);
        post_recv(conn_manger);
#endif

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
                // Proceed to resolve route
                if (0 != rdma_resolve_route(cm_id, 2000)) {
                    perror("rdma_resolve_route failed\n");
                    return -1;
                }
                break;
            case RDMA_CM_EVENT_ROUTE_RESOLVED:

                initialize_connection(cm_id, on_completion_client);
                // Now we can connect
                if (0 != rdma_connect(cm_id, NULL)) {
                    perror("rdma_connect failed\n");
                    return -1;
                }
                break;
            case RDMA_CM_EVENT_ESTABLISHED:
                on_connect_established(event->id);
                break;
            case RDMA_CM_EVENT_DISCONNECTED:
                disconnected = 1;
                destory_connection(event->id);
                break;
            default:
                printf("Unhandled event: %d\n", event->event);
                break;
        }

    }


    return 0;
}



