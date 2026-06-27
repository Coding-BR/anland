#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "../common/protocol.h"
#include "../common/socket_utils.h"

#define MAX_EVENTS 16
#define MAX_FDS    4
#define ABSTRACT_SOCKET_NAME "anland_display"

struct listener {
    int fd;
    const char *name;
    const char *unlink_path;
};

struct client {
    int  ctrl_fd;
    bool is_consumer;
};

static struct client *consumer;
static struct client *producer;
static int epoll_fd;
static volatile bool running = true;

static struct screen_info stored_screen;
static bool has_screen_info;

static int deposited_fds[MAX_FDS];
static int deposited_fd_count;

static bool producer_waiting_screen;
static bool producer_waiting_fds;

static struct listener file_listener = { .fd = -1 };
static struct listener abstract_listener = { .fd = -1 };

static void handle_signal(int sig)
{
    (void)sig;
    running = false;
}

static void client_free(struct client *c)
{
    if (!c) return;
    if (c->ctrl_fd >= 0) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c->ctrl_fd, NULL);
        close(c->ctrl_fd);
    }
    free(c);
}

static void clear_deposited_fds(void)
{
    for (int i = 0; i < deposited_fd_count; i++)
        close(deposited_fds[i]);
    deposited_fd_count = 0;
}

static int send_ctrl(int fd, uint32_t type)
{
    struct ctrl_msg msg = { .type = type, .size = 0 };
    return send_all(fd, &msg, sizeof(msg));
}

static int send_screen_info_msg(int fd)
{
    struct ctrl_msg hdr = { .type = CTRL_MSG_SCREEN_INFO, .size = sizeof(struct screen_info) };
    uint8_t buf[sizeof(struct ctrl_msg) + sizeof(struct screen_info)];
    memcpy(buf, &hdr, sizeof(hdr));
    memcpy(buf + sizeof(hdr), &stored_screen, sizeof(stored_screen));
    return send_all(fd, buf, sizeof(buf));
}

static void update_screen_info(const struct screen_info *si)
{
    bool changed = !has_screen_info || memcmp(si, &stored_screen, sizeof(*si)) != 0;

    stored_screen = *si;
    has_screen_info = true;

    if (changed) {
        fprintf(stderr, "daemon: screen info %ux%u fmt=%u refresh=%u\n",
                si->width, si->height, si->format, si->refresh);
        if (producer) {
            send_screen_info_msg(producer->ctrl_fd);
            producer_waiting_screen = false;
        }
    }
}

static void try_deliver_fds(void)
{
    if (!producer || deposited_fd_count < 4) {
        producer_waiting_fds = true;
        return;
    }

    if (consumer)
        send_ctrl(consumer->ctrl_fd, CTRL_MSG_FDS_READY);

    struct ctrl_msg msg = { .type = CTRL_MSG_FDS_READY, .size = 0 };
    if (send_fds(producer->ctrl_fd, &msg, sizeof(msg),
                 deposited_fds, deposited_fd_count) < 0) {
        fprintf(stderr, "daemon: failed to send fds to producer\n");
        producer_waiting_fds = true;
        return;
    }

    for (int i = 0; i < deposited_fd_count; i++)
        close(deposited_fds[i]);
    deposited_fd_count = 0;
    producer_waiting_fds = false;

    fprintf(stderr, "daemon: fds delivered to producer\n");
}

static void handle_disconnect(struct client *c)
{
    if (c == consumer) {
        fprintf(stderr, "daemon: consumer disconnected\n");
        client_free(consumer);
        consumer = NULL;
    } else if (c == producer) {
        fprintf(stderr, "daemon: producer disconnected\n");
        client_free(producer);
        producer = NULL;
        producer_waiting_screen = false;
        producer_waiting_fds = false;
    }
}

static void handle_client_data(struct client *c)
{
    struct ctrl_msg hdr;
    int fds[MAX_FDS];
    int fd_count = 0;

    int n = recv_fds(c->ctrl_fd, &hdr, sizeof(hdr), fds, MAX_FDS, &fd_count);
    if (n <= 0) {
        handle_disconnect(c);
        return;
    }

    uint8_t payload[sizeof(struct screen_info)];
    if (hdr.size > 0) {
        if (hdr.size > sizeof(payload) || recv_all(c->ctrl_fd, payload, hdr.size) < 0) {
            handle_disconnect(c);
            return;
        }
    }

    switch (hdr.type) {
    case CTRL_MSG_CONSUMER_HELLO:
        if (c == consumer && fd_count >= 3) {
            clear_deposited_fds();
            memcpy(deposited_fds, fds, sizeof(int) * fd_count);
            deposited_fd_count = fd_count;
            fprintf(stderr, "daemon: consumer re-deposited %d fds\n", fd_count);
            if (producer_waiting_fds)
                try_deliver_fds();
        }
        break;

    case CTRL_MSG_SCREEN_INFO:
        if (c == consumer && hdr.size == sizeof(struct screen_info)) {
            struct screen_info si;
            memcpy(&si, payload, sizeof(si));
            update_screen_info(&si);
            if (producer_waiting_screen && producer) {
                send_screen_info_msg(producer->ctrl_fd);
                producer_waiting_screen = false;
            }
        }
        break;

    case CTRL_MSG_PICKUP_FDS:
        if (c == producer)
            try_deliver_fds();
        break;

    default:
        break;
    }
}

static void handle_new_connection(int listen_fd)
{
    int client_fd = accept(listen_fd, NULL, NULL);
    if (client_fd < 0)
        return;

    struct ctrl_msg hdr;
    int fds[MAX_FDS];
    int fd_count = 0;

    int n = recv_fds(client_fd, &hdr, sizeof(hdr), fds, MAX_FDS, &fd_count);
    if (n < (int)sizeof(struct ctrl_msg)) {
        close(client_fd);
        return;
    }

    struct client *c = calloc(1, sizeof(*c));
    c->ctrl_fd = client_fd;

    if (hdr.type == CTRL_MSG_CONSUMER_HELLO) {
        if (consumer)
            client_free(consumer);
        c->is_consumer = true;
        consumer = c;

        clear_deposited_fds();
        memcpy(deposited_fds, fds, sizeof(int) * fd_count);
        deposited_fd_count = fd_count;
        fprintf(stderr, "daemon: consumer connected, %d fds\n", fd_count);

        if (producer_waiting_fds)
            try_deliver_fds();

    } else if (hdr.type == CTRL_MSG_PRODUCER_HELLO) {
        if (producer)
            client_free(producer);
        c->is_consumer = false;
        producer = c;
        producer_waiting_screen = false;
        producer_waiting_fds = false;
        fprintf(stderr, "daemon: producer connected\n");

        if (has_screen_info)
            send_screen_info_msg(client_fd);
        else
            producer_waiting_screen = true;

    } else {
        close(client_fd);
        free(c);
        return;
    }

    struct epoll_event ev = { .events = EPOLLIN | EPOLLHUP | EPOLLERR, .data.ptr = c };
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
}

static int create_listener(struct listener *listener, const char *path, bool abstract)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    socklen_t addr_len = sizeof(addr);

    if (abstract) {
        size_t name_len = strlen(path);
        if (name_len > sizeof(addr.sun_path) - 2) {
            close(fd);
            errno = ENAMETOOLONG;
            perror("abstract socket name");
            return -1;
        }
        addr.sun_path[0] = '\0';
        memcpy(addr.sun_path + 1, path, name_len);
        addr_len = (socklen_t)(sizeof(addr.sun_family) + 1 + name_len);
        listener->unlink_path = NULL;
    } else {
        unlink(path);
        strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
        listener->unlink_path = path;
    }

    if (bind(fd, (struct sockaddr *)&addr, addr_len) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, 4) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }

    listener->fd = fd;
    listener->name = path;
    return 0;
}

int main(int argc, char **argv)
{
    const char *sock_path = (argc > 1) ? argv[1] : "/data/local/tmp/display_daemon.sock";

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGPIPE, SIG_IGN);

    if (create_listener(&file_listener, sock_path, false) < 0)
        return 1;
    if (create_listener(&abstract_listener, ABSTRACT_SOCKET_NAME, true) < 0)
        fprintf(stderr, "daemon: abstract socket @%s unavailable, continuing with file socket only\n",
                ABSTRACT_SOCKET_NAME);

    epoll_fd = epoll_create1(0);
    struct epoll_event ev = { .events = EPOLLIN, .data.ptr = &file_listener };
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, file_listener.fd, &ev);
    if (abstract_listener.fd >= 0) {
        ev.data.ptr = &abstract_listener;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, abstract_listener.fd, &ev);
    }

    fprintf(stderr, "daemon: listening on %s\n", sock_path);
    if (abstract_listener.fd >= 0)
        fprintf(stderr, "daemon: listening on @%s\n", ABSTRACT_SOCKET_NAME);

    struct epoll_event events[MAX_EVENTS];
    while (running) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.ptr == &file_listener ||
                events[i].data.ptr == &abstract_listener) {
                struct listener *listener = events[i].data.ptr;
                handle_new_connection(listener->fd);
            } else {
                struct client *c = events[i].data.ptr;
                if (events[i].events & (EPOLLHUP | EPOLLERR))
                    handle_disconnect(c);
                else
                    handle_client_data(c);
            }
        }
    }

    clear_deposited_fds();
    client_free(consumer);
    client_free(producer);
    close(file_listener.fd);
    if (abstract_listener.fd >= 0)
        close(abstract_listener.fd);
    close(epoll_fd);
    if (file_listener.unlink_path)
        unlink(file_listener.unlink_path);
    fprintf(stderr, "daemon: shutdown\n");
    return 0;
}
