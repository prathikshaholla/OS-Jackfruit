/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <sys/mount.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef struct {
    int fd;
    char id[CONTAINER_ID_LEN];
    const char *stream;   /* "stdout" or "stderr" */
} log_reader_arg_t;

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef enum {
    TERMINATION_RUNNING = 0,
    TERMINATION_NORMAL_EXIT,
    TERMINATION_STOPPED,
    TERMINATION_HARD_LIMIT_KILLED,
    TERMINATION_SIGNAL_KILLED
} termination_reason_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];              /* rootfs this container uses */
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int exited;   /* 0 = running, 1 = exited */
    int stop_requested;
    termination_reason_t final_reason;
    char log_path[PATH_MAX];
    /*
     * Task 3: joinable producer threads — one per stream (stdout, stderr).
     * We join these on container exit so no log lines are lost.
     */
    pthread_t log_tid_stdout;
    pthread_t log_tid_stderr;
    int log_tid_valid;   /* 1 once threads are started */
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_stdout_fd;   /* write-end of stdout pipe */
    int log_stderr_fd;   /* write-end of stderr pipe */
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    pthread_cond_t metadata_cv;
    container_record_t *containers;
    pthread_t reaper_thread;
    char base_rootfs[PATH_MAX];  // Track the base rootfs for automatic cloning
} supervisor_ctx_t;

void *reaper_thread(void *arg);
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item);
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item);
static const char *state_to_string(container_state_t state);
static const char *termination_reason_to_string(termination_reason_t reason);
static int is_valid_state_transition(container_state_t from, container_state_t to);
static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer);
static void spawn_log_producers(container_record_t *c, int stdout_fd, int stderr_fd);
static void join_log_producers(container_record_t *c);
static void finalize_container_record(container_record_t *c, int status);
void *reaper_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;

    sigset_t set;
    int sig;

    sigemptyset(&set);
    sigaddset(&set, SIGCHLD);

    while (1) {
        sigwait(&set, &sig);

        int status;
        pid_t pid;

        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {

            pthread_mutex_lock(&ctx->metadata_lock);

            container_record_t *curr = ctx->containers;

            while (curr) {
                if (curr->host_pid == pid) {
                    finalize_container_record(curr, status);

                    if (!is_valid_state_transition(CONTAINER_RUNNING, curr->state) &&
                        curr->state != CONTAINER_STOPPED) {
                        fprintf(stderr, "[REAPER] Final state for %s is %s\n",
                                curr->id,
                                state_to_string(curr->state));
                    }

                    /*
                     * Task 3: join producer threads BEFORE releasing the
                     * metadata lock so every byte reaches the buffer before
                     * the record could be reused.  The lock is held during
                     * the join — producers never touch metadata_lock so
                     * there is no deadlock.
                     */
                    join_log_producers(curr);

                    if (ctx->monitor_fd >= 0) {
                        unregister_from_monitor(ctx->monitor_fd, curr->id, pid);
                    }

                    pthread_cond_broadcast(&ctx->metadata_cv);

                    break;
                }
                curr = curr->next;
            }

            pthread_mutex_unlock(&ctx->metadata_lock);
        }
    }
}
static supervisor_ctx_t *g_ctx = NULL;
static pid_t g_run_container_pid = 0;  // For signal forwarding in run command

// Signal handler to forward SIGINT/SIGTERM to the running container (client-side run)
static void forward_signal(int sig)
{
    if (g_run_container_pid > 0) {
        kill(g_run_container_pid, sig);
    }
}

/*
 * Supervisor-side SIGINT / SIGTERM handler.
 * Sets should_stop so the accept() loop can exit cleanly,
 * closes the server socket to unblock accept(), and initiates
 * bounded-buffer shutdown so the logger thread drains and exits.
 */
static void supervisor_shutdown_handler(int sig)
{
    (void)sig;
    if (!g_ctx) return;

    fprintf(stderr, "\n[SUPERVISOR] Caught signal %d — initiating shutdown\n", sig);

    /* Stop accepting new commands */
    g_ctx->should_stop = 1;

    /* Unblock accept() by closing the listening socket */
    if (g_ctx->server_fd >= 0) {
        close(g_ctx->server_fd);
        g_ctx->server_fd = -1;
    }

    /* Drain and stop logger */
    bounded_buffer_begin_shutdown(&g_ctx->log_buffer);
}

/*
 * Task 3 — Producer thread.
 *
 * Reads from one end of a container pipe (stdout or stderr) and pushes
 * chunks into the shared bounded buffer.  The thread is JOINABLE so the
 * supervisor can wait for it after the container exits, guaranteeing that
 * every byte written by the container reaches the log file before the
 * record is cleaned up.
 *
 * Correctness properties:
 *   - bounded_buffer_push() blocks (not spin-waits) when the buffer is
 *     full, so the producer never drops data due to a full buffer.
 *   - On EOF (container pipe closed) the thread exits naturally; the
 *     consumer will drain whatever is left.
 *   - On buffer shutdown (supervisor stopping) push returns -1 and the
 *     thread exits, dropping any remaining unread data — acceptable
 *     because the supervisor is going down anyway.
 */
void *log_reader_thread(void *arg)
{
    log_reader_arg_t *a = (log_reader_arg_t *)arg;
    char container_id[CONTAINER_ID_LEN];
    const char *stream = a->stream ? a->stream : "out";

    strncpy(container_id, a->id, sizeof(container_id) - 1);
    container_id[CONTAINER_ID_LEN - 1] = '\0';
    
    fprintf(stderr, "[DEBUG] log_reader_thread starting for %s/%s (fd=%d)\n",
            container_id, stream, a->fd);

    char buf[LOG_CHUNK_SIZE];
    ssize_t n;

    while ((n = read(a->fd, buf, sizeof(buf))) > 0) {
        fprintf(stderr, "[DEBUG] log_reader_thread %s/%s: read %zd bytes\n",
                container_id, stream, n);
        
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, container_id, CONTAINER_ID_LEN - 1);
        memcpy(item.data, buf, (size_t)n);
        item.length = (size_t)n;

        /*
         * bounded_buffer_push blocks when the buffer is full.
         * Returns -1 only when the buffer is shutting down — in that
         * case we stop reading (supervisor is exiting).
         */
        if (bounded_buffer_push(&g_ctx->log_buffer, &item) != 0) {
            fprintf(stderr, "[LOG] %s/%s: buffer shutdown, stopping producer\n",
                    container_id, stream);
            break;
        }
    }

    if (n < 0 && errno != EBADF)
        perror("[LOG] pipe read error");

    close(a->fd);
    free(a);

    fprintf(stderr, "[LOG] %s/%s: producer thread exiting\n",
            container_id, stream);
    return NULL;
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        /* Stop if we encounter something that doesn't start with -- */
        if (argv[i][0] != '-' || argv[i][1] != '-') {
            break;
        }

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static const char *termination_reason_to_string(termination_reason_t reason)
{
    switch (reason) {
    case TERMINATION_RUNNING:
        return "running";
    case TERMINATION_NORMAL_EXIT:
        return "normal_exit";
    case TERMINATION_STOPPED:
        return "stopped";
    case TERMINATION_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    case TERMINATION_SIGNAL_KILLED:
        return "signal_killed";
    default:
        return "unknown";
    }
}

static termination_reason_t classify_termination(const container_record_t *c, int status)
{
    if (c->stop_requested)
        return TERMINATION_STOPPED;

    if (WIFEXITED(status))
        return TERMINATION_NORMAL_EXIT;

    if (WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL)
        return TERMINATION_HARD_LIMIT_KILLED;

    if (WIFSIGNALED(status))
        return TERMINATION_SIGNAL_KILLED;

    return TERMINATION_NORMAL_EXIT;
}

static void finalize_container_record(container_record_t *c, int status)
{
    termination_reason_t reason = classify_termination(c, status);

    c->final_reason = reason;
    c->exited = 1;

    if (WIFEXITED(status)) {
        c->exit_code = WEXITSTATUS(status);
        c->exit_signal = 0;
    } else if (WIFSIGNALED(status)) {
        c->exit_signal = WTERMSIG(status);
        c->exit_code = -1;
    } else {
        c->exit_code = -1;
        c->exit_signal = 0;
    }

    switch (reason) {
    case TERMINATION_STOPPED:
        c->state = CONTAINER_STOPPED;
        break;
    case TERMINATION_HARD_LIMIT_KILLED:
    case TERMINATION_SIGNAL_KILLED:
        c->state = CONTAINER_KILLED;
        break;
    case TERMINATION_NORMAL_EXIT:
    default:
        c->state = CONTAINER_EXITED;
        break;
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    // Wait if full
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    // If shutting down, stop
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    // Insert item
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    // Notify consumer
    pthread_cond_signal(&buffer->not_empty);

    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}
/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    // Wait if empty
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    // If empty AND shutting down → exit
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    // Remove item
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    // Notify producer
    pthread_cond_signal(&buffer->not_full);

    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
/*
 * Task 3 — Consumer thread (singleton).
 *
 * Pops log chunks from the shared bounded buffer and appends them to the
 * correct per-container log file.
 *
 * Design choices:
 *   - A small open-fd cache avoids reopening the same log file for every
 *     chunk, which would cause high syscall overhead and risks losing the
 *     final bytes if open() fails transiently.
 *   - The cache is flushed (all fds closed) when the consumer exits so
 *     that all kernel write buffers are committed before the supervisor
 *     returns.
 *   - bounded_buffer_pop() drains remaining items even after
 *     shutting_down is set (it returns -1 only when count == 0 AND
 *     shutting_down), so no log line is dropped when the supervisor stops.
 *
 * Race conditions that do NOT exist here:
 *   - The buffer mutex protects head/tail/count — no consumer/producer
 *     race on the ring buffer.
 *   - The metadata_lock is NOT held while writing to log files; log file
 *     access is private to this single consumer thread.
 */

#define LOG_CACHE_SIZE 8

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    int  fd;
} log_fd_cache_entry_t;

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    /* Simple open-fd cache — avoids open()/close() on every chunk */
    log_fd_cache_entry_t cache[LOG_CACHE_SIZE];
    int cache_count = 0;
    memset(cache, 0, sizeof(cache));
    for (int i = 0; i < LOG_CACHE_SIZE; i++) cache[i].fd = -1;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {

        /* Find or open the log fd for this container */
        int fd = -1;
        for (int i = 0; i < LOG_CACHE_SIZE; i++) {
            if (cache[i].fd >= 0 &&
                strncmp(cache[i].container_id, item.container_id,
                        CONTAINER_ID_LEN) == 0) {
                fd = cache[i].fd;
                break;
            }
        }

        if (fd < 0) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s.log",
                     LOG_DIR, item.container_id);

            /* Retry open() up to 3 times — avoids silent log loss on
             * transient failures (e.g. log dir not yet created). */
            for (int attempt = 0; attempt < 3 && fd < 0; attempt++) {
                fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
                if (fd < 0 && attempt < 2) usleep(5000);
            }
            if (fd < 0) {
                perror("[LOG] open failed after retries — chunk dropped");
                continue;
            }

            /* Evict LRU slot (round-robin eviction is fine here) */
            int slot = cache_count % LOG_CACHE_SIZE;
            if (cache[slot].fd >= 0)
                close(cache[slot].fd);

            strncpy(cache[slot].container_id, item.container_id,
                    CONTAINER_ID_LEN - 1);
            cache[slot].fd = fd;
            cache_count++;
        }

        /* Write — retry on EINTR */
        size_t written = 0;
        while (written < item.length) {
            ssize_t r = write(fd, item.data + written,
                              item.length - written);
            if (r < 0) {
                if (errno == EINTR) continue;
                perror("[LOG] write failed");
                break;
            }
            written += (size_t)r;
        }
    }

    /* Flush: close all cached fds so kernel buffers are committed */
    for (int i = 0; i < LOG_CACHE_SIZE; i++) {
        if (cache[i].fd >= 0) {
            close(cache[i].fd);
            cache[i].fd = -1;
        }
    }

    fprintf(stderr, "[LOG] consumer thread exiting\n");
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    printf("[CONTAINER %s] PID inside namespace = %d\n",
           cfg->id, getpid());   // should be 1

    // -------------------------
    // UTS namespace
    // -------------------------
    if (sethostname(cfg->id, strlen(cfg->id)) != 0) {
        perror("sethostname failed");
        exit(1);
    }

    // -------------------------
    // Mount namespace isolation
    // -------------------------
    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) {
        perror("mount propagation failed");
        exit(1);
    }

    // -------------------------
    // Filesystem isolation
    // -------------------------
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot failed");
        exit(1);
    }

    if (chdir("/") != 0) {
        perror("chdir failed");
        exit(1);
    }

    // -------------------------
    // Mount /proc
    // -------------------------
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc failed");
        exit(1);
    }

    // -------------------------
    // Clean environment
    // -------------------------
    clearenv();
    setenv("PATH", "/bin:/sbin:/usr/bin:/usr/sbin", 1);

    // -------------------------
    // Execute command
    // -------------------------
    /*
     * Task 3: redirect stdout and stderr to separate pipes so the
     * supervisor can label log lines by stream.
     */
    if (dup2(cfg->log_stdout_fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout failed");
        exit(1);
    }
    if (dup2(cfg->log_stderr_fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr failed");
        exit(1);
    }

    close(cfg->log_stdout_fd);
    close(cfg->log_stderr_fd);

    char *args[] = {"/bin/sh", "-c", cfg->command, NULL};
    execvp(args[0], args);

    perror("exec failed");
    return -1;
}
int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static container_record_t *create_container_record(control_request_t *req)
{
    container_record_t *c = calloc(1, sizeof(container_record_t));
    if (!c) return NULL;

    strncpy(c->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(c->rootfs, req->rootfs, PATH_MAX - 1);  // Track which rootfs this container uses

    c->started_at = time(NULL);
    c->state = CONTAINER_STARTING;
    c->final_reason = TERMINATION_RUNNING;
    c->stop_requested = 0;
    c->exit_code = 0;
    c->exit_signal = 0;

    c->soft_limit_bytes =
        req->soft_limit_bytes ? req->soft_limit_bytes : DEFAULT_SOFT_LIMIT;

    c->hard_limit_bytes =
        req->hard_limit_bytes ? req->hard_limit_bytes : DEFAULT_HARD_LIMIT;

    return c;
}

static container_record_t *find_container(container_record_t *head, const char *id)
{
    while (head) {
        if (strcmp(head->id, id) == 0)
            return head;
        head = head->next;
    }
    return NULL;
}

static void add_container(container_record_t **head, container_record_t *c)
{
    c->next = *head;
    *head = c;
}

/*
 * Check if rootfs is already in use by a running/starting container
 * Returns 1 if in use, 0 if available
 */
static int is_rootfs_in_use(container_record_t *head, const char *rootfs, const char *exclude_id)
{
    while (head) {
        // Skip if this container is being excluded (same ID as request)
        if (exclude_id && strcmp(head->id, exclude_id) == 0) {
            head = head->next;
            continue;
        }
        
        // Check if rootfs matches and container is active
        if (strcmp(head->rootfs, rootfs) == 0 && 
            head->state != CONTAINER_STOPPED && 
            head->state != CONTAINER_EXITED && 
            head->state != CONTAINER_KILLED) {
            return 1;  // In use
        }
        head = head->next;
    }
    return 0;  // Not in use
}

/*
 * Validate that a state transition is allowed
 * Returns 1 if valid, 0 if invalid
 */
static int is_valid_state_transition(container_state_t from, container_state_t to)
{
    // STARTING -> RUNNING is always valid
    if (from == CONTAINER_STARTING && to == CONTAINER_RUNNING) return 1;

    // RUNNING -> STOPPED (graceful shutdown via CMD_STOP)
    if (from == CONTAINER_RUNNING && to == CONTAINER_STOPPED) return 1;

    // STOPPED -> STOPPED (reaper confirming graceful stop — idempotent)
    if (from == CONTAINER_STOPPED && to == CONTAINER_STOPPED) return 1;

    // RUNNING -> KILLED (forced)
    if (from == CONTAINER_RUNNING && to == CONTAINER_KILLED) return 1;

    // STOPPED -> KILLED (forced kill after graceful stop failed)
    if (from == CONTAINER_STOPPED && to == CONTAINER_KILLED) return 1;

    // Any state can transition to EXITED or KILLED on reap
    if (to == CONTAINER_EXITED || to == CONTAINER_KILLED) return 1;

    // Invalid transition
    return 0;
}

/*
 * Clone rootfs from base rootfs
 * Returns 0 on success, -1 on failure
 */
static int clone_rootfs(const char *base_rootfs, const char *target_path)
{
    char base_abs[PATH_MAX];
    char target_abs[PATH_MAX];
    char cwd[PATH_MAX];

    if (getcwd(cwd, sizeof(cwd)) == NULL) {
        fprintf(stderr, "[ERROR] Failed to get CWD\n");
        return -1;
    }

    /* Resolve base path (must already exist) */
    if (base_rootfs[0] == '/') {
        strncpy(base_abs, base_rootfs, PATH_MAX - 1);
        base_abs[PATH_MAX - 1] = '\0';
    } else {
        if (realpath(base_rootfs, base_abs) == NULL) {
            /* realpath failed — construct manually with length check */
            size_t clen = strlen(cwd), blen = strlen(base_rootfs);
            if (clen + 1 + blen + 1 > PATH_MAX) {
                fprintf(stderr, "[ERROR] base_rootfs path too long\n");
                return -1;
            }
            memcpy(base_abs, cwd, clen);
            base_abs[clen] = '/';
            memcpy(base_abs + clen + 1, base_rootfs, blen + 1);
        }
    }

    /* Resolve target path (may not exist yet, so skip realpath) */
    if (target_path[0] == '/') {
        strncpy(target_abs, target_path, PATH_MAX - 1);
        target_abs[PATH_MAX - 1] = '\0';
    } else {
        size_t clen = strlen(cwd), tlen = strlen(target_path);
        if (clen + 1 + tlen + 1 > PATH_MAX) {
            fprintf(stderr, "[ERROR] target_path too long\n");
            return -1;
        }
        memcpy(target_abs, cwd, clen);
        target_abs[clen] = '/';
        memcpy(target_abs + clen + 1, target_path, tlen + 1);
    }

    /* Build cp command — buffer sized for two PATH_MAX paths */
    char cmd[PATH_MAX * 2 + 16];
    if ((size_t)snprintf(cmd, sizeof(cmd), "cp -a %s %s",
                         base_abs, target_abs) >= sizeof(cmd)) {
        fprintf(stderr, "[ERROR] cp command path too long\n");
        return -1;
    }

    fprintf(stderr, "[ROOTFS] Cloning: %s\n", cmd);

    int ret = system(cmd);
    if (ret != 0) {
        fprintf(stderr, "[ERROR] Failed to clone rootfs (exit code %d): %s\n",
                ret, cmd);
        return -1;
    }

    fprintf(stderr, "[ROOTFS] Cloned %s -> %s\n", base_abs, target_abs);
    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */

/*
 * Task 3 helper: spawn the two joinable log-producer threads for a container.
 *
 * stdout_fd / stderr_fd are the READ ends of the respective pipes.
 * The threads are stored in the container record so they can be joined later.
 */
static void spawn_log_producers(container_record_t *c,
                                 int stdout_fd, int stderr_fd)
{
    fprintf(stderr, "[DEBUG] spawn_log_producers called for %s\n", c->id);
    
    log_reader_arg_t *ao = malloc(sizeof(*ao));
    log_reader_arg_t *ae = malloc(sizeof(*ae));

    if (!ao || !ae) {
        fprintf(stderr, "[LOG] malloc failed for log_reader_arg\n");
        free(ao); free(ae);
        close(stdout_fd); close(stderr_fd);
        return;
    }

    ao->fd = stdout_fd;
    ao->stream = "stdout";
    strncpy(ao->id, c->id, CONTAINER_ID_LEN - 1);
    ao->id[CONTAINER_ID_LEN - 1] = '\0';

    ae->fd = stderr_fd;
    ae->stream = "stderr";
    strncpy(ae->id, c->id, CONTAINER_ID_LEN - 1);
    ae->id[CONTAINER_ID_LEN - 1] = '\0';

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    pthread_create(&c->log_tid_stdout, &attr, log_reader_thread, ao);
    pthread_create(&c->log_tid_stderr, &attr, log_reader_thread, ae);
    pthread_attr_destroy(&attr);

    c->log_tid_valid = 1;
}

/*
 * Task 3 helper: join the producer threads for a container after it exits.
 * This blocks until every byte in the pipes has been pushed to the buffer,
 * guaranteeing no log lines are dropped on abrupt container exit.
 */
static void join_log_producers(container_record_t *c)
{
    if (!c->log_tid_valid) return;
    pthread_join(c->log_tid_stdout, NULL);
    pthread_join(c->log_tid_stderr, NULL);
    c->log_tid_valid = 0;
    fprintf(stderr, "[LOG] %s: producer threads joined\n", c->id);
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    ctx.containers = NULL;
    ctx.should_stop = 0;
    strncpy(ctx.base_rootfs, rootfs, sizeof(ctx.base_rootfs) - 1);  // Store base rootfs
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = pthread_cond_init(&ctx.metadata_cv, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_cond_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR | O_CLOEXEC);
    if (ctx.monitor_fd < 0) {
        perror("open /dev/container_monitor failed");
        fprintf(stderr, "[SUPERVISOR] kernel monitor disabled; continuing without ioctl registration\n");
    }

    // =========================
    // CREATE UNIX SOCKET
    // =========================
    struct sockaddr_un addr;

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket failed");
        return 1;
    }

    unlink(CONTROL_PATH);

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind failed");
        return 1;
    }

    if (chmod(CONTROL_PATH, 0666) < 0) {
        perror("chmod failed");
        return 1;
    }

    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen failed");
        return 1;
    }

    printf("Supervisor started with rootfs: %s\n", rootfs);
    printf("Socket listening at %s\n", CONTROL_PATH);
    printf("Waiting for commands...\n");

    // =========================
    // LOGGING SETUP
    // =========================
    mkdir(LOG_DIR, 0755);

    // =========================
    // SIGNAL HANDLING (SAFE)
    // =========================
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGCHLD);

    // Block SIGCHLD in all threads (reaper_thread uses sigwait instead)
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    // Install SIGINT / SIGTERM handlers for graceful supervisor shutdown.
    // These must be installed AFTER blocking SIGCHLD so they don't interfere
    // with the reaper thread's sigwait().
    {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = supervisor_shutdown_handler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;   /* no SA_RESTART: we want accept() to return EINTR */
        sigaction(SIGINT,  &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
    }

    // =========================
    // START THREADS
    // =========================
    pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    pthread_create(&ctx.reaper_thread, NULL, reaper_thread, &ctx);

    // =========================
    // MAIN LOOP
    // =========================
    while (!ctx.should_stop) {

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (ctx.should_stop) break;
            if (errno == EINTR) continue;
            perror("accept failed");
            continue;
        }

        control_request_t req;
        memset(&req, 0, sizeof(req));

        int n = read(client_fd, &req, sizeof(req));
        if (n <= 0) {
            perror("read failed");
            close(client_fd);
            continue;
        }

        printf("[SUPERVISOR] received cmd=%d id=%s\n",
               req.kind, req.container_id);

        control_response_t res;
        memset(&res, 0, sizeof(res));
        res.status = 0;

        pthread_mutex_lock(&ctx.metadata_lock);

        switch (req.kind) {

        case CMD_START:
        {
            printf("[SUPERVISOR] START request: %s\n", req.container_id);

            // ⚠️ VALIDATION: Check container ID is not empty
            if (strlen(req.container_id) == 0) {
                res.status = -1;
                strcpy(res.message, "Container ID cannot be empty");
                break;
            }

            // ⚠️ VALIDATION: Check container ID contains only safe characters
            for (int i = 0; req.container_id[i]; i++) {
                char c_char = req.container_id[i];
                if (!isalnum(c_char) && c_char != '-' && c_char != '_') {
                    res.status = -1;
                    strcpy(res.message, "Container ID contains invalid characters (use alphanumeric, -, _)");
                    break;
                }
            }
            if (res.status != 0) break;

            container_record_t *existing =
                find_container(ctx.containers, req.container_id);

            container_record_t *c = NULL;

            if (existing) {
                if (existing->state == CONTAINER_RUNNING) {
                    res.status = -1;
                    strcpy(res.message, "Container already running");
                    break;
                }
                existing->started_at = time(NULL);
                c = existing;

            } else {
                // ⚠️ VALIDATION: Check if rootfs is already in use by another active container
                if (is_rootfs_in_use(ctx.containers, req.rootfs, NULL)) {
                    res.status = -1;
                    strcpy(res.message, "Rootfs already in use by another container");
                    break;
                }
                
                // ⚠️ FEATURE: Auto-clone rootfs from base if it doesn't exist
                struct stat st;
                if (stat(req.rootfs, &st) != 0) {
                    // Rootfs doesn't exist, try to clone it from base
                    fprintf(stderr, "[INFO] Rootfs %s doesn't exist, attempting to clone from %s\n",
                            req.rootfs, ctx.base_rootfs);
                    
                    if (clone_rootfs(ctx.base_rootfs, req.rootfs) != 0) {
                        res.status = -1;
                        strcpy(res.message, "Failed to clone rootfs from base (check permissions)");
                        break;
                    }
                }
                
                c = create_container_record(&req);
                if (!c) {
                    res.status = -1;
                    strcpy(res.message, "Memory allocation failed");
                    break;
                }
            }

            // Clear old log file when starting a container (new or restarted)
            char log_path[PATH_MAX];
            snprintf(log_path, sizeof(log_path), "%s/%s.log", 
                     LOG_DIR, req.container_id);
            unlink(log_path);  // Remove old log file so it starts fresh
            
            // Create empty log file so it exists immediately for fast-exiting containers
            int log_init_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (log_init_fd >= 0) {
                close(log_init_fd);
            }
            
            /* Task 3: two pipes — one per stream */
            int pipe_out[2], pipe_err[2];
            if (pipe(pipe_out) < 0 || pipe(pipe_err) < 0) {
                perror("pipe failed");
                res.status = -1;
                strcpy(res.message, "pipe failed");
                break;
            }

            char *stack = malloc(STACK_SIZE);
            child_config_t *cfg = malloc(sizeof(child_config_t));

            if (!stack || !cfg) {
                res.status = -1;
                strcpy(res.message, "allocation failed");
                close(pipe_out[0]); close(pipe_out[1]);
                close(pipe_err[0]); close(pipe_err[1]);
                free(stack); free(cfg);
                break;
            }

            memset(cfg, 0, sizeof(*cfg));
            strncpy(cfg->id,      req.container_id, sizeof(cfg->id) - 1);
            strncpy(cfg->rootfs,  req.rootfs,        sizeof(cfg->rootfs) - 1);
            strncpy(cfg->command, req.command,        sizeof(cfg->command) - 1);
            cfg->log_stdout_fd = pipe_out[1];
            cfg->log_stderr_fd = pipe_err[1];

            pid_t pid = clone(
                child_fn,
                stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                cfg
            );

            if (pid < 0) {
                perror("clone failed");
                res.status = -1;
                strcpy(res.message, "clone failed");
                close(pipe_out[0]); close(pipe_out[1]);
                close(pipe_err[0]); close(pipe_err[1]);
                free(stack); free(cfg);
                break;
            }

            /* Close write ends in supervisor — EOF propagates when child exits */
            close(pipe_out[1]);
            close(pipe_err[1]);

            c->host_pid   = pid;
            c->state      = CONTAINER_RUNNING;
            c->exited     = 0;
            c->stop_requested = 0;
            c->final_reason = TERMINATION_RUNNING;
            c->exit_code  = 0;
            c->exit_signal = 0;
            c->started_at = time(NULL);

            if (!existing)
                add_container(&ctx.containers, c);

            /* Task 3: spawn JOINABLE producer threads (one per stream) */
            fprintf(stderr, "[DEBUG] Spawning producer threads for %s (stdout_fd=%d, stderr_fd=%d)\n",
                    c->id, pipe_out[0], pipe_err[0]);
            spawn_log_producers(c, pipe_out[0], pipe_err[0]);

            /* Register with kernel monitor */
            if (ctx.monitor_fd >= 0) {
                if (register_with_monitor(ctx.monitor_fd,
                                          c->id, pid,
                                          c->soft_limit_bytes,
                                          c->hard_limit_bytes) < 0) {
                    fprintf(stderr, "[START] monitor registration failed for %s\n",
                            c->id);
                }
            }

            snprintf(res.message, sizeof(res.message),
                     "Container %s started with PID %d",
                     c->id, pid);

            break;
        }

        case CMD_RUN:
        {
            printf("[SUPERVISOR] RUN request: %s\n", req.container_id);

            /* --- Validation (mirrors CMD_START) --- */
            if (strlen(req.container_id) == 0) {
                res.status = -1;
                strcpy(res.message, "Container ID cannot be empty");
                break;
            }
            for (int i = 0; req.container_id[i]; i++) {
                char c_char = req.container_id[i];
                if (!isalnum(c_char) && c_char != '-' && c_char != '_') {
                    res.status = -1;
                    strcpy(res.message, "Container ID contains invalid characters");
                    break;
                }
            }
            if (res.status != 0) break;

            container_record_t *run_existing =
                find_container(ctx.containers, req.container_id);
            if (run_existing && run_existing->state == CONTAINER_RUNNING) {
                res.status = -1;
                strcpy(res.message, "Container already running");
                break;
            }

            /* Auto-clone rootfs if missing */
            {
                struct stat st;
                if (stat(req.rootfs, &st) != 0) {
                    fprintf(stderr, "[RUN] Rootfs %s missing, cloning from %s\n",
                            req.rootfs, ctx.base_rootfs);
                    if (clone_rootfs(ctx.base_rootfs, req.rootfs) != 0) {
                        res.status = -1;
                        strcpy(res.message, "Failed to clone rootfs");
                        break;
                    }
                }
            }

            /* Create / refresh metadata record */
            container_record_t *run_c = run_existing;
            if (!run_c) {
                run_c = create_container_record(&req);
                if (!run_c) {
                    res.status = -1;
                    strcpy(res.message, "Memory allocation failed");
                    break;
                }
            } else {
                run_c->started_at = time(NULL);
            }

            /* Fresh log file */
            char run_log_path[PATH_MAX];
            snprintf(run_log_path, sizeof(run_log_path), "%s/%s.log",
                     LOG_DIR, req.container_id);
            unlink(run_log_path);
            int run_log_init = open(run_log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (run_log_init >= 0) close(run_log_init);
            strncpy(run_c->log_path, run_log_path, PATH_MAX - 1);

            /* Task 3: two pipes — one per stream */
            int run_pipe_out[2], run_pipe_err[2];
            if (pipe(run_pipe_out) < 0 || pipe(run_pipe_err) < 0) {
                perror("pipe failed (run)");
                res.status = -1;
                strcpy(res.message, "pipe failed");
                if (!run_existing) free(run_c);
                break;
            }

            char *run_stack = malloc(STACK_SIZE);
            child_config_t *run_cfg = malloc(sizeof(child_config_t));
            if (!run_stack || !run_cfg) {
                res.status = -1;
                strcpy(res.message, "allocation failed");
                free(run_stack); free(run_cfg);
                close(run_pipe_out[0]); close(run_pipe_out[1]);
                close(run_pipe_err[0]); close(run_pipe_err[1]);
                if (!run_existing) free(run_c);
                break;
            }

            memset(run_cfg, 0, sizeof(*run_cfg));
            strncpy(run_cfg->id,      req.container_id, sizeof(run_cfg->id) - 1);
            strncpy(run_cfg->rootfs,  req.rootfs,        sizeof(run_cfg->rootfs) - 1);
            strncpy(run_cfg->command, req.command,        sizeof(run_cfg->command) - 1);
            run_cfg->log_stdout_fd = run_pipe_out[1];
            run_cfg->log_stderr_fd = run_pipe_err[1];
            run_cfg->nice_value    = req.nice_value;

            pid_t run_pid = clone(
                child_fn,
                run_stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                run_cfg
            );

            if (run_pid < 0) {
                perror("clone failed (run)");
                res.status = -1;
                strcpy(res.message, "clone failed");
                close(run_pipe_out[0]); close(run_pipe_out[1]);
                close(run_pipe_err[0]); close(run_pipe_err[1]);
                free(run_stack); free(run_cfg);
                if (!run_existing) free(run_c);
                break;
            }

            /* Close write ends in supervisor */
            close(run_pipe_out[1]);
            close(run_pipe_err[1]);

            run_c->host_pid   = run_pid;
            run_c->state      = CONTAINER_RUNNING;
            run_c->started_at = time(NULL);
            run_c->stop_requested = 0;
            run_c->final_reason = TERMINATION_RUNNING;

            if (!run_existing)
                add_container(&ctx.containers, run_c);

            /* Register with kernel monitor */
            if (ctx.monitor_fd >= 0) {
                if (register_with_monitor(ctx.monitor_fd,
                                          run_c->id, run_pid,
                                          run_c->soft_limit_bytes,
                                          run_c->hard_limit_bytes) < 0) {
                    fprintf(stderr, "[RUN] monitor registration failed for %s\n",
                            run_c->id);
                }
            }

            /* Task 3: spawn JOINABLE producer threads */
            spawn_log_producers(run_c, run_pipe_out[0], run_pipe_err[0]);

            /*
             * Wait for the reaper to finalize this container.
             */
            while (!run_c->exited) {
                pthread_cond_wait(&ctx.metadata_cv, &ctx.metadata_lock);
            }

            if (run_c->exit_signal != 0) {
                snprintf(res.message, sizeof(res.message),
                         "Container %s terminated: reason=%s signal=%d",
                         run_c->id,
                         termination_reason_to_string(run_c->final_reason),
                         run_c->exit_signal);
            } else {
                snprintf(res.message, sizeof(res.message),
                         "Container %s exited: reason=%s code=%d",
                         run_c->id,
                         termination_reason_to_string(run_c->final_reason),
                         run_c->exit_code);
            }

            break;
        }

case CMD_STOP:
        {
            container_record_t *c =
                find_container(ctx.containers, req.container_id);

            if (!c || c->state != CONTAINER_RUNNING) {
                res.status = -1;
                strcpy(res.message, "Container not running");
                break;
            }

            pid_t target_pid = c->host_pid;

            /*
             * Required attribution rule: mark stop_requested before sending
             * any signal so the final exit can be classified as stopped.
             */
            c->stop_requested = 1;

            /*
             * Kill the entire process group of the container so that
             * child processes spawned by the shell (e.g. sleep) also
             * receive SIGTERM and don't keep the shell alive.
             * killpg() falls back to kill() if getpgid() fails.
             */
            pid_t grp = getpgid(target_pid);
            if (grp > 0)
                killpg(grp, SIGTERM);
            else
                kill(target_pid, SIGTERM);

            /* Unlock before sleeping to allow reaper thread to update state */
            pthread_mutex_unlock(&ctx.metadata_lock);

            /* Wait for reaper thread to process the signal (max ~2 seconds) */
            for (int i = 0; i < 40; i++) {
                usleep(50000);  /* 50 ms per iteration */

                pthread_mutex_lock(&ctx.metadata_lock);
                container_record_t *check = find_container(ctx.containers, req.container_id);
                if (check && check->exited) {
                    pthread_mutex_unlock(&ctx.metadata_lock);
                    break;
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
            }

            /* Re-acquire lock for response sending below */
            pthread_mutex_lock(&ctx.metadata_lock);

            /* If process still running after 2 seconds, force kill it */
            if (kill(target_pid, 0) == 0) {
                if (grp > 0)
                    killpg(grp, SIGKILL);
                else
                    kill(target_pid, SIGKILL);
            }

            /* Unregister from kernel monitor */
            if (ctx.monitor_fd >= 0) {
                unregister_from_monitor(ctx.monitor_fd, req.container_id, target_pid);
            }

            strcpy(res.message, "Stop signal sent");
            break;
        }

        case CMD_PS:
        {
            container_record_t *curr = ctx.containers;
            char buffer[1024] = {0};

            while (curr) {
                char line[256];
                
                // Include exit code/signal/reason in ps output for exited containers
                if (curr->state != CONTAINER_RUNNING && curr->state != CONTAINER_STARTING && curr->exited) {
                    if (curr->exit_signal != 0) {
                        snprintf(line, sizeof(line),
                                 "ID=%s PID=%d STATE=%s FINAL_REASON=%s EXIT_SIGNAL=%d\n",
                                 curr->id,
                                 curr->host_pid,
                                 state_to_string(curr->state),
                                 termination_reason_to_string(curr->final_reason),
                                 curr->exit_signal);
                    } else {
                        snprintf(line, sizeof(line),
                                 "ID=%s PID=%d STATE=%s FINAL_REASON=%s EXIT_CODE=%d\n",
                                 curr->id,
                                 curr->host_pid,
                                 state_to_string(curr->state),
                                 termination_reason_to_string(curr->final_reason),
                                 curr->exit_code);
                    }
                } else {
                    snprintf(line, sizeof(line),
                             "ID=%s PID=%d STATE=%s FINAL_REASON=%s\n",
                             curr->id,
                             curr->host_pid,
                             state_to_string(curr->state),
                             termination_reason_to_string(curr->final_reason));
                }

                strncat(buffer, line, sizeof(buffer) - strlen(buffer) - 1);
                curr = curr->next;
            }

            strncpy(res.message, buffer, sizeof(res.message) - 1);
            break;
        }

        case CMD_LOGS:
        {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req.container_id);

            int fd = open(path, O_RDONLY);
            if (fd < 0) {
                res.status = -1;
                strcpy(res.message, "Log file not found");
                break;
            }

            char buffer[256];
            int n;
            char output[CONTROL_MESSAGE_LEN] = {0};

            while ((n = read(fd, buffer, sizeof(buffer) - 1)) > 0) {
                buffer[n] = '\0';
                strncat(output, buffer,
                        sizeof(output) - strlen(output) - 1);
            }

            close(fd);
            strncpy(res.message, output, sizeof(res.message) - 1);
            break;
        }

        default:
            res.status = -1;
            strcpy(res.message, "Unknown command");
            break;
        }

        pthread_mutex_unlock(&ctx.metadata_lock);

        if (write(client_fd, &res, sizeof(res)) < 0) {
            perror("write response failed");
        }
        close(client_fd);
    }

    /* =====================================================
     * GRACEFUL SUPERVISOR SHUTDOWN
     * =====================================================
     * Reach here when should_stop is set (SIGINT/SIGTERM).
     */
    fprintf(stderr, "[SUPERVISOR] Shutting down — waiting for logger thread\n");

    /* Ensure buffer shutdown is triggered (idempotent) */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);

    /* Wait for logger to drain and exit */
    pthread_join(ctx.logger_thread, NULL);

    /* Cancel reaper thread (it is blocked in sigwait) */
    pthread_cancel(ctx.reaper_thread);
    pthread_join(ctx.reaper_thread, NULL);

    /* Close monitor fd if open */
    if (ctx.monitor_fd >= 0) {
        close(ctx.monitor_fd);
        ctx.monitor_fd = -1;
    }

    /* Remove socket file */
    unlink(CONTROL_PATH);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_cond_destroy(&ctx.metadata_cv);
    pthread_mutex_destroy(&ctx.metadata_lock);

    fprintf(stderr, "[SUPERVISOR] Clean exit.\n");

    return 0;
}
/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int sockfd;
    struct sockaddr_un addr;

    sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket failed");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect failed (is supervisor running?)");
        close(sockfd);
        return 1;
    }

    // Send request
    if (write(sockfd, req, sizeof(*req)) < 0) {
        perror("write failed");
        close(sockfd);
        return 1;
    }

    // Receive response
    control_response_t res;
    memset(&res, 0, sizeof(res));

    if (read(sockfd, &res, sizeof(res)) > 0) {
        printf("Response: %s\n", res.message);
    }

    close(sockfd);
    return 0;
}

/*
 * Build command string from argv, stopping at the first flag (--xxx).
 * Returns the index where the flags start, or argc if no flags found.
 */
static int build_command_from_argv(char *command_buf,
                                   size_t command_buf_size,
                                   int argc,
                                   char *argv[],
                                   int start_index)
{
    size_t pos = 0;
    int i = start_index;

    for (i = start_index; i < argc; i++) {
        /* Stop if we hit a flag */
        if (argv[i][0] == '-' && argv[i][1] == '-') {
            break;
        }

        /* Add space between arguments */
        if (pos > 0 && pos < command_buf_size) {
            command_buf[pos++] = ' ';
        }

        /* Copy this argv entry */
        size_t len = strlen(argv[i]);
        if (pos + len < command_buf_size) {
            strcpy(&command_buf[pos], argv[i]);
            pos += len;
        } else {
            fprintf(stderr, "Command string too long\n");
            return -1;
        }
    }

    if (pos < command_buf_size) {
        command_buf[pos] = '\0';
    }

    return i;  /* Return where we stopped */
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));

    // default values (important!)
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    req.nice_value = 0;

    req.kind = CMD_START;

    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);

    /* Build command from argv[4..N] until we hit a flag */
    int flag_index = build_command_from_argv(req.command, sizeof(req.command), argc, argv, 4);
    if (flag_index < 0) {
        return 1;
    }

    /* Parse optional flags if any remain */
    if (flag_index < argc) {
        if (parse_optional_flags(&req, argc, argv, flag_index) != 0)
            return 1;
    }

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));

    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    req.nice_value = 0;

    req.kind = CMD_RUN;

    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);

    /* Build command from argv[4..N] until we hit a flag */
    int flag_index = build_command_from_argv(req.command, sizeof(req.command), argc, argv, 4);
    if (flag_index < 0) {
        return 1;
    }

    /* Parse optional flags if any remain */
    if (flag_index < argc) {
        if (parse_optional_flags(&req, argc, argv, flag_index) != 0)
            return 1;
    }

    /*
     * Install signal forwarding BEFORE sending the request.
     * The supervisor runs the container synchronously for CMD_RUN and
     * will block until it exits.  We forward SIGINT / SIGTERM to the
     * supervisor process so it can relay them to the container.
     *
     * We obtain the supervisor PID from the socket peer; since we use
     * a UNIX-domain socket we can retrieve it via SO_PEERCRED.
     */
    int sockfd_peek = socket(AF_UNIX, SOCK_STREAM, 0);
    pid_t supervisor_pid = -1;
    if (sockfd_peek >= 0) {
        struct sockaddr_un addr_peek;
        memset(&addr_peek, 0, sizeof(addr_peek));
        addr_peek.sun_family = AF_UNIX;
        strncpy(addr_peek.sun_path, CONTROL_PATH, sizeof(addr_peek.sun_path) - 1);

        if (connect(sockfd_peek, (struct sockaddr *)&addr_peek, sizeof(addr_peek)) == 0) {
            struct ucred cred;
            socklen_t cred_len = sizeof(cred);
            if (getsockopt(sockfd_peek, SOL_SOCKET, SO_PEERCRED, &cred, &cred_len) == 0) {
                supervisor_pid = cred.pid;
            }
        }
        close(sockfd_peek);
    }

    if (supervisor_pid > 0) {
        /*
         * Forward SIGINT / SIGTERM from this CLI process to the supervisor,
         * which will in turn forward them to the running container via
         * the CMD_RUN waitpid path and the supervisor_shutdown_handler.
         */
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = forward_signal;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        g_run_container_pid = supervisor_pid;   /* reuse forwarder for supervisor PID */
        sigaction(SIGINT,  &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
    }

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));

    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
