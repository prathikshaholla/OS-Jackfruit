/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Provided boilerplate:
 *   - device registration and teardown
 *   - timer setup
 *   - RSS helper
 *   - soft-limit and hard-limit event helpers
 *   - ioctl dispatch shell
 *
 * YOUR WORK: Fill in all sections marked // TODO.
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1

/* ==============================================================
 * TODO 1: Define your linked-list node struct.
 *
 * Requirements:
 *   - track PID, container ID, soft limit, and hard limit
 *   - remember whether the soft-limit warning was already emitted
 *   - include `struct list_head` linkage
 * ============================================================== */
struct monitored_process {
    pid_t pid;
    char container_id[MONITOR_NAME_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int soft_warned;
    struct list_head list;
};


/* ==============================================================
 * TODO 2: Declare the global monitored list and a lock.
 *
 * Requirements:
 *   - shared across ioctl and timer code paths
 *   - protect insert, remove, and iteration safely
 *
 * You may choose either a mutex or a spinlock, but your README must
 * justify the choice in terms of the code paths you implemented.
 * ============================================================== */
static LIST_HEAD(monitored_processes);
static spinlock_t monitored_processes_lock;


/* --- Provided: internal device / timer state --- */
static struct timer_list monitor_timer;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 *
 * Returns the Resident Set Size in bytes for the given PID,
 * or -1 if the task no longer exists.
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    } else {
        put_task_struct(task);
        return -1;
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ---------------------------------------------------------------
 * Provided: soft-limit helper
 *
 * Log a warning when a process exceeds the soft limit.
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: hard-limit helper
 *
 * Kill a process when it exceeds the hard limit.
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

static struct monitored_process *find_entry_locked(pid_t pid, const char *container_id)
{
    struct monitored_process *entry;

    list_for_each_entry(entry, &monitored_processes, list) {
        if (pid > 0 && entry->pid != pid)
            continue;
        if (container_id[0] != '\0' && strncmp(entry->container_id, container_id, MONITOR_NAME_LEN) != 0)
            continue;
        return entry;
    }

    return NULL;
}

/* ---------------------------------------------------------------
 * Timer Callback - fires every CHECK_INTERVAL_SEC seconds.
 * --------------------------------------------------------------- */
static void timer_callback(struct timer_list *t)
{
    struct monitored_process *entry, *tmp;
    unsigned long flags;

    spin_lock_irqsave(&monitored_processes_lock, flags);
    list_for_each_entry_safe(entry, tmp, &monitored_processes, list) {
        long rss_bytes = get_rss_bytes(entry->pid);

        if (rss_bytes < 0) {
            printk(KERN_INFO
                   "[container_monitor] Removing stale entry container=%s pid=%d\n",
                   entry->container_id, entry->pid);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        /* Check soft limit first (warning only) */
        if (!entry->soft_warned && rss_bytes > (long)entry->soft_limit_bytes) {
            entry->soft_warned = 1;
            log_soft_limit_event(entry->container_id,
                                 entry->pid,
                                 entry->soft_limit_bytes,
                                 rss_bytes);
        }

        /* Check hard limit (kill) */
        if (rss_bytes > (long)entry->hard_limit_bytes) {
            char container_id[MONITOR_NAME_LEN];
            pid_t pid = entry->pid;
            unsigned long limit_bytes = entry->hard_limit_bytes;
            long rss = rss_bytes;

            strncpy(container_id, entry->container_id, sizeof(container_id) - 1);
            container_id[MONITOR_NAME_LEN - 1] = '\0';

            list_del(&entry->list);
            kfree(entry);
            spin_unlock_irqrestore(&monitored_processes_lock, flags);

            kill_process(container_id, pid, limit_bytes, rss);
            mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
            return;
        }
    }
    spin_unlock_irqrestore(&monitored_processes_lock, flags);

    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ---------------------------------------------------------------
 * IOCTL Handler
 *
 * Supported operations:
 *   - register a PID with soft + hard limits
 *   - unregister a PID when the runtime no longer needs tracking
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid, req.soft_limit_bytes, req.hard_limit_bytes);

        if (req.pid <= 0 || req.container_id[0] == '\0')
            return -EINVAL;

        if (req.soft_limit_bytes == 0 || req.hard_limit_bytes == 0 ||
            req.soft_limit_bytes > req.hard_limit_bytes)
            return -EINVAL;

        /* ==============================================================
         * TODO 4: Add a monitored entry.
         *
         * Requirements:
         *   - allocate and initialize one node from req
         *   - validate allocation and limits
         *   - insert into the shared list under the chosen lock
         * ============================================================== */
        {
            struct monitored_process *entry;
            unsigned long flags;

            spin_lock_irqsave(&monitored_processes_lock, flags);
            entry = find_entry_locked(req.pid, req.container_id);

            if (entry) {
                entry->soft_limit_bytes = req.soft_limit_bytes;
                entry->hard_limit_bytes = req.hard_limit_bytes;
                entry->soft_warned = 0;
            } else {
                entry = kzalloc(sizeof(*entry), GFP_ATOMIC);
                if (!entry) {
                    spin_unlock_irqrestore(&monitored_processes_lock, flags);
                    return -ENOMEM;
                }

                entry->pid = req.pid;
                entry->soft_limit_bytes = req.soft_limit_bytes;
                entry->hard_limit_bytes = req.hard_limit_bytes;
                entry->soft_warned = 0;
                strncpy(entry->container_id, req.container_id, MONITOR_NAME_LEN - 1);
                list_add_tail(&entry->list, &monitored_processes);
            }
            spin_unlock_irqrestore(&monitored_processes_lock, flags);
        }

        return 0;
    }

    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    /* ==============================================================
     * TODO 5: Remove a monitored entry on explicit unregister.
     *
     * Requirements:
     *   - search by PID, container ID, or both
     *   - remove the matching entry safely if found
     *   - return status indicating whether a matching entry was removed
     * ============================================================== */
    {
        struct monitored_process *entry, *tmp;
        unsigned long flags;

        spin_lock_irqsave(&monitored_processes_lock, flags);
        int removed = 0;

        list_for_each_entry_safe(entry, tmp, &monitored_processes, list) {
            int pid_matches = (req.pid > 0 && entry->pid == req.pid);
            int id_matches = (req.container_id[0] != '\0' &&
                              strncmp(entry->container_id, req.container_id, MONITOR_NAME_LEN) == 0);

            if (req.pid > 0 && req.container_id[0] != '\0') {
                if (!pid_matches || !id_matches)
                    continue;
            } else if (!pid_matches && !id_matches) {
                continue;
            }

            list_del(&entry->list);
            kfree(entry);
            removed = 1;
            break;
        }

        spin_unlock_irqrestore(&monitored_processes_lock, flags);

        return removed ? 0 : -ENOENT;
    }
}

/* --- Provided: file operations --- */
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void)
{
    spin_lock_init(&monitored_processes_lock);

    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void)
{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 8, 0)
    timer_delete_sync(&monitor_timer);
#else
    del_timer_sync(&monitor_timer);
#endif

    /* ==============================================================
     * TODO 6: Free all remaining monitored entries.
     *
     * Requirements:
     *   - remove and free every list node safely
     *   - leave no leaked state on module unload
     * ============================================================== */
    {
        unsigned long flags;

        spin_lock_irqsave(&monitored_processes_lock, flags);
    while (!list_empty(&monitored_processes)) {
        struct monitored_process *entry = list_first_entry(&monitored_processes,
                                                           struct monitored_process,
                                                           list);
        list_del(&entry->list);
        kfree(entry);
    }
        spin_unlock_irqrestore(&monitored_processes_lock, flags);
    }

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
