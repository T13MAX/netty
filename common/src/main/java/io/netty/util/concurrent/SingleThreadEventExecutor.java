/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThreadExecutorMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jetbrains.annotations.Async.Schedule;

import java.lang.Thread.State;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract base class for {@link OrderedEventExecutor}'s that execute all its submitted tasks in a single thread.
 */
public abstract class SingleThreadEventExecutor extends AbstractScheduledEventExecutor implements OrderedEventExecutor {

    // 默认最大待执行任务数量（队列大小上限），可以通过系统属性配置
    static final int DEFAULT_MAX_PENDING_EXECUTOR_TASKS = Math.max(16, SystemPropertyUtil.getInt("io.netty.eventexecutor.maxPendingTasks", Integer.MAX_VALUE));
    // 日志记录器
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SingleThreadEventExecutor.class);
    // EventExecutor 状态常量定义
    private static final int ST_NOT_STARTED = 1;      // 尚未启动
    private static final int ST_SUSPENDING = 2;       // 正在挂起中
    private static final int ST_SUSPENDED = 3;        // 已挂起
    private static final int ST_STARTED = 4;          // 已启动
    private static final int ST_SHUTTING_DOWN = 5;    // 正在关闭
    private static final int ST_SHUTDOWN = 6;         // 已关闭
    private static final int ST_TERMINATED = 7;       // 已终止
    // 空任务，占位用的 NOOP 实现
    private static final Runnable NOOP_TASK = new Runnable() {
        @Override
        public void run() {
            // Do nothing.
        }
    };
    // 用于 CAS 操作的状态更新器
    private static final AtomicIntegerFieldUpdater<SingleThreadEventExecutor> STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(SingleThreadEventExecutor.class, "state");
    // 用于延迟初始化 threadProperties 的原子引用更新器
    private static final AtomicReferenceFieldUpdater<SingleThreadEventExecutor, ThreadProperties> PROPERTIES_UPDATER = AtomicReferenceFieldUpdater.newUpdater(SingleThreadEventExecutor.class, ThreadProperties.class, "threadProperties");

    // 任务队列 保存待执行的任务 大多数时候使用MPSC(多生产者单消费者)无锁队列
    private final Queue<Runnable> taskQueue;
    // 执行任务的线程
    private volatile Thread thread;
    // 当前线程的属性信息(惰性初始化)
    @SuppressWarnings("unused")
    private volatile ThreadProperties threadProperties;
    // 执行器 用于创建内部执行线程
    private final Executor executor;
    // 是否曾被中断(用于记录中断状态)
    private volatile boolean interrupted;
    // 加锁保护任务处理的互斥锁 (少量地方使用)
    private final Lock processingLock = new ReentrantLock();
    // 用于阻塞其他线程直到线程真正启动完毕
    private final CountDownLatch threadLock = new CountDownLatch(1);
    // 关闭时需要执行的钩子任务集合
    private final Set<Runnable> shutdownHooks = new LinkedHashSet<Runnable>();
    // 是否在添加任务时立即唤醒线程
    private final boolean addTaskWakesUp;
    // 最大可排队任务数(taskQueue容量限制)
    private final int maxPendingTasks;
    // 任务拒绝处理器
    private final RejectedExecutionHandler rejectedExecutionHandler;
    // 是否支持线程挂起功能
    private final boolean supportSuspension;
    // 最后一次执行任务的时间
    private long lastExecutionTime;
    // 当前执行器状态
    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile int state = ST_NOT_STARTED;
    // 优雅关闭时的“静默期” (期间无任务才算安静)
    private volatile long gracefulShutdownQuietPeriod;
    // 优雅关闭的超时时间 (最大等待时间)
    private volatile long gracefulShutdownTimeout;
    // 优雅关闭的起始时间戳(纳秒)
    private long gracefulShutdownStartTime;
    // 执行器最终关闭的通知 Future
    private final Promise<?> terminationFuture = new DefaultPromise<Void>(GlobalEventExecutor.INSTANCE);


    /**
     * Create a new instance
     *
     * @param parent         the {@link EventExecutorGroup} which is the parent of this instance and belongs to it
     * @param threadFactory  the {@link ThreadFactory} which will be used for the used {@link Thread}
     * @param addTaskWakesUp {@code true} if and only if invocation of {@link #addTask(Runnable)} will wake up the
     *                       executor thread
     */
    protected SingleThreadEventExecutor(EventExecutorGroup parent, ThreadFactory threadFactory, boolean addTaskWakesUp) {
        this(parent, new ThreadPerTaskExecutor(threadFactory), addTaskWakesUp);
    }

    /**
     * Create a new instance
     *
     * @param parent          the {@link EventExecutorGroup} which is the parent of this instance and belongs to it
     * @param threadFactory   the {@link ThreadFactory} which will be used for the used {@link Thread}
     * @param addTaskWakesUp  {@code true} if and only if invocation of {@link #addTask(Runnable)} will wake up the
     *                        executor thread
     * @param maxPendingTasks the maximum number of pending tasks before new tasks will be rejected.
     * @param rejectedHandler the {@link RejectedExecutionHandler} to use.
     */
    protected SingleThreadEventExecutor(EventExecutorGroup parent, ThreadFactory threadFactory, boolean addTaskWakesUp, int maxPendingTasks, RejectedExecutionHandler rejectedHandler) {
        this(parent, new ThreadPerTaskExecutor(threadFactory), addTaskWakesUp, maxPendingTasks, rejectedHandler);
    }

    /**
     * Create a new instance
     *
     * @param parent            the {@link EventExecutorGroup} which is the parent of this instance and belongs to it
     * @param threadFactory     the {@link ThreadFactory} which will be used for the used {@link Thread}
     * @param addTaskWakesUp    {@code true} if and only if invocation of {@link #addTask(Runnable)} will wake up the
     *                          executor thread
     * @param supportSuspension {@code true} if suspension of this {@link SingleThreadEventExecutor} is supported.
     * @param maxPendingTasks   the maximum number of pending tasks before new tasks will be rejected.
     * @param rejectedHandler   the {@link RejectedExecutionHandler} to use.
     */
    protected SingleThreadEventExecutor(EventExecutorGroup parent, ThreadFactory threadFactory, boolean addTaskWakesUp, boolean supportSuspension, int maxPendingTasks, RejectedExecutionHandler rejectedHandler) {
        this(parent, new ThreadPerTaskExecutor(threadFactory), addTaskWakesUp, supportSuspension, maxPendingTasks, rejectedHandler);
    }

    /**
     * Create a new instance
     *
     * @param parent         the {@link EventExecutorGroup} which is the parent of this instance and belongs to it
     * @param executor       the {@link Executor} which will be used for executing
     * @param addTaskWakesUp {@code true} if and only if invocation of {@link #addTask(Runnable)} will wake up the
     *                       executor thread
     */
    protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor, boolean addTaskWakesUp) {
        this(parent, executor, addTaskWakesUp, DEFAULT_MAX_PENDING_EXECUTOR_TASKS, RejectedExecutionHandlers.reject());
    }

    /**
     * Create a new instance
     *
     * @param parent          the {@link EventExecutorGroup} which is the parent of this instance and belongs to it
     * @param executor        the {@link Executor} which will be used for executing
     * @param addTaskWakesUp  {@code true} if and only if invocation of {@link #addTask(Runnable)} will wake up the
     *                        executor thread
     * @param maxPendingTasks the maximum number of pending tasks before new tasks will be rejected.
     * @param rejectedHandler the {@link RejectedExecutionHandler} to use.
     */
    protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor, boolean addTaskWakesUp, int maxPendingTasks, RejectedExecutionHandler rejectedHandler) {
        this(parent, executor, addTaskWakesUp, false, maxPendingTasks, rejectedHandler);
    }

    /**
     * Create a new instance
     *
     * @param parent            the {@link EventExecutorGroup} which is the parent of this instance and belongs to it
     * @param executor          the {@link Executor} which will be used for executing
     * @param addTaskWakesUp    {@code true} if and only if invocation of {@link #addTask(Runnable)} will wake up the
     *                          executor thread
     * @param supportSuspension {@code true} if suspension of this {@link SingleThreadEventExecutor} is supported.
     * @param maxPendingTasks   the maximum number of pending tasks before new tasks will be rejected.
     * @param rejectedHandler   the {@link RejectedExecutionHandler} to use.
     */
    protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor, boolean addTaskWakesUp, boolean supportSuspension, int maxPendingTasks, RejectedExecutionHandler rejectedHandler) {
        super(parent);
        this.addTaskWakesUp = addTaskWakesUp;
        this.supportSuspension = supportSuspension;
        this.maxPendingTasks = Math.max(16, maxPendingTasks);
        this.executor = ThreadExecutorMap.apply(executor, this);
        taskQueue = newTaskQueue(this.maxPendingTasks);
        rejectedExecutionHandler = ObjectUtil.checkNotNull(rejectedHandler, "rejectedHandler");
    }

    protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor, boolean addTaskWakesUp, Queue<Runnable> taskQueue, RejectedExecutionHandler rejectedHandler) {
        this(parent, executor, addTaskWakesUp, false, taskQueue, rejectedHandler);
    }

    protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor, boolean addTaskWakesUp, boolean supportSuspension, Queue<Runnable> taskQueue, RejectedExecutionHandler rejectedHandler) {
        super(parent);
        this.addTaskWakesUp = addTaskWakesUp;
        this.supportSuspension = supportSuspension;
        this.maxPendingTasks = DEFAULT_MAX_PENDING_EXECUTOR_TASKS;
        this.executor = ThreadExecutorMap.apply(executor, this);
        this.taskQueue = ObjectUtil.checkNotNull(taskQueue, "taskQueue");
        this.rejectedExecutionHandler = ObjectUtil.checkNotNull(rejectedHandler, "rejectedHandler");
    }

    /**
     * @deprecated Please use and override {@link #newTaskQueue(int)}.
     */
    @Deprecated
    protected Queue<Runnable> newTaskQueue() {
        return newTaskQueue(maxPendingTasks);
    }

    /**
     * Create a new {@link Queue} which will holds the tasks to execute. This default implementation will return a
     * {@link LinkedBlockingQueue} but if your sub-class of {@link SingleThreadEventExecutor} will not do any blocking
     * calls on the this {@link Queue} it may make sense to {@code @Override} this and return some more performant
     * implementation that does not support blocking operations at all.
     * 新建任务队列
     */
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        return new LinkedBlockingQueue<Runnable>(maxPendingTasks);
    }

    /**
     * Interrupt the current running {@link Thread}.
     * 中断执行器内部线程
     */
    protected void interruptThread() {
        Thread currentThread = thread;
        if (currentThread == null) {
            // 如果线程尚未启动 记录中断标记，稍后启动时可以处理中断逻辑
            interrupted = true;
        } else {
            // 如果线程已经存在 直接调用 interrupt 方法中断线程
            currentThread.interrupt();
        }
    }

    /**
     * 从队列里拿一个任务
     *
     * @see Queue#poll()
     */
    protected Runnable pollTask() {
        assert inEventLoop();
        return pollTaskFrom(taskQueue);
    }

    protected static Runnable pollTaskFrom(Queue<Runnable> taskQueue) {
        for (; ; ) {
            Runnable task = taskQueue.poll();
            if (task != WAKEUP_TASK) {
                return task;
            }
        }
    }

    /**
     * Take the next {@link Runnable} from the task queue and so will block if no task is currently present.
     * <p>
     * Be aware that this method will throw an {@link UnsupportedOperationException} if the task queue, which was
     * created via {@link #newTaskQueue()}, does not implement {@link BlockingQueue}.
     * </p>
     * 从任务队列中获取一个要执行的任务 (阻塞式)
     * 只有在 taskQueue 是 BlockingQueue 时才能使用该方法，一般用于 OIO 模式等需要阻塞的场景
     *
     * @return {@code null} if the executor thread has been interrupted or waken up.
     */
    protected Runnable takeTask() {
        // 必须在当前 EventLoop 线程中调用
        assert inEventLoop();

        // 如果不是 BlockingQueue，则不支持阻塞式获取任务
        if (!(taskQueue instanceof BlockingQueue)) {
            throw new UnsupportedOperationException();
        }

        BlockingQueue<Runnable> taskQueue = (BlockingQueue<Runnable>) this.taskQueue;

        for (; ; ) {
            // 检查是否有定时任务
            ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
            if (scheduledTask == null) {
                // 没有定时任务，直接 take (阻塞等待) 普通任务
                Runnable task = null;
                try {
                    task = taskQueue.take(); // 阻塞直到有任务
                    if (task == WAKEUP_TASK) {
                        task = null; // WAKEUP_TASK 是唤醒标志 忽略
                    }
                } catch (InterruptedException e) {
                    // 被中断则忽略，返回 null
                }
                return task;
            } else {
                // 有定时任务
                long delayNanos = scheduledTask.delayNanos(); // 计算定时任务还需要多久才到期
                Runnable task = null;
                if (delayNanos > 0) {
                    try {
                        // 如果还没到期 就阻塞等待 delay 时间内是否有普通任务进来
                        task = taskQueue.poll(delayNanos, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        return null; // 被中断，返回 null
                    }
                }

                if (task == null) {
                    // 如果等待期间没有任务，尝试强制将到期的定时任务加入队列
                    // 否则如果 taskQueue 中总有一个任务，定时任务可能永远得不到执行
                    fetchFromScheduledTaskQueue();
                    task = taskQueue.poll(); // 再次尝试获取普通任务
                }

                if (task != null) {
                    if (task == WAKEUP_TASK) {
                        return null; // 忽略唤醒任务
                    }
                    return task;
                }
            }
        }
    }


    private boolean fetchFromScheduledTaskQueue() {
        return fetchFromScheduledTaskQueue(taskQueue);
    }

    /**
     * @return {@code true} if at least one scheduled task was executed.
     * 执行到期定时任务
     */
    private boolean executeExpiredScheduledTasks() {
        if (scheduledTaskQueue == null || scheduledTaskQueue.isEmpty()) {
            return false;
        }
        long nanoTime = getCurrentTimeNanos();
        Runnable scheduledTask = pollScheduledTask(nanoTime);
        if (scheduledTask == null) {
            return false;
        }
        do {
            safeExecute(scheduledTask);
        } while ((scheduledTask = pollScheduledTask(nanoTime)) != null);
        return true;
    }

    /**
     * @see Queue#peek()
     */
    protected Runnable peekTask() {
        assert inEventLoop();
        return taskQueue.peek();
    }

    /**
     * @see Queue#isEmpty()
     */
    protected boolean hasTasks() {
        assert inEventLoop();
        return !taskQueue.isEmpty();
    }

    /**
     * Return the number of tasks that are pending for processing.
     */
    public int pendingTasks() {
        return taskQueue.size();
    }

    /**
     * Add a task to the task queue, or throws a {@link RejectedExecutionException} if this instance was shutdown
     * before.
     */
    protected void addTask(Runnable task) {
        ObjectUtil.checkNotNull(task, "task");
        if (!offerTask(task)) {
            reject(task);
        }
    }

    final boolean offerTask(Runnable task) {
        if (isShutdown()) {
            reject();
        }
        return taskQueue.offer(task);
    }

    /**
     * @see Queue#remove(Object)
     */
    protected boolean removeTask(Runnable task) {
        return taskQueue.remove(ObjectUtil.checkNotNull(task, "task"));
    }

    /**
     * Poll all tasks from the task queue and run them via {@link Runnable#run()} method.
     * 执行所有任务
     *
     * @return {@code true} if and only if at least one task was run
     */
    protected boolean runAllTasks() {
        assert inEventLoop();
        boolean fetchedAll;
        boolean ranAtLeastOne = false;

        do {
            fetchedAll = fetchFromScheduledTaskQueue(taskQueue);
            if (runAllTasksFrom(taskQueue)) {
                ranAtLeastOne = true;
            }
        } while (!fetchedAll); // keep on processing until we fetched all scheduled tasks.

        if (ranAtLeastOne) {
            lastExecutionTime = getCurrentTimeNanos();
        }
        afterRunningAllTasks();
        return ranAtLeastOne;
    }

    /**
     * Execute all expired scheduled tasks and all current tasks in the executor queue until both queues are empty,
     * or {@code maxDrainAttempts} has been exceeded.
     * 执行所有定时任务和任务队列的任务
     *
     * @param maxDrainAttempts The maximum amount of times this method attempts to drain from queues. This is to prevent
     *                         continuous task execution and scheduling from preventing the EventExecutor thread to
     *                         make progress and return to the selector mechanism to process inbound I/O events.
     * @return {@code true} if at least one task was run.
     */
    protected final boolean runScheduledAndExecutorTasks(final int maxDrainAttempts) {
        assert inEventLoop();
        boolean ranAtLeastOneTask;
        int drainAttempt = 0;
        do {
            // We must run the taskQueue tasks first, because the scheduled tasks from outside the EventLoop are queued
            // here because the taskQueue is thread safe and the scheduledTaskQueue is not thread safe.
            ranAtLeastOneTask = runExistingTasksFrom(taskQueue) | executeExpiredScheduledTasks();
        } while (ranAtLeastOneTask && ++drainAttempt < maxDrainAttempts);

        if (drainAttempt > 0) {
            lastExecutionTime = getCurrentTimeNanos();
        }
        afterRunningAllTasks();

        return drainAttempt > 0;
    }

    /**
     * Runs all tasks from the passed {@code taskQueue}.
     * 执行指定任务队列的所有任务
     * 跳过WAKEUP_TASK 没有限制 直到队列为空才结束 全清
     *
     * @param taskQueue To poll and execute all tasks.
     * @return {@code true} if at least one task was executed.
     */
    protected final boolean runAllTasksFrom(Queue<Runnable> taskQueue) {
        Runnable task = pollTaskFrom(taskQueue);
        if (task == null) {
            return false;
        }
        for (; ; ) {
            safeExecute(task);
            task = pollTaskFrom(taskQueue);
            if (task == null) {
                return true;
            }
        }
    }

    /**
     * What ever tasks are present in {@code taskQueue} when this method is invoked will be {@link Runnable#run()}.
     * 执行队列中的方法
     * 不跳 WAKEUP_TASK 处理一批 不一定全清
     *
     * @param taskQueue the task queue to drain.
     * @return {@code true} if at least {@link Runnable#run()} was called.
     */
    private boolean runExistingTasksFrom(Queue<Runnable> taskQueue) {
        Runnable task = pollTaskFrom(taskQueue);
        if (task == null) {
            return false;
        }
        int remaining = Math.min(maxPendingTasks, taskQueue.size());
        safeExecute(task);
        // Use taskQueue.poll() directly rather than pollTaskFrom() since the latter may
        // silently consume more than one item from the queue (skips over WAKEUP_TASK instances)
        while (remaining-- > 0 && (task = taskQueue.poll()) != null) {
            safeExecute(task);
        }
        return true;
    }

    /**
     * Poll all tasks from the task queue and run them via {@link Runnable#run()} method.  This method stops running
     * the tasks in the task queue and returns if it ran longer than {@code timeoutNanos}.
     * 带超时的执行所有任务
     */
    protected boolean runAllTasks(long timeoutNanos) {
        fetchFromScheduledTaskQueue(taskQueue);
        Runnable task = pollTask();
        if (task == null) {
            afterRunningAllTasks();
            return false;
        }

        final long deadline = timeoutNanos > 0 ? getCurrentTimeNanos() + timeoutNanos : 0;
        long runTasks = 0;
        long lastExecutionTime;
        for (; ; ) {
            safeExecute(task);

            runTasks++;

            // Check timeout every 64 tasks because nanoTime() is relatively expensive.
            // XXX: Hard-coded value - will make it configurable if it is really a problem.
            if ((runTasks & 0x3F) == 0) {
                lastExecutionTime = getCurrentTimeNanos();
                if (lastExecutionTime >= deadline) {
                    break;
                }
            }

            task = pollTask();
            if (task == null) {
                lastExecutionTime = getCurrentTimeNanos();
                break;
            }
        }

        //钩子方法
        afterRunningAllTasks();
        this.lastExecutionTime = lastExecutionTime;
        return true;
    }

    /**
     * Invoked before returning from {@link #runAllTasks()} and {@link #runAllTasks(long)}.
     */
    protected void afterRunningAllTasks() {
    }

    /**
     * Returns the amount of time left until the scheduled task with the closest dead line is executed.
     * 计算当前时间距离最近的定时任务到期还有多久
     */
    protected long delayNanos(long currentTimeNanos) {
        currentTimeNanos -= ticker().initialNanoTime();

        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        if (scheduledTask == null) {
            return SCHEDULE_PURGE_INTERVAL;
        }

        return scheduledTask.delayNanos(currentTimeNanos);
    }

    /**
     * Returns the absolute point in time (relative to {@link #getCurrentTimeNanos()}) at which the next
     * closest scheduled task should run.
     * 当前时间距离最近的定时任务的到期时间
     */
    protected long deadlineNanos() {
        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        if (scheduledTask == null) {
            return getCurrentTimeNanos() + SCHEDULE_PURGE_INTERVAL;
        }
        return scheduledTask.deadlineNanos();
    }

    /**
     * Updates the internal timestamp that tells when a submitted task was executed most recently.
     * {@link #runAllTasks()} and {@link #runAllTasks(long)} updates this timestamp automatically, and thus there's
     * usually no need to call this method.  However, if you take the tasks manually using {@link #takeTask()} or
     * {@link #pollTask()}, you have to call this method at the end of task execution loop for accurate quiet period
     * checks.
     * 更新上次执行时间
     */
    protected void updateLastExecutionTime() {
        lastExecutionTime = getCurrentTimeNanos();
    }

    /**
     * Run the tasks in the {@link #taskQueue}
     * 执行taskQueue中的任务
     */
    protected abstract void run();

    /**
     * Do nothing, sub-classes may override
     * 清理方法 钩子
     */
    protected void cleanup() {
        // NOOP
    }

    //提交唤醒任务
    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop) {
            // Use offer as we actually only need this to unblock the thread and if offer fails we do not care as there
            // is already something in the queue.
            taskQueue.offer(WAKEUP_TASK);
        }
    }

    //是否在当前EventLoop
    @Override
    public boolean inEventLoop(Thread thread) {
        return thread == this.thread;
    }

    /**
     * Add a {@link Runnable} which will be executed on shutdown of this instance
     * 添加终止钩子
     */
    public void addShutdownHook(final Runnable task) {
        if (inEventLoop()) {
            shutdownHooks.add(task);
        } else {
            execute(new Runnable() {
                @Override
                public void run() {
                    shutdownHooks.add(task);
                }
            });
        }
    }

    /**
     * Remove a previous added {@link Runnable} as a shutdown hook
     * 移除终止钩子
     */
    public void removeShutdownHook(final Runnable task) {
        if (inEventLoop()) {
            shutdownHooks.remove(task);
        } else {
            execute(new Runnable() {
                @Override
                public void run() {
                    shutdownHooks.remove(task);
                }
            });
        }
    }

    //执行终止钩子
    private boolean runShutdownHooks() {
        boolean ran = false;
        // Note shutdown hooks can add / remove shutdown hooks.
        //钩子任务中可能添加新的任务
        while (!shutdownHooks.isEmpty()) {
            List<Runnable> copy = new ArrayList<Runnable>(shutdownHooks);
            shutdownHooks.clear();
            for (Runnable task : copy) {
                try {
                    runTask(task);
                } catch (Throwable t) {
                    logger.warn("Shutdown hook raised an exception.", t);
                } finally {
                    ran = true;
                }
            }
        }

        //至少执行过一次任务 更新时间
        if (ran) {
            lastExecutionTime = getCurrentTimeNanos();
        }

        return ran;
    }

    //shutdown
    private void shutdown0(long quietPeriod, long timeout, int shutdownState) {
        // 如果已经处于关闭过程中，直接返回
        if (isShuttingDown()) {
            return;
        }

        boolean inEventLoop = inEventLoop(); // 是否在 EventLoop 线程中
        boolean wakeup; // 是否需要唤醒线程
        int oldState;

        // CAS 设置新的状态，保证线程安全
        for (; ; ) {
            if (isShuttingDown()) {
                return;
            }
            int newState;
            wakeup = true;
            oldState = state;

            if (inEventLoop) {
                newState = shutdownState; // 当前线程是事件线程 直接设置新状态
            } else {
                switch (oldState) {
                    case ST_NOT_STARTED:
                    case ST_STARTED:
                    case ST_SUSPENDING:
                    case ST_SUSPENDED:
                        newState = shutdownState; // 可切换到 shutdown 状态
                        break;
                    default:
                        newState = oldState; // 状态不可变
                        wakeup = false; // 不需要唤醒
                }
            }

            // 尝试设置状态成功则退出循环
            if (STATE_UPDATER.compareAndSet(this, oldState, newState)) {
                break;
            }
        }

        // 设置优雅关闭的参数
        if (quietPeriod != -1) {
            gracefulShutdownQuietPeriod = quietPeriod;
        }
        if (timeout != -1) {
            gracefulShutdownTimeout = timeout;
        }

        // 如果线程还未启动，这里尝试启动线程
        if (ensureThreadStarted(oldState)) {
            return;
        }

        // 提交一个 WAKEUP_TASK 来唤醒 selector，避免卡在 select()
        if (wakeup) {
            taskQueue.offer(WAKEUP_TASK);
            if (!addTaskWakesUp) {
                wakeup(inEventLoop); // 主动唤醒 select()，通常调用 selector.wakeup()
            }
        }
    }

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        ObjectUtil.checkPositiveOrZero(quietPeriod, "quietPeriod");
        //超时时间不能小于静默期
        if (timeout < quietPeriod) {
            throw new IllegalArgumentException("timeout: " + timeout + " (expected >= quietPeriod (" + quietPeriod + "))");
        }
        ObjectUtil.checkNotNull(unit, "unit");
        //shutdown 静默期 超时时间 shutdown状态
        shutdown0(unit.toNanos(quietPeriod), unit.toNanos(timeout), ST_SHUTTING_DOWN);
        return terminationFuture();
    }

    @Override
    public Future<?> terminationFuture() {
        return terminationFuture;
    }

    @Override
    @Deprecated
    public void shutdown() {
        shutdown0(-1, -1, ST_SHUTDOWN);
    }

    @Override
    public boolean isShuttingDown() {
        return state >= ST_SHUTTING_DOWN;
    }

    @Override
    public boolean isShutdown() {
        return state >= ST_SHUTDOWN;
    }

    @Override
    public boolean isTerminated() {
        return state == ST_TERMINATED;
    }

    @Override
    public boolean isSuspended() {
        int currentState = state;
        return currentState == ST_SUSPENDED || currentState == ST_SUSPENDING;
    }

    @Override
    public boolean trySuspend() {
        if (supportSuspension) {
            if (STATE_UPDATER.compareAndSet(this, ST_STARTED, ST_SUSPENDING)) {
                wakeup(inEventLoop());
                return true;
            }
            int currentState = state;
            return currentState == ST_SUSPENDED || currentState == ST_SUSPENDING;
        }
        return false;
    }

    /**
     * Returns {@code true} if this {@link SingleThreadEventExecutor} can be suspended at the moment, {@code false}
     * otherwise.
     * 能否挂起
     *
     * @return if suspension is possible at the moment.
     */
    protected boolean canSuspend() {
        return canSuspend(state);
    }

    /**
     * Returns {@code true} if this {@link SingleThreadEventExecutor} can be suspended at the moment, {@code false}
     * otherwise.
     * <p>
     * Subclasses might override this method to add extra checks.
     *
     * @param state the current internal state of the {@link SingleThreadEventExecutor}.
     * @return if suspension is possible at the moment.
     */
    protected boolean canSuspend(int state) {
        assert inEventLoop();
        return supportSuspension && (state == ST_SUSPENDED || state == ST_SUSPENDING) && !hasTasks() && nextScheduledTaskDeadlineNanos() == -1;
    }

    /**
     * Confirm that the shutdown if the instance should be done now!
     * 确认终止
     */
    protected boolean confirmShutdown() {
        if (!isShuttingDown()) {
            return false;
        }

        if (!inEventLoop()) {
            throw new IllegalStateException("must be invoked from an event loop");
        }

        cancelScheduledTasks();

        if (gracefulShutdownStartTime == 0) {
            gracefulShutdownStartTime = getCurrentTimeNanos();
        }

        if (runAllTasks() || runShutdownHooks()) {
            if (isShutdown()) {
                // Executor shut down - no new tasks anymore.
                return true;
            }

            // There were tasks in the queue. Wait a little bit more until no tasks are queued for the quiet period or
            // terminate if the quiet period is 0.
            // See https://github.com/netty/netty/issues/4241
            if (gracefulShutdownQuietPeriod == 0) {
                return true;
            }
            taskQueue.offer(WAKEUP_TASK);
            return false;
        }

        final long nanoTime = getCurrentTimeNanos();

        if (isShutdown() || nanoTime - gracefulShutdownStartTime > gracefulShutdownTimeout) {
            return true;
        }

        if (nanoTime - lastExecutionTime <= gracefulShutdownQuietPeriod) {
            // Check if any tasks were added to the queue every 100ms.
            // TODO: Change the behavior of takeTask() so that it returns on timeout.
            taskQueue.offer(WAKEUP_TASK);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // Ignore
            }

            return false;
        }

        // No tasks were added for last quiet period - hopefully safe to shut down.
        // (Hopefully because we really cannot make a guarantee that there will be no execute() calls by a user.)
        return true;
    }

    //等待终止
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        ObjectUtil.checkNotNull(unit, "unit");
        if (inEventLoop()) {
            throw new IllegalStateException("cannot await termination of the current thread");
        }

        threadLock.await(timeout, unit);

        return isTerminated();
    }

    @Override
    public void execute(Runnable task) {
        execute0(task);
    }

    @Override
    public void lazyExecute(Runnable task) {
        lazyExecute0(task);
    }

    private void execute0(@Schedule Runnable task) {
        ObjectUtil.checkNotNull(task, "task");
        execute(task, wakesUpForTask(task));
    }

    private void lazyExecute0(@Schedule Runnable task) {
        execute(ObjectUtil.checkNotNull(task, "task"), false);
    }

    @Override
    void scheduleRemoveScheduled(final ScheduledFutureTask<?> task) {
        ObjectUtil.checkNotNull(task, "task");
        int currentState = state;
        if (supportSuspension && currentState == ST_SUSPENDED) {
            // In the case of scheduling for removal we need to also ensure we will recover the "suspend" state
            // after it if it was set before. Otherwise we will always end up "unsuspending" things on cancellation
            // which is not optimal.
            execute(new Runnable() {
                @Override
                public void run() {
                    task.run();
                    if (canSuspend(ST_SUSPENDED)) {
                        // Try suspending again to recover the state before we submitted the new task that will
                        // handle cancellation itself.
                        trySuspend();
                    }
                }
            }, true);
        } else {
            // task will remove itself from scheduled task queue when it runs
            execute(task, false);
        }
    }

    private void execute(Runnable task, boolean immediate) {
        boolean inEventLoop = inEventLoop(); // 当前线程是否是EventLoop线程
        addTask(task); // 添加任务到队列

        if (!inEventLoop) { // 如果是外部线程提交的任务
            startThread(); // 启动EventLoop线程 如果还没启动

            if (isShutdown()) { // 如果线程池已关闭
                boolean reject = false;
                try {
                    if (removeTask(task)) { // 如果能从队列中移除刚添加的任务
                        reject = true;
                    }
                } catch (UnsupportedOperationException e) {
                    // 队列不支持移除 可能是无锁队列 忽略异常
                }
                if (reject) {
                    reject(); // 拒绝执行该任务 抛出RejectedExecutionException等
                }
            }
        }

        // 如果设置了不自动唤醒 并且传入 immediate=true，主动唤醒 selector.select()
        if (!addTaskWakesUp && immediate) {
            wakeup(inEventLoop);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throwIfInEventLoop("invokeAny");
        return super.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throwIfInEventLoop("invokeAny");
        return super.invokeAny(tasks, timeout, unit);
    }

    @Override
    public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throwIfInEventLoop("invokeAll");
        return super.invokeAll(tasks);
    }

    @Override
    public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        throwIfInEventLoop("invokeAll");
        return super.invokeAll(tasks, timeout, unit);
    }

    private void throwIfInEventLoop(String method) {
        if (inEventLoop()) {
            throw new RejectedExecutionException("Calling " + method + " from within the EventLoop is not allowed");
        }
    }

    /**
     * Returns the {@link ThreadProperties} of the {@link Thread} that powers the {@link SingleThreadEventExecutor}.
     * If the {@link SingleThreadEventExecutor} is not started yet, this operation will start it and block until
     * it is fully started.
     */
    public final ThreadProperties threadProperties() {
        ThreadProperties threadProperties = this.threadProperties;
        if (threadProperties == null) {
            Thread thread = this.thread;
            if (thread == null) {
                assert !inEventLoop();
                //等待完成
                submit(NOOP_TASK).syncUninterruptibly();
                thread = this.thread;
                assert thread != null;
            }

            threadProperties = new DefaultThreadProperties(thread);
            if (!PROPERTIES_UPDATER.compareAndSet(this, null, threadProperties)) {
                threadProperties = this.threadProperties;
            }
        }

        return threadProperties;
    }

    /**
     * @deprecated override {@link SingleThreadEventExecutor#wakesUpForTask} to re-create this behaviour
     */
    @Deprecated
    protected interface NonWakeupRunnable extends LazyRunnable {
    }

    /**
     * Can be overridden to control which tasks require waking the {@link EventExecutor} thread
     * if it is waiting so that they can be run immediately.
     * 可以重写 让阻塞的EventLoop可以被唤醒然后立即执行
     */
    protected boolean wakesUpForTask(Runnable task) {
        return true;
    }

    //关闭的时候 拒绝 直接抛出异常
    protected static void reject() {
        throw new RejectedExecutionException("event executor terminated");
    }

    /**
     * Offers the task to the associated {@link RejectedExecutionHandler}.
     * 拒绝任务
     *
     * @param task to reject.
     */
    protected final void reject(Runnable task) {
        rejectedExecutionHandler.rejected(task, this);
    }

    // ScheduledExecutorService implementation

    private static final long SCHEDULE_PURGE_INTERVAL = TimeUnit.SECONDS.toNanos(1);

    private void startThread() {
        int currentState = state;
        if (currentState == ST_NOT_STARTED || currentState == ST_SUSPENDED) {
            if (STATE_UPDATER.compareAndSet(this, currentState, ST_STARTED)) {
                boolean success = false;
                try {
                    doStartThread();
                    success = true;
                } finally {
                    if (!success) {
                        STATE_UPDATER.compareAndSet(this, ST_STARTED, ST_NOT_STARTED);
                    }
                }
            }
        }
    }

    // 确保线程启动 有异常处理
    private boolean ensureThreadStarted(int oldState) {
        // 如果是未启动或挂起状态 尝试启动线程
        if (oldState == ST_NOT_STARTED || oldState == ST_SUSPENDED) {
            try {
                doStartThread(); // 启动线程
            } catch (Throwable cause) {
                // 如果启动线程失败 设置状态为终止
                STATE_UPDATER.set(this, ST_TERMINATED);
                //失败
                terminationFuture.tryFailure(cause);
                // 非 Exception 异常（如 OOM）要直接抛出
                if (!(cause instanceof Exception)) {
                    PlatformDependent.throwException(cause);
                }
                return true;
            }
        }
        return false;
    }

    //启动线程
    private void doStartThread() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                processingLock.lock(); // 加锁，防止多个线程并发进入
                assert thread == null;
                thread = Thread.currentThread(); // 设置当前线程为 eventLoop 线程
                if (interrupted) {
                    thread.interrupt(); // 恢复之前设置的中断标志
                    interrupted = false;
                }

                boolean success = false;
                boolean suspend = false;
                Throwable unexpectedException = null;

                updateLastExecutionTime(); // 更新最近一次执行时间

                try {
                    for (; ; ) {
                        SingleThreadEventExecutor.this.run(); // 执行主循环，处理任务
                        success = true;

                        int currentState = state;
                        if (canSuspend(currentState)) {
                            // 状态从 SUSPENDING -> SUSPENDED
                            if (!STATE_UPDATER.compareAndSet(SingleThreadEventExecutor.this, ST_SUSPENDING, ST_SUSPENDED)) {
                                continue; // 如果失败重试
                            }

                            // 如果此时有任务提交，则尝试恢复到 STARTED
                            if (!canSuspend(ST_SUSPENDED) && STATE_UPDATER.compareAndSet(SingleThreadEventExecutor.this, ST_SUSPENDED, ST_STARTED)) {
                                continue;
                            }

                            suspend = true;
                        }
                        break;
                    }
                } catch (Throwable t) {
                    unexpectedException = t;
                    logger.warn("Unexpected exception from an event executor: ", t);
                } finally {
                    boolean shutdown = !suspend;

                    // 如果不是 suspend，进入 shutdown 流程
                    if (shutdown) {
                        for (; ; ) {
                            int oldState = state;
                            if (oldState >= ST_SHUTTING_DOWN || STATE_UPDATER.compareAndSet(SingleThreadEventExecutor.this, oldState, ST_SHUTTING_DOWN)) {
                                break;
                            }
                        }

                        if (success && gracefulShutdownStartTime == 0) {
                            logger.error("Buggy EventExecutor: confirmShutdown() not called");
                        }
                    }

                    try {
                        if (shutdown) {
                            // 等待所有任务完成 + 安全关闭流程
                            for (; ; ) {
                                if (confirmShutdown()) {
                                    break;
                                }
                            }

                            for (; ; ) {
                                int currentState = state;
                                if (currentState >= ST_SHUTDOWN || STATE_UPDATER.compareAndSet(SingleThreadEventExecutor.this, currentState, ST_SHUTDOWN)) {
                                    break;
                                }
                            }

                            confirmShutdown(); // 最后一次确认

                            try {
                                cleanup(); // 清理资源
                            } finally {
                                FastThreadLocal.removeAll(); // 清除线程本地变量

                                STATE_UPDATER.set(SingleThreadEventExecutor.this, ST_TERMINATED);
                                threadLock.countDown(); // 标记线程已结束

                                int numUserTasks = drainTasks(); // 清理剩余任务
                                if (numUserTasks > 0) {
                                    logger.warn("terminated with non-empty task queue");
                                }

                                if (unexpectedException == null) {
                                    terminationFuture.setSuccess(null);
                                } else {
                                    terminationFuture.setFailure(unexpectedException);
                                }
                            }
                        } else {
                            // 挂起情况下清理线程本地变量
                            FastThreadLocal.removeAll();
                            threadProperties = null;
                        }
                    } finally {
                        thread = null;
                        processingLock.unlock(); // 解锁
                    }
                }
            }
        });
    }

    //清空任务队列
    final int drainTasks() {
        int numTasks = 0;
        for (; ; ) {
            Runnable runnable = taskQueue.poll(); // 从队列取一个任务
            if (runnable == null) {
                break; // 队列空则退出
            }
            // WAKEUP_TASK 是内部唤醒用的 不算用户任务
            if (WAKEUP_TASK != runnable) {
                numTasks++; // 统计用户任务数量
            }
        }
        return numTasks; // 返回剩余的用户任务数
    }

    private static final class DefaultThreadProperties implements ThreadProperties {
        private final Thread t;

        DefaultThreadProperties(Thread t) {
            this.t = t;
        }

        @Override
        public State state() {
            return t.getState();
        }

        @Override
        public int priority() {
            return t.getPriority();
        }

        @Override
        public boolean isInterrupted() {
            return t.isInterrupted();
        }

        @Override
        public boolean isDaemon() {
            return t.isDaemon();
        }

        @Override
        public String name() {
            return t.getName();
        }

        @Override
        public long id() {
            return t.getId();
        }

        @Override
        public StackTraceElement[] stackTrace() {
            return t.getStackTrace();
        }

        @Override
        public boolean isAlive() {
            return t.isAlive();
        }
    }
}
