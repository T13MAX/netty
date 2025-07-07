/*
 * Copyright 2015 The Netty Project
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

import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PriorityQueue;

import java.util.Comparator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class for {@link EventExecutor}s that want to support scheduling.
 * 支持Scheduled的EventExecutor抽象基类
 * 自管理定时任务队列 + 单线程驱动执行
 */
public abstract class AbstractScheduledEventExecutor extends AbstractEventExecutor {

    //任务比较器
    private static final Comparator<ScheduledFutureTask<?>> SCHEDULED_FUTURE_TASK_COMPARATOR =
            new Comparator<ScheduledFutureTask<?>>() {
                @Override
                public int compare(ScheduledFutureTask<?> o1, ScheduledFutureTask<?> o2) {
                    return o1.compareTo(o2);
                }
            };

    //唤醒任务 本身什么也不做 执行器可能因为队列为空而阻塞等待 这时候有任务被取消或者新任务早于最近任务 需要唤醒重新评估队列任务
    static final Runnable WAKEUP_TASK = new Runnable() {
        @Override
        public void run() {
        } // Do nothing
    };

    //优先队列 小顶堆 定时任务
    PriorityQueue<ScheduledFutureTask<?>> scheduledTaskQueue;

    //全局递增的唯一任务 ID 分配器
    long nextTaskId;

    protected AbstractScheduledEventExecutor() {
    }

    protected AbstractScheduledEventExecutor(EventExecutorGroup parent) {
        super(parent);
    }

    @Override
    public Ticker ticker() {
        return Ticker.systemTicker();
    }

    /**
     * Get the current time in nanoseconds by this executor's clock. This is not the same as {@link System#nanoTime()}
     * for two reasons:
     *
     * <ul>
     *     <li>We apply a fixed offset to the {@link System#nanoTime() nanoTime}</li>
     *     <li>Implementations (in particular EmbeddedEventLoop) may use their own time source so they can control time
     *     for testing purposes.</li>
     * </ul>
     *
     * @deprecated Please use (or override) {@link #ticker()} instead. This method delegates to {@link #ticker()}. Old
     * code may still call this method for compatibility.
     */
    @Deprecated
    protected long getCurrentTimeNanos() {
        return ticker().nanoTime();
    }

    /**
     * @deprecated Use the non-static {@link #ticker()} instead.
     */
    @Deprecated
    protected static long nanoTime() {
        return Ticker.systemTicker().nanoTime();
    }

    /**
     * @deprecated Use the non-static {@link #ticker()} instead.
     */
    @Deprecated
    static long defaultCurrentTimeNanos() {
        return Ticker.systemTicker().nanoTime();
    }

    //计算执行时间
    static long deadlineNanos(long nanoTime, long delay) {
        long deadlineNanos = nanoTime + delay;
        // Guard against overflow
        return deadlineNanos < 0 ? Long.MAX_VALUE : deadlineNanos;
    }

    /**
     * Given an arbitrary deadline {@code deadlineNanos}, calculate the number of nano seconds from now
     * {@code deadlineNanos} would expire.
     *
     * @param deadlineNanos An arbitrary deadline in nano seconds.
     * @return the number of nano seconds from now {@code deadlineNanos} would expire.
     * @deprecated Use {@link #ticker()} instead
     */
    @Deprecated
    protected static long deadlineToDelayNanos(long deadlineNanos) {
        return ScheduledFutureTask.deadlineToDelayNanos(defaultCurrentTimeNanos(), deadlineNanos);
    }

    /**
     * Returns the amount of time left until the scheduled task with the closest dead line is executed.
     * 计算当前时间距离最近一个计划任务的延迟时间
     */
    protected long delayNanos(long currentTimeNanos, long scheduledPurgeInterval) {

        //减去起始时间 获取相对于起始时间的偏移
        //这样做避免了系统纳秒时间的大数值带来的溢出或精度问题
        currentTimeNanos -= ticker().initialNanoTime();

        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        if (scheduledTask == null) {
            return scheduledPurgeInterval;
        }

        return scheduledTask.delayNanos(currentTimeNanos);
    }

    /**
     * The initial value used for delay and computations based upon a monatomic time source.
     *
     * @return initial value used for delay and computations based upon a monatomic time source.
     * @deprecated Use {@link #ticker()} instead
     */
    @Deprecated
    protected static long initialNanoTime() {
        return Ticker.systemTicker().initialNanoTime();
    }

    //定时任务队列
    PriorityQueue<ScheduledFutureTask<?>> scheduledTaskQueue() {
        if (scheduledTaskQueue == null) {
            //新建一个 (Netty自己实现的优先队列)
            scheduledTaskQueue = new DefaultPriorityQueue<ScheduledFutureTask<?>>(
                    SCHEDULED_FUTURE_TASK_COMPARATOR,
                    // Use same initial capacity as java.util.PriorityQueue
                    11);
        }
        return scheduledTaskQueue;
    }

    private static boolean isNullOrEmpty(Queue<ScheduledFutureTask<?>> queue) {
        return queue == null || queue.isEmpty();
    }

    /**
     * Cancel all scheduled tasks.
     * <p>
     * This method MUST be called only when {@link #inEventLoop()} is {@code true}.
     * 取消当前所有已调度但未执行的任务 必须在EventLoop里执行
     */
    protected void cancelScheduledTasks() {
        // 确保在 EventLoop 线程中执行 避免线程安全问题
        assert inEventLoop();

        PriorityQueue<ScheduledFutureTask<?>> scheduledTaskQueue = this.scheduledTaskQueue;

        // 如果任务队列为空  直接返回
        if (isNullOrEmpty(scheduledTaskQueue)) {
            return;
        }

        // 将任务队列转换为数组 避免在遍历时修改队列导致异常
        final ScheduledFutureTask<?>[] scheduledTasks = scheduledTaskQueue.toArray(new ScheduledFutureTask<?>[0]);

        // 遍历所有任务 调用取消但不从队列移除(避免遍历时修改结构)
        for (ScheduledFutureTask<?> task : scheduledTasks) {
            task.cancelWithoutRemove(false);
        }

        // 清空任务队列 同时忽略内部索引的更新（提升清空性能）
        scheduledTaskQueue.clearIgnoringIndexes();
    }

    /**
     * @see #pollScheduledTask(long)
     */
    protected final Runnable pollScheduledTask() {
        return pollScheduledTask(getCurrentTimeNanos());
    }

    /**
     * Fetch scheduled tasks from the internal queue and add these to the given {@link Queue}.
     * 获取定时任务到指定任务队列
     *
     * @param taskQueue the task queue into which the fetched scheduled tasks should be transferred.
     * @return {@code true} if we were able to transfer everything, {@code false} if we need to call this method again
     * as soon as there is space again in {@code taskQueue}.
     */
    protected boolean fetchFromScheduledTaskQueue(Queue<Runnable> taskQueue) {
        assert inEventLoop();
        Objects.requireNonNull(taskQueue, "taskQueue");
        if (scheduledTaskQueue == null || scheduledTaskQueue.isEmpty()) {
            return true;
        }
        long nanoTime = getCurrentTimeNanos();
        for (; ; ) {
            Runnable scheduledTask = pollScheduledTask(nanoTime);
            if (scheduledTask == null) {
                return true;
            }
            if (!taskQueue.offer(scheduledTask)) {
                // No space left in the task queue add it back to the scheduledTaskQueue so we pick it up again.
                //没空间了 放回去 重新取
                scheduledTaskQueue.add((ScheduledFutureTask<?>) scheduledTask);
                return false;
            }
        }
    }

    /**
     * Return the {@link Runnable} which is ready to be executed with the given {@code nanoTime}.
     * You should use {@link #getCurrentTimeNanos()} to retrieve the correct {@code nanoTime}.
     * 已经到执行时间的下一个要执行的任务
     */
    protected final Runnable pollScheduledTask(long nanoTime) {
        assert inEventLoop();

        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        if (scheduledTask == null || scheduledTask.deadlineNanos() - nanoTime > 0) {
            return null;
        }
        scheduledTaskQueue.remove();
        scheduledTask.setConsumed();
        return scheduledTask;
    }

    /**
     * Return the nanoseconds until the next scheduled task is ready to be run or {@code -1} if no task is scheduled.
     * 下一个任务执行的时间 减去了初始偏移
     */
    protected final long nextScheduledTaskNano() {
        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        return scheduledTask != null ? scheduledTask.delayNanos() : -1;
    }

    /**
     * Return the deadline (in nanoseconds) when the next scheduled task is ready to be run or {@code -1}
     * if no task is scheduled.
     * 下一个任务执行的时间 返回原始时间戳
     */
    protected final long nextScheduledTaskDeadlineNanos() {
        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        return scheduledTask != null ? scheduledTask.deadlineNanos() : -1;
    }

    final ScheduledFutureTask<?> peekScheduledTask() {
        Queue<ScheduledFutureTask<?>> scheduledTaskQueue = this.scheduledTaskQueue;
        return scheduledTaskQueue != null ? scheduledTaskQueue.peek() : null;
    }

    /**
     * Returns {@code true} if a scheduled task is ready for processing.
     * 是否有到时间了可以执行的任务
     */
    protected final boolean hasScheduledTasks() {
        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        return scheduledTask != null && scheduledTask.deadlineNanos() <= getCurrentTimeNanos();
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        ObjectUtil.checkNotNull(command, "command");
        ObjectUtil.checkNotNull(unit, "unit");
        if (delay < 0) {
            delay = 0;
        }
        //这个方法废弃了
        validateScheduled0(delay, unit);

        //用Netty的定时任务包装
        return schedule(new ScheduledFutureTask<Void>(
                this,
                command,
                deadlineNanos(getCurrentTimeNanos(), unit.toNanos(delay))));
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        ObjectUtil.checkNotNull(callable, "callable");
        ObjectUtil.checkNotNull(unit, "unit");
        if (delay < 0) {
            delay = 0;
        }
        validateScheduled0(delay, unit);

        return schedule(new ScheduledFutureTask<V>(this, callable, deadlineNanos(getCurrentTimeNanos(), unit.toNanos(delay))));
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        ObjectUtil.checkNotNull(command, "command");
        ObjectUtil.checkNotNull(unit, "unit");
        if (initialDelay < 0) {
            throw new IllegalArgumentException(String.format("initialDelay: %d (expected: >= 0)", initialDelay));
        }
        if (period <= 0) {
            throw new IllegalArgumentException(String.format("period: %d (expected: > 0)", period));
        }
        validateScheduled0(initialDelay, unit);
        validateScheduled0(period, unit);

        return schedule(new ScheduledFutureTask<Void>(this, command, deadlineNanos(getCurrentTimeNanos(), unit.toNanos(initialDelay)), unit.toNanos(period)));
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        ObjectUtil.checkNotNull(command, "command");
        ObjectUtil.checkNotNull(unit, "unit");
        if (initialDelay < 0) {
            throw new IllegalArgumentException(String.format("initialDelay: %d (expected: >= 0)", initialDelay));
        }
        if (delay <= 0) {
            throw new IllegalArgumentException(String.format("delay: %d (expected: > 0)", delay));
        }

        validateScheduled0(initialDelay, unit);
        validateScheduled0(delay, unit);

        return schedule(new ScheduledFutureTask<Void>(this, command, deadlineNanos(getCurrentTimeNanos(), unit.toNanos(initialDelay)), -unit.toNanos(delay)));
    }

    @SuppressWarnings("deprecation")
    private void validateScheduled0(long amount, TimeUnit unit) {
        validateScheduled(amount, unit);
    }

    /**
     * Sub-classes may override this to restrict the maximal amount of time someone can use to schedule a task.
     *
     * @deprecated will be removed in the future.
     */
    @Deprecated
    protected void validateScheduled(long amount, TimeUnit unit) {
        // NOOP
    }

    //提交到任务队列
    final void scheduleFromEventLoop(final ScheduledFutureTask<?> task) {
        // nextTaskId a long and so there is no chance it will overflow back to 0
        scheduledTaskQueue().add(task.setId(++nextTaskId));
    }

    //提交一个定时任务到调度队列中 返回ScheduledFuture
    private <V> ScheduledFuture<V> schedule(final ScheduledFutureTask<V> task) {
        if (inEventLoop()) {
            //从EventLoop内执行任务
            scheduleFromEventLoop(task);
        } else {
            final long deadlineNanos = task.deadlineNanos();
            // task will add itself to scheduled task queue when run if not expired
            //如果任务尚未到期，则正常执行 task.run() 会判断是否过期 并将自己加入 scheduledTaskQueue
            if (beforeScheduledTaskSubmitted(deadlineNanos)) {
                //立即提交任务
                execute(task);
            } else {
                //如果已经过期或快过期，懒执行 非阻塞加入
                lazyExecute(task);
                // Second hook after scheduling to facilitate race-avoidance
                if (afterScheduledTaskSubmitted(deadlineNanos)) {
                    //Netty 的 EventLoop 是单线程的 可能正在 select() 等待IO事件 这时候你从别的线程添加了一个任务它可能不会立刻执行
                    //WAKEUP_TASK 打断 select()（底层实现中调用 wakeup()）
                    //让 EventLoop 重新进入 runAllTasks()，尽快处理新任务
                    execute(WAKEUP_TASK);
                }
            }
        }

        return task;
    }

    final void removeScheduled(final ScheduledFutureTask<?> task) {
        assert task.isCancelled();
        if (inEventLoop()) {
            scheduledTaskQueue().removeTyped(task);
        } else {
            // task will remove itself from scheduled task queue when it runs
            scheduleRemoveScheduled(task);
        }
    }

    void scheduleRemoveScheduled(final ScheduledFutureTask<?> task) {
        // task will remove itself from scheduled task queue when it runs
        lazyExecute(task);
    }

    /**
     * Called from arbitrary non-{@link EventExecutor} threads prior to scheduled task submission.
     * Returns {@code true} if the {@link EventExecutor} thread should be woken immediately to
     * process the scheduled task (if not already awake).
     * <p>
     * If {@code false} is returned, {@link #afterScheduledTaskSubmitted(long)} will be called with
     * the same value <i>after</i> the scheduled task is enqueued, providing another opportunity
     * to wake the {@link EventExecutor} thread if required.
     * 钩子方法 在任务提交前判断是否需要继续正常调度该任务
     *
     * @param deadlineNanos deadline of the to-be-scheduled task
     *                      relative to {@link AbstractScheduledEventExecutor#getCurrentTimeNanos()}
     * @return {@code true} if the {@link EventExecutor} thread should be woken, {@code false} otherwise
     */
    protected boolean beforeScheduledTaskSubmitted(long deadlineNanos) {
        return true;
    }

    /**
     * See {@link #beforeScheduledTaskSubmitted(long)}. Called only after that method returns false.
     * 钩子方法 任务提交后
     *
     * @param deadlineNanos relative to {@link AbstractScheduledEventExecutor#getCurrentTimeNanos()}
     * @return {@code true} if the {@link EventExecutor} thread should be woken, {@code false} otherwise
     */
    protected boolean afterScheduledTaskSubmitted(long deadlineNanos) {
        return true;
    }
}
