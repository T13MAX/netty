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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The {@link EventExecutorGroup} is responsible for providing the {@link EventExecutor}'s to use
 * via its {@link #next()} method. Besides this, it is also responsible for handling their
 * life-cycle and allows shutting them down in a global fashion.
 * 线程池组 负责管理多个EventExecutor（具体的执行线程）用于处理事件（如I/O、定时任务等）
 * 协变返回类型 对ScheduledExecutorService进行了增强
 */
public interface EventExecutorGroup extends ScheduledExecutorService, Iterable<EventExecutor> {

    /**
     * Returns {@code true} if and only if all {@link EventExecutor}s managed by this {@link EventExecutorGroup}
     * are being {@linkplain #shutdownGracefully() shut down gracefully} or was {@linkplain #isShutdown() shut down}.
     * 判断当前 EventExecutorGroup 是否正在优雅关闭或已关闭
     */
    boolean isShuttingDown();

    /**
     * Shortcut method for {@link #shutdownGracefully(long, long, TimeUnit)} with sensible default values.
     * 优雅关闭执行器组 使用默认的 quietPeriod 和 timeout 时间
     *
     * @return the {@link #terminationFuture()}
     */
    Future<?> shutdownGracefully();

    /**
     * Signals this executor that the caller wants the executor to be shut down.  Once this method is called,
     * {@link #isShuttingDown()} starts to return {@code true}, and the executor prepares to shut itself down.
     * Unlike {@link #shutdown()}, graceful shutdown ensures that no tasks are submitted for <i>'the quiet period'</i>
     * (usually a couple seconds) before it shuts itself down.  If a task is submitted during the quiet period,
     * it is guaranteed to be accepted and the quiet period will start over.
     * 优雅关闭执行器组，执行器不会立即关闭，而是等待一段安静期
     * 如果这段时间没有新任务提交 则开始关闭
     * 如果有新任务 则重新计算安静期
     * 超过 timeout 后无论如何都会关闭
     *
     * @param quietPeriod the quiet period as described in the documentation
     * @param timeout     the maximum amount of time to wait until the executor is {@linkplain #shutdown()}
     *                    regardless if a task was submitted during the quiet period
     * @param unit        the unit of {@code quietPeriod} and {@code timeout}
     * @return the {@link #terminationFuture()}
     */
    Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit);

    /**
     * Returns the {@link Future} which is notified when all {@link EventExecutor}s managed by this
     * {@link EventExecutorGroup} have been terminated.
     * 返回一个 Future，当所有执行器终止时，该 Future 完成。
     */
    Future<?> terminationFuture();

    /**
     * @deprecated {@link #shutdownGracefully(long, long, TimeUnit)} or {@link #shutdownGracefully()} instead.
     * 强制立即关闭（已废弃，不推荐使用）。
     */
    @Override
    @Deprecated
    void shutdown();

    /**
     * 强制立即关闭并返回尚未执行的任务列表（已废弃，不推荐使用）。
     *
     * @deprecated {@link #shutdownGracefully(long, long, TimeUnit)} or {@link #shutdownGracefully()} instead.
     */
    @Override
    @Deprecated
    List<Runnable> shutdownNow();

    /**
     * Returns one of the {@link EventExecutor}s managed by this {@link EventExecutorGroup}.
     * 返回一个执行器 轮询分配任务
     */
    EventExecutor next();

    //获取当前执行器组的迭代器
    @Override
    Iterator<EventExecutor> iterator();

    //提交一个任务 返回 Netty 的 Future
    @Override
    Future<?> submit(Runnable task);

    //提交一个任务 指定返回结果 返回 Netty 的 Future
    @Override
    <T> Future<T> submit(Runnable task, T result);

    //提交一个带返回值的任务，返回 Netty 的 Future
    @Override
    <T> Future<T> submit(Callable<T> task);

    /**
     * The ticker for this executor. Usually the {@link #schedule} methods will follow the
     * {@link Ticker#systemTicker() system ticker} (i.e. {@link System#nanoTime()}), but especially for testing it is
     * sometimes useful to have more control over the ticker. In that case, this method will be overridden. Code that
     * schedules tasks on this executor should use this ticker in order to stay consistent with the executor (e.g. not
     * be surprised by scheduled tasks running "early").
     * 返回当前使用的 Ticker（一般为 System.nanoTime()）
     * 可在测试时重写以使用自定义时间源
     *
     * @return The ticker for this scheduler
     */
    default Ticker ticker() {
        return Ticker.systemTicker();
    }

    //调度延迟执行的任务
    @Override
    ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    //调度延迟执行的带返回值任务
    @Override
    <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit);

    //以固定频率调度任务 任务开始时间固定
    @Override
    ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

    //以固定延迟调度任务 任务结束后延迟一段时间再执行下一次
    @Override
    ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit);
}
