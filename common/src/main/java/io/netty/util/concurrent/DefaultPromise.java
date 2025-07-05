/*
 * Copyright 2013 The Netty Project
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

import io.netty.util.internal.InternalThreadLocalMap;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThrowableUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static io.netty.util.internal.ObjectUtil.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Promise默认实现
 *
 * @Author: t13max
 * @Since: 8:24 2025/7/4
 */
public class DefaultPromise<V> extends AbstractFuture<V> implements Promise<V> {

    /**
     * System property with integer type value, that determine the max reentrancy/recursion level for when
     * listener notifications prompt other listeners to be notified.
     * <p>
     * When the reentrancy/recursion level becomes greater than this number, a new task will instead be scheduled
     * on the event loop, to finish notifying any subsequent listners.
     * <p>
     * The default value is {@code 8}.
     */
    // 监听器最大嵌套深度，防止 StackOverflow
    public static final String PROPERTY_MAX_LISTENER_STACK_DEPTH = "io.netty.defaultPromise.maxListenerStackDepth";

    // 日志记录器
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultPromise.class);
    // 当执行监听器任务被拒绝时使用的日志记录器
    private static final InternalLogger rejectedExecutionLogger = InternalLoggerFactory.getInstance(DefaultPromise.class.getName() + ".rejectedExecution");
    // 监听器最大调用栈深度（默认 8），超过后会提交到 EventLoop 执行，避免递归死循环
    private static final int MAX_LISTENER_STACK_DEPTH = Math.min(8, SystemPropertyUtil.getInt(PROPERTY_MAX_LISTENER_STACK_DEPTH, 8));
    // 原子更新 result 字段 用于多线程下设置 Promise 的完成结果 防御性编程 取消和完成竞争 超时和完成竞争 用户逻辑错误
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<DefaultPromise, Object> RESULT_UPDATER = AtomicReferenceFieldUpdater.newUpdater(DefaultPromise.class, Object.class, "result");
    // 表示成功的常量对象
    private static final Object SUCCESS = new Object();
    // 表示不可取消的常量对象
    private static final Object UNCANCELLABLE = new Object();
    // 表示被取消的结果包装，包含异常信息
    private static final CauseHolder CANCELLATION_CAUSE_HOLDER = new CauseHolder(StacklessCancellationException.newInstance(DefaultPromise.class, "cancel(...)"));
    // 取消时使用的预定义栈信息，减少内存分配
    private static final StackTraceElement[] CANCELLATION_STACK = CANCELLATION_CAUSE_HOLDER.cause.getStackTrace();

    //结果
    private volatile Object result;
    //执行回调的线程
    private final EventExecutor executor;

    /**
     * One or more listeners. Can be a {@link GenericFutureListener} or a {@link DefaultFutureListeners}.
     * If {@code null}, it means either 1) no listeners were added yet or 2) all listeners were notified.
     * <p>
     * Threading - synchronized(this). We must support adding listeners when there is no EventExecutor.
     * 单个监听器，用于优化只注册一个监听器的情况
     */
    private GenericFutureListener<? extends Future<?>> listener;
    //多个监听器，用于注册多个监听器时使用
    private DefaultFutureListeners listeners;

    /**
     * Threading - synchronized(this). We are required to hold the monitor to use Java's underlying wait()/notifyAll().
     * /等待在该 Promise 上阻塞等待结果的线程数量
     */
    private short waiters;

    /**
     * Threading - synchronized(this). We must prevent concurrent notification and FIFO listener notification if the
     * executor changes.
     * 标记当前是否正在通知监听器，防止重复或并发通知
     */
    private boolean notifyingListeners;

    /**
     * Creates a new instance.
     * <p>
     * It is preferable to use {@link EventExecutor#newPromise()} to create a new promise
     *
     * @param executor the {@link EventExecutor} which is used to notify the promise once it is complete.
     *                 It is assumed this executor will protect against {@link StackOverflowError} exceptions.
     *                 The executor may be used to avoid {@link StackOverflowError} by executing a {@link Runnable} if the stack
     *                 depth exceeds a threshold.
     */
    public DefaultPromise(EventExecutor executor) {
        this.executor = checkNotNull(executor, "executor");
    }

    /**
     * See {@link #executor()} for expectations of the executor.
     */
    protected DefaultPromise() {
        // only for subclasses
        executor = null;
    }

    @Override
    public Promise<V> setSuccess(V result) {
        if (setSuccess0(result)) {
            return this;
        }
        throw new IllegalStateException("complete already: " + this);
    }

    @Override
    public boolean trySuccess(V result) {
        return setSuccess0(result);
    }

    @Override
    public Promise<V> setFailure(Throwable cause) {
        if (setFailure0(cause)) {
            return this;
        }
        throw new IllegalStateException("complete already: " + this, cause);
    }

    @Override
    public boolean tryFailure(Throwable cause) {
        return setFailure0(cause);
    }

    @Override
    public boolean setUncancellable() {
        //把不可取消设置在result上 真正的结果来了会cas设置结果
        if (RESULT_UPDATER.compareAndSet(this, null, UNCANCELLABLE)) {
            return true;
        }
        Object result = this.result;
        //没完成 没取消
        return !isDone0(result) || !isCancelled0(result);
    }

    @Override
    public boolean isSuccess() {
        Object result = this.result;
        return result != null && result != UNCANCELLABLE && !(result instanceof CauseHolder);
    }

    @Override
    public boolean isCancellable() {
        //为空默认可取消 因为不可取消的时候会把UNCANCELLABLE设置在result上 如果真正的结果来了和=或异常了 那就是完事了 也不可取消
        return result == null;
    }

    //轻量级 任务被取消了异常
    private static final class LeanCancellationException extends CancellationException {

        private static final long serialVersionUID = 2794674970981187807L;

        // Suppress a warning since the method doesn't need synchronization
        //重写 直接设置静态的栈信息 避免频繁构建栈信息
        @Override
        public Throwable fillInStackTrace() {
            setStackTrace(CANCELLATION_STACK);
            return this;
        }

        @Override
        public String toString() {
            return CancellationException.class.getName();
        }
    }

    @Override
    public Throwable cause() {
        //返回异常
        return cause0(result);
    }

    private Throwable cause0(Object result) {
        //判断类型是否正确
        if (!(result instanceof CauseHolder)) {
            return null;
        }
        //取消异常 用精简版包裹返回
        if (result == CANCELLATION_CAUSE_HOLDER) {
            CancellationException ce = new LeanCancellationException();
            if (RESULT_UPDATER.compareAndSet(this, CANCELLATION_CAUSE_HOLDER, new CauseHolder(ce))) {
                return ce;
            }
            result = this.result;
        }
        //返回原始异常
        return ((CauseHolder) result).cause;
    }

    @Override
    public Promise<V> addListener(GenericFutureListener<? extends Future<? super V>> listener) {
        checkNotNull(listener, "listener");

        synchronized (this) {
            addListener0(listener);
        }

        if (isDone()) {
            notifyListeners();
        }

        return this;
    }

    @Override
    public Promise<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners) {
        //判空
        checkNotNull(listeners, "listeners");
        //锁住
        synchronized (this) {
            for (GenericFutureListener<? extends Future<? super V>> listener : listeners) {
                if (listener == null) {
                    break;
                }
                //添加
                addListener0(listener);
            }
        }
        //是否完成
        if (isDone()) {
            //通知
            notifyListeners();
        }
        return this;
    }

    @Override
    public Promise<V> removeListener(final GenericFutureListener<? extends Future<? super V>> listener) {
        checkNotNull(listener, "listener");

        synchronized (this) {
            removeListener0(listener);
        }

        return this;
    }

    @Override
    public Promise<V> removeListeners(final GenericFutureListener<? extends Future<? super V>>... listeners) {
        checkNotNull(listeners, "listeners");

        synchronized (this) {
            for (GenericFutureListener<? extends Future<? super V>> listener : listeners) {
                if (listener == null) {
                    break;
                }
                removeListener0(listener);
            }
        }

        return this;
    }

    @Override
    public Promise<V> await() throws InterruptedException {
        //任务已完成直接返回
        if (isDone()) {
            return this;
        }
        //如果线程已被中断 抛出中断异常
        if (Thread.interrupted()) {
            throw new InterruptedException(toString());
        }

        //检测死锁 比如在EventLoop里阻塞
        checkDeadLock();

        //wait() 和 notifyAll() 是 Java 对象的监视器方法，必须在持有该对象的锁时才能调用
        synchronized (this) {
            while (!isDone()) {
                //递增等待者
                incWaiters();
                try {
                    //等待 使用 wait/notify 已经够用 不需要ReentrantLock那一套
                    wait();
                } finally {
                    //递减
                    decWaiters();
                }
            }
        }
        return this;
    }

    //不可中断等待
    @Override
    public Promise<V> awaitUninterruptibly() {
        // 如果已经完成，直接返回
        if (isDone()) {
            return this;
        }

        // 检查是否存在死锁（例如在 EventLoop 线程中调用阻塞方法）
        checkDeadLock();
        // 记录是否被中断过
        boolean interrupted = false;
        synchronized (this) {
            while (!isDone()) {
                incWaiters();
                try {
                    wait();
                } catch (InterruptedException e) {
                    //吞掉异常 不能中断
                    interrupted = true;
                } finally {
                    decWaiters();
                }
            }
        }

        // 恢复中断状态，让上层知道线程曾被中断过
        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        return this;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return await0(unit.toNanos(timeout), true);
    }

    @Override
    public boolean await(long timeoutMillis) throws InterruptedException {
        return await0(MILLISECONDS.toNanos(timeoutMillis), true);
    }

    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
        try {
            return await0(unit.toNanos(timeout), false);
        } catch (InterruptedException e) {
            // Should not be raised at all.
            // 如果走到这 那就是有bug 是个不该发生的错误
            throw new InternalError();
        }
    }

    @Override
    public boolean awaitUninterruptibly(long timeoutMillis) {
        try {
            return await0(MILLISECONDS.toNanos(timeoutMillis), false);
        } catch (InterruptedException e) {
            // Should not be raised at all.
            throw new InternalError();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V getNow() {
        Object result = this.result;
        if (result instanceof CauseHolder || result == SUCCESS || result == UNCANCELLABLE) {
            return null;
        }
        return (V) result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get() throws InterruptedException, ExecutionException {
        Object result = this.result;
        if (!isDone0(result)) {
            await();
            result = this.result;
        }
        if (result == SUCCESS || result == UNCANCELLABLE) {
            return null;
        }
        Throwable cause = cause0(result);
        if (cause == null) {
            return (V) result;
        }
        if (cause instanceof CancellationException) {
            throw (CancellationException) cause;
        }
        throw new ExecutionException(cause);
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        Object result = this.result;
        if (!isDone0(result)) {
            if (!await(timeout, unit)) {
                throw new TimeoutException();
            }
            result = this.result;
        }
        if (result == SUCCESS || result == UNCANCELLABLE) {
            return null;
        }
        Throwable cause = cause0(result);
        if (cause == null) {
            return (V) result;
        }
        if (cause instanceof CancellationException) {
            throw (CancellationException) cause;
        }
        throw new ExecutionException(cause);
    }

    /**
     * {@inheritDoc}
     *
     * @param mayInterruptIfRunning this value has no effect in this implementation.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (RESULT_UPDATER.compareAndSet(this, null, CANCELLATION_CAUSE_HOLDER)) {
            //有线程在等待 则唤醒
            if (checkNotifyWaiters()) {
                //通知listener promise的状态变化
                notifyListeners();
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean isCancelled() {
        return isCancelled0(result);
    }

    @Override
    public boolean isDone() {
        return isDone0(result);
    }

    @Override
    public Promise<V> sync() throws InterruptedException {
        await();
        rethrowIfFailed();
        return this;
    }

    @Override
    public Promise<V> syncUninterruptibly() {
        awaitUninterruptibly();
        rethrowIfFailed();
        return this;
    }

    @Override
    public String toString() {
        return toStringBuilder().toString();
    }

    protected StringBuilder toStringBuilder() {
        StringBuilder buf = new StringBuilder(64)
                .append(StringUtil.simpleClassName(this))
                .append('@')
                .append(Integer.toHexString(hashCode()));

        Object result = this.result;
        if (result == SUCCESS) {
            buf.append("(success)");
        } else if (result == UNCANCELLABLE) {
            buf.append("(uncancellable)");
        } else if (result instanceof CauseHolder) {
            buf.append("(failure: ")
                    .append(((CauseHolder) result).cause)
                    .append(')');
        } else if (result != null) {
            buf.append("(success: ")
                    .append(result)
                    .append(')');
        } else {
            buf.append("(incomplete)");
        }

        return buf;
    }

    /**
     * Get the executor used to notify listeners when this promise is complete.
     * <p>
     * It is assumed this executor will protect against {@link StackOverflowError} exceptions.
     * The executor may be used to avoid {@link StackOverflowError} by executing a {@link Runnable} if the stack
     * depth exceeds a threshold.
     *
     * @return The executor used to notify listeners when this promise is complete.
     * 获取用于在此 promise 完成时通知侦听器的 executor
     */
    protected EventExecutor executor() {
        return executor;
    }

    //检测死锁
    protected void checkDeadLock() {
        EventExecutor e = executor();
        //在EventLoop里阻塞
        if (e != null && e.inEventLoop()) {
            throw new BlockingOperationException(toString());
        }
    }

    /**
     * Notify a listener that a future has completed.
     * <p>
     * This method has a fixed depth of {@link #MAX_LISTENER_STACK_DEPTH} that will limit recursion to prevent
     * {@link StackOverflowError} and will stop notifying listeners added after this threshold is exceeded.
     *
     * @param eventExecutor the executor to use to notify the listener {@code listener}.
     * @param future        the future that is complete.
     * @param listener      the listener to notify.
     */
    protected static void notifyListener(EventExecutor eventExecutor, final Future<?> future, final GenericFutureListener<?> listener) {
        notifyListenerWithStackOverFlowProtection(checkNotNull(eventExecutor, "eventExecutor"), checkNotNull(future, "future"), checkNotNull(listener, "listener"));
    }

    //添加listener cancel setValue 都会调用
    private void notifyListeners() {
        EventExecutor executor = executor();
        if (executor.inEventLoop()) {
            final InternalThreadLocalMap threadLocals = InternalThreadLocalMap.get();
            final int stackDepth = threadLocals.futureListenerStackDepth();
            if (stackDepth < MAX_LISTENER_STACK_DEPTH) {
                threadLocals.setFutureListenerStackDepth(stackDepth + 1);
                try {
                    notifyListenersNow();
                } finally {
                    threadLocals.setFutureListenerStackDepth(stackDepth);
                }
                return;
            }
        }

        safeExecute(executor, new Runnable() {
            @Override
            public void run() {
                notifyListenersNow();
            }
        });
    }

    /**
     * The logic in this method should be identical to {@link #notifyListeners()} but
     * cannot share code because the listener(s) cannot be cached for an instance of {@link DefaultPromise} since the
     * listener(s) may be changed and is protected by a synchronized operation.
     */
    private static void notifyListenerWithStackOverFlowProtection(final EventExecutor executor, final Future<?> future, final GenericFutureListener<?> listener) {
        if (executor.inEventLoop()) {
            final InternalThreadLocalMap threadLocals = InternalThreadLocalMap.get();
            final int stackDepth = threadLocals.futureListenerStackDepth();
            if (stackDepth < MAX_LISTENER_STACK_DEPTH) {
                threadLocals.setFutureListenerStackDepth(stackDepth + 1);
                try {
                    notifyListener0(future, listener);
                } finally {
                    threadLocals.setFutureListenerStackDepth(stackDepth);
                }
                return;
            }
        }

        safeExecute(executor, new Runnable() {
            @Override
            public void run() {
                notifyListener0(future, listener);
            }
        });
    }

    private void notifyListenersNow() {
        GenericFutureListener listener;
        DefaultFutureListeners listeners;
        synchronized (this) {
            listener = this.listener;
            listeners = this.listeners;
            // Only proceed if there are listeners to notify and we are not already notifying listeners.
            if (notifyingListeners || (listener == null && listeners == null)) {
                return;
            }
            notifyingListeners = true;
            if (listener != null) {
                this.listener = null;
            } else {
                this.listeners = null;
            }
        }
        for (; ; ) {
            if (listener != null) {
                notifyListener0(this, listener);
            } else {
                notifyListeners0(listeners);
            }
            synchronized (this) {
                if (this.listener == null && this.listeners == null) {
                    // Nothing can throw from within this method, so setting notifyingListeners back to false does not
                    // need to be in a finally block.
                    notifyingListeners = false;
                    return;
                }
                listener = this.listener;
                listeners = this.listeners;
                if (listener != null) {
                    this.listener = null;
                } else {
                    this.listeners = null;
                }
            }
        }
    }

    private void notifyListeners0(DefaultFutureListeners listeners) {
        GenericFutureListener<?>[] a = listeners.listeners();
        int size = listeners.size();
        for (int i = 0; i < size; i++) {
            notifyListener0(this, a[i]);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void notifyListener0(Future future, GenericFutureListener l) {
        try {
            l.operationComplete(future);
        } catch (Throwable t) {
            if (logger.isWarnEnabled()) {
                logger.warn("An exception was thrown by " + l.getClass().getName() + ".operationComplete()", t);
            }
        }
    }

    private void addListener0(GenericFutureListener<? extends Future<? super V>> listener) {
        if (this.listener == null) {
            if (listeners == null) {
                this.listener = listener;
            } else {
                listeners.add(listener);
            }
        } else {
            assert listeners == null;
            listeners = new DefaultFutureListeners(this.listener, listener);
            this.listener = null;
        }
    }

    private void removeListener0(GenericFutureListener<? extends Future<? super V>> toRemove) {
        if (listener == toRemove) {
            listener = null;
        } else if (listeners != null) {
            listeners.remove(toRemove);
            // Removal is rare, no need for compaction
            if (listeners.size() == 0) {
                listeners = null;
            }
        }
    }

    private boolean setSuccess0(V result) {
        return setValue0(result == null ? SUCCESS : result);
    }

    private boolean setFailure0(Throwable cause) {
        return setValue0(new CauseHolder(checkNotNull(cause, "cause")));
    }

    private boolean setValue0(Object objResult) {
        if (RESULT_UPDATER.compareAndSet(this, null, objResult) ||
                RESULT_UPDATER.compareAndSet(this, UNCANCELLABLE, objResult)) {
            if (checkNotifyWaiters()) {
                notifyListeners();
            }
            return true;
        }
        return false;
    }

    /**
     * Check if there are any waiters and if so notify these.
     *
     * @return {@code true} if there are any listeners attached to the promise, {@code false} otherwise.
     */
    private synchronized boolean checkNotifyWaiters() {
        if (waiters > 0) {
            notifyAll();
        }
        return listener != null || listeners != null;
    }

    private void incWaiters() {
        if (waiters == Short.MAX_VALUE) {
            throw new IllegalStateException("too many waiters: " + this);
        }
        ++waiters;
    }

    private void decWaiters() {
        --waiters;
    }

    private void rethrowIfFailed() {
        Throwable cause = cause();
        if (cause == null) {
            return;
        }

        if (!(cause instanceof CancellationException) && cause.getSuppressed().length == 0) {
            cause.addSuppressed(new CompletionException("Rethrowing promise failure cause", null));
        }
        PlatformDependent.throwException(cause);
    }

    // 等待 Promise 完成，支持是否响应中断 + 超时时间控制
    private boolean await0(long timeoutNanos, boolean interruptable) throws InterruptedException {
        // 如果已完成，直接返回
        if (isDone()) {
            return true;
        }

        // 如果传入的超时时间为 0 或负数，直接检查 isDone 并返回
        if (timeoutNanos <= 0) {
            return isDone();
        }

        // 如果允许中断且当前线程已中断，抛出 InterruptedException
        if (interruptable && Thread.interrupted()) {
            throw new InterruptedException(toString());
        }

        // 检查是否可能死锁
        checkDeadLock();

        // 记录开始等待的时间（用于后续计算剩余时间）
        final long startTime = System.nanoTime();

        // 同步块保护 wait/notify 机制
        synchronized (this) {
            boolean interrupted = false;
            try {
                long waitTime = timeoutNanos;

                // 如果未完成，且剩余等待时间大于 0，则继续等待
                while (!isDone() && waitTime > 0) {
                    // 增加等待线程计数
                    incWaiters();
                    try {
                        // 等待指定时间（毫秒和纳秒）
                        wait(waitTime / 1000000, (int) (waitTime % 1000000));
                    } catch (InterruptedException e) {
                        if (interruptable) {
                            // 如果允许中断则抛出异常
                            throw e;
                        } else {
                            // 不允许中断则记录被中断标记，等函数退出后重置中断状态
                            interrupted = true;
                        }
                    } finally {
                        // 减少等待线程计数
                        decWaiters();
                    }

                    // 如果已完成则立即返回
                    if (isDone()) {
                        return true;
                    }

                    // 重新计算剩余等待时间
                    waitTime = timeoutNanos - (System.nanoTime() - startTime);
                }

                // 超时或完成后最终返回结果状态
                return isDone();
            } finally {
                if (interrupted) {
                    // 如果期间中断但未抛出异常，则恢复中断标志位
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Notify all progressive listeners.
     * <p>
     * No attempt is made to ensure notification order if multiple calls are made to this method before
     * the original invocation completes.
     * <p>
     * This will do an iteration over all listeners to get all of type {@link GenericProgressiveFutureListener}s.
     * DefaultProgressivePromise使用的方法
     * 通知所有进度监听器（GenericProgressiveFutureListener），传入当前进度和总大小。
     *
     * @param progress the new progress.
     * @param total    the total progress.
     */
    @SuppressWarnings("unchecked")
    void notifyProgressiveListeners(final long progress, final long total) {

        // 获取当前注册的进度监听器，可能是单个、多个或 null
        final Object listeners = progressiveListeners();
        if (listeners == null) {
            return;
        }

        final ProgressiveFuture<V> self = (ProgressiveFuture<V>) this;

        EventExecutor executor = executor();
        if (executor.inEventLoop()) {
            // 当前线程在 EventLoop 中，直接执行监听器
            if (listeners instanceof GenericProgressiveFutureListener[]) {
                notifyProgressiveListeners0(self, (GenericProgressiveFutureListener<?>[]) listeners, progress, total);
            } else {
                notifyProgressiveListener0(self, (GenericProgressiveFutureListener<ProgressiveFuture<V>>) listeners, progress, total);
            }
        } else {
            // 当前线程不是 EventLoop，异步提交到对应的线程执行
            if (listeners instanceof GenericProgressiveFutureListener[]) {
                final GenericProgressiveFutureListener<?>[] array = (GenericProgressiveFutureListener<?>[]) listeners;
                safeExecute(executor, new Runnable() {
                    @Override
                    public void run() {
                        notifyProgressiveListeners0(self, array, progress, total);
                    }
                });
            } else {
                final GenericProgressiveFutureListener<ProgressiveFuture<V>> l = (GenericProgressiveFutureListener<ProgressiveFuture<V>>) listeners;
                safeExecute(executor, new Runnable() {
                    @Override
                    public void run() {
                        notifyProgressiveListener0(self, l, progress, total);
                    }
                });
            }
        }
    }

    /**
     * Returns a {@link GenericProgressiveFutureListener}, an array of {@link GenericProgressiveFutureListener}, or
     * 返回当前注册的进度监听器
     * {@code null}.
     */
    private synchronized Object progressiveListeners() {
        final GenericFutureListener listener = this.listener;
        final DefaultFutureListeners listeners = this.listeners;

        if (listener == null && listeners == null) {
            return null;
        }

        if (listeners != null) {
            // 多个监听器，筛选出进度监听器
            DefaultFutureListeners dfl = listeners;
            int progressiveSize = dfl.progressiveSize();
            switch (progressiveSize) {
                case 0:
                    return null;
                case 1:
                    // 只有一个，直接返回
                    for (GenericFutureListener<?> l : dfl.listeners()) {
                        if (l instanceof GenericProgressiveFutureListener) {
                            return l;
                        }
                    }
                    return null;
            }

            // 多个监听器，复制所有进度监听器到一个新数组中
            GenericFutureListener<?>[] array = dfl.listeners();
            GenericProgressiveFutureListener<?>[] copy = new GenericProgressiveFutureListener[progressiveSize];
            for (int i = 0, j = 0; j < progressiveSize; i++) {
                GenericFutureListener<?> l = array[i];
                if (l instanceof GenericProgressiveFutureListener) {
                    copy[j++] = (GenericProgressiveFutureListener<?>) l;
                }
            }
            return copy;

        } else if (listener instanceof GenericProgressiveFutureListener) {
            // 单个监听器是进度监听器，直接返回
            return listener;
        } else {
            // 单个监听器但不是进度监听器
            return null;
        }
    }

    private static void notifyProgressiveListeners0(ProgressiveFuture<?> future, GenericProgressiveFutureListener<?>[] listeners, long progress, long total) {
        for (GenericProgressiveFutureListener<?> l : listeners) {
            if (l == null) {
                break;
            }
            notifyProgressiveListener0(future, l, progress, total);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void notifyProgressiveListener0(ProgressiveFuture future, GenericProgressiveFutureListener l, long progress, long total) {
        try {
            l.operationProgressed(future, progress, total);
        } catch (Throwable t) {
            if (logger.isWarnEnabled()) {
                logger.warn("An exception was thrown by " + l.getClass().getName() + ".operationProgressed()", t);
            }
        }
    }

    private static boolean isCancelled0(Object result) {
        return result instanceof CauseHolder && ((CauseHolder) result).cause instanceof CancellationException;
    }

    private static boolean isDone0(Object result) {
        return result != null && result != UNCANCELLABLE;
    }

    //异常包装
    private static final class CauseHolder {

        final Throwable cause;

        CauseHolder(Throwable cause) {
            this.cause = cause;
        }
    }

    private static void safeExecute(EventExecutor executor, Runnable task) {
        try {
            executor.execute(task);
        } catch (Throwable t) {
            rejectedExecutionLogger.error("Failed to submit a listener notification task. Event loop shut down?", t);
        }
    }

    private static final class StacklessCancellationException extends CancellationException {

        private static final long serialVersionUID = -2974906711413716191L;

        private StacklessCancellationException() {
        }

        // Override fillInStackTrace() so we not populate the backtrace via a native call and so leak the
        // Classloader.
        @Override
        public Throwable fillInStackTrace() {
            //这样就不生成任何堆栈信息
            return this;
        }

        static StacklessCancellationException newInstance(Class<?> clazz, String method) {
            return ThrowableUtil.unknownStackTrace(new StacklessCancellationException(), clazz, method);
        }
    }
}
