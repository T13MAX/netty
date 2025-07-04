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
package io.netty.channel;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

/**
 * Special {@link ChannelFuture} which is writable.
 * 比JDK的Future多了addListener()
 */
public interface ChannelPromise extends ChannelFuture, Promise<Void> {

    /**
     * 获取关联的 Channel 对象
     */
    @Override
    Channel channel();

    /**
     * 设置异步操作成功（带 Void 参数）
     */
    @Override
    ChannelPromise setSuccess(Void result);

    /**
     * 设置异步操作成功（无参数）
     */
    ChannelPromise setSuccess();

    /**
     * 尝试设置为成功，若已完成则返回 false
     */
    boolean trySuccess();

    /**
     * 设置异步操作失败，附带异常信息
     */
    @Override
    ChannelPromise setFailure(Throwable cause);

    /**
     * 添加监听器，操作完成时回调
     */
    @Override
    ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener);

    /**
     * 添加多个监听器
     */
    @Override
    ChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners);

    /**
     * 移除指定监听器
     */
    @Override
    ChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener);

    /**
     * 移除多个监听器
     */
    @Override
    ChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners);

    /**
     * 阻塞直到操作完成，期间可中断
     */
    @Override
    ChannelPromise sync() throws InterruptedException;

    /**
     * 阻塞直到操作完成，不可中断
     */
    @Override
    ChannelPromise syncUninterruptibly();

    /**
     * 阻塞直到完成，期间可中断，抛出 InterruptedException
     */
    @Override
    ChannelPromise await() throws InterruptedException;

    /**
     * 阻塞直到完成，不可中断，不抛异常
     */
    @Override
    ChannelPromise awaitUninterruptibly();

    /**
     * 若当前为 voidPromise，返回新的 可用的Promise，否则返回自身
     * 当某个逻辑可能传入的是 voidPromise()，但又需要一个正常的 promise 来处理结果 调用此方法 获取真正的Promise
     */
    ChannelPromise unvoid();

}
