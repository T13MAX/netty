/*
 * Copyright 2019 The Netty Project
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

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.PlatformDependent;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * 用来标记 ChannelHandler 支持哪些事件的位掩码
 * 检测是否存在某个方法
 * 用于在执行的时候进行快速跳过
 *
 * @Author: t13max
 * @Since: 8:13 2025/6/30
 */
final class ChannelHandlerMask {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelHandlerMask.class);

    // Using to mask which methods must be called for a ChannelHandler.
    // 异常捕获事件掩码，值为 1（第0位）
    static final int MASK_EXCEPTION_CAUGHT = 1;

    // Channel 注册事件掩码，值为 1 左移 1 位，即 2（第1位）
    static final int MASK_CHANNEL_REGISTERED = 1 << 1;

    // Channel 注销事件掩码，值为 1 左移 2 位，即 4（第2位）
    static final int MASK_CHANNEL_UNREGISTERED = 1 << 2;

    // Channel 激活事件掩码，值为 1 左移 3 位，即 8（第3位）
    static final int MASK_CHANNEL_ACTIVE = 1 << 3;

    // Channel 不活跃事件掩码，值为 1 左移 4 位，即 16（第4位）
    static final int MASK_CHANNEL_INACTIVE = 1 << 4;

    // Channel 读事件掩码，值为 1 左移 5 位，即 32（第5位）
    static final int MASK_CHANNEL_READ = 1 << 5;

    // 读完成事件掩码，值为 1 左移 6 位，即 64（第6位）
    static final int MASK_CHANNEL_READ_COMPLETE = 1 << 6;

    // 用户自定义事件触发掩码，值为 1 左移 7 位，即 128（第7位）
    static final int MASK_USER_EVENT_TRIGGERED = 1 << 7;

    // Channel 可写性变化事件掩码，值为 1 左移 8 位，即 256（第8位）
    static final int MASK_CHANNEL_WRITABILITY_CHANGED = 1 << 8;

    // 绑定事件掩码，值为 1 左移 9 位，即 512（第9位）
    static final int MASK_BIND = 1 << 9;

    // 连接事件掩码，值为 1 左移 10 位，即 1024（第10位）
    static final int MASK_CONNECT = 1 << 10;

    // 断开连接事件掩码，值为 1 左移 11 位，即 2048（第11位）
    static final int MASK_DISCONNECT = 1 << 11;

    // 关闭事件掩码，值为 1 左移 12 位，即 4096（第12位）
    static final int MASK_CLOSE = 1 << 12;

    // 注销事件掩码，值为 1 左移 13 位，即 8192（第13位）
    static final int MASK_DEREGISTER = 1 << 13;

    // 读操作事件掩码，值为 1 左移 14 位，即 16384（第14位）
    static final int MASK_READ = 1 << 14;

    // 写操作事件掩码，值为 1 左移 15 位，即 32768（第15位）
    static final int MASK_WRITE = 1 << 15;

    // 刷新事件掩码，值为 1 左移 16 位，即 65536（第16位）
    static final int MASK_FLUSH = 1 << 16;


    static final int MASK_ONLY_INBOUND = MASK_CHANNEL_REGISTERED |
            MASK_CHANNEL_UNREGISTERED | MASK_CHANNEL_ACTIVE | MASK_CHANNEL_INACTIVE | MASK_CHANNEL_READ |
            MASK_CHANNEL_READ_COMPLETE | MASK_USER_EVENT_TRIGGERED | MASK_CHANNEL_WRITABILITY_CHANGED;
    private static final int MASK_ALL_INBOUND = MASK_EXCEPTION_CAUGHT | MASK_ONLY_INBOUND;
    static final int MASK_ONLY_OUTBOUND = MASK_BIND | MASK_CONNECT | MASK_DISCONNECT |
            MASK_CLOSE | MASK_DEREGISTER | MASK_READ | MASK_WRITE | MASK_FLUSH;
    private static final int MASK_ALL_OUTBOUND = MASK_EXCEPTION_CAUGHT | MASK_ONLY_OUTBOUND;

    private static final FastThreadLocal<Map<Class<? extends ChannelHandler>, Integer>> MASKS =
            new FastThreadLocal<Map<Class<? extends ChannelHandler>, Integer>>() {
                @Override
                protected Map<Class<? extends ChannelHandler>, Integer> initialValue() {
                    return new WeakHashMap<Class<? extends ChannelHandler>, Integer>(32);
                }
            };

    /**
     * Return the {@code executionMask}.
     */
    static int mask(Class<? extends ChannelHandler> clazz) {
        // Try to obtain the mask from the cache first. If this fails calculate it and put it in the cache for fast
        // lookup in the future.
        Map<Class<? extends ChannelHandler>, Integer> cache = MASKS.get();
        Integer mask = cache.get(clazz);
        if (mask == null) {
            mask = mask0(clazz);
            cache.put(clazz, mask);
        }
        return mask;
    }

    /**
     * Calculate the {@code executionMask}.
     * 生成一个handler的掩码 依次判断方法是否存在
     */
    private static int mask0(Class<? extends ChannelHandler> handlerType) {
        int mask = MASK_EXCEPTION_CAUGHT;
        try {
            if (ChannelInboundHandler.class.isAssignableFrom(handlerType)) {
                mask |= MASK_ALL_INBOUND;

                if (isSkippable(handlerType, "channelRegistered", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_REGISTERED;
                }
                if (isSkippable(handlerType, "channelUnregistered", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_UNREGISTERED;
                }
                if (isSkippable(handlerType, "channelActive", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_ACTIVE;
                }
                if (isSkippable(handlerType, "channelInactive", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_INACTIVE;
                }
                if (isSkippable(handlerType, "channelRead", ChannelHandlerContext.class, Object.class)) {
                    mask &= ~MASK_CHANNEL_READ;
                }
                if (isSkippable(handlerType, "channelReadComplete", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_READ_COMPLETE;
                }
                if (isSkippable(handlerType, "channelWritabilityChanged", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_WRITABILITY_CHANGED;
                }
                if (isSkippable(handlerType, "userEventTriggered", ChannelHandlerContext.class, Object.class)) {
                    mask &= ~MASK_USER_EVENT_TRIGGERED;
                }
            }

            if (ChannelOutboundHandler.class.isAssignableFrom(handlerType)) {
                mask |= MASK_ALL_OUTBOUND;

                if (isSkippable(handlerType, "bind", ChannelHandlerContext.class,
                        SocketAddress.class, ChannelPromise.class)) {
                    mask &= ~MASK_BIND;
                }
                if (isSkippable(handlerType, "connect", ChannelHandlerContext.class, SocketAddress.class,
                        SocketAddress.class, ChannelPromise.class)) {
                    mask &= ~MASK_CONNECT;
                }
                if (isSkippable(handlerType, "disconnect", ChannelHandlerContext.class, ChannelPromise.class)) {
                    mask &= ~MASK_DISCONNECT;
                }
                if (isSkippable(handlerType, "close", ChannelHandlerContext.class, ChannelPromise.class)) {
                    mask &= ~MASK_CLOSE;
                }
                if (isSkippable(handlerType, "deregister", ChannelHandlerContext.class, ChannelPromise.class)) {
                    mask &= ~MASK_DEREGISTER;
                }
                if (isSkippable(handlerType, "read", ChannelHandlerContext.class)) {
                    mask &= ~MASK_READ;
                }
                if (isSkippable(handlerType, "write", ChannelHandlerContext.class,
                        Object.class, ChannelPromise.class)) {
                    mask &= ~MASK_WRITE;
                }
                if (isSkippable(handlerType, "flush", ChannelHandlerContext.class)) {
                    mask &= ~MASK_FLUSH;
                }
            }

            if (isSkippable(handlerType, "exceptionCaught", ChannelHandlerContext.class, Throwable.class)) {
                mask &= ~MASK_EXCEPTION_CAUGHT;
            }
        } catch (Exception e) {
            // Should never reach here.
            PlatformDependent.throwException(e);
        }

        return mask;
    }

    /**
     * 判断方法是否存在
     * 存在则跳过
     *
     * @Author: t13max
     * @Since: 8:19 2025/6/30
     */
    private static boolean isSkippable(
            final Class<?> handlerType, final String methodName, final Class<?>... paramTypes) throws Exception {
        return AccessController.doPrivileged(new PrivilegedExceptionAction<Boolean>() {
            @Override
            public Boolean run() throws Exception {
                Method m;
                try {
                    m = handlerType.getMethod(methodName, paramTypes);
                } catch (NoSuchMethodException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Class {} missing method {}, assume we can not skip execution", handlerType, methodName, e);
                    }
                    return false;
                }
                //存在Skip注解也跳过
                return m.isAnnotationPresent(Skip.class);
            }
        });
    }

    private ChannelHandlerMask() {
    }

    /**
     * Indicates that the annotated event handler method in {@link ChannelHandler} will not be invoked by
     * {@link ChannelPipeline} and so <strong>MUST</strong> only be used when the {@link ChannelHandler}
     * method does nothing except forward to the next {@link ChannelHandler} in the pipeline.
     * <p>
     * Note that this annotation is not {@linkplain Inherited inherited}. If a user overrides a method annotated with
     * {@link Skip}, it will not be skipped anymore. Similarly, the user can override a method not annotated with
     * {@link Skip} and simply pass the event through to the next handler, which reverses the behavior of the
     * supertype.
     * </p>
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Skip {
        // no value
    }
}
