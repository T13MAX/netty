#         

## 重点包

- transport 核心模块，包含 NIO 实现、EventLoop、Channel、Pipeline 等
- buffer ByteBuf 实现及内存池管理
- common 工具类、线程池、Promise、Timer 等通用功能
- codec 编解码基础类（如 ByteToMessageDecoder、LengthFieldBasedFrameDecoder）
- handler 内置的 Handler（如 IdleStateHandler、SSL、HTTP 支持等）
-
- resolver DNS 解析相关，可选
- transport-native-unix-common 支持 native transport 的通用部分（非重点，跳过也行）

## Netty 核心源码阅读推荐清单（按模块分类）

---

### ✅ 1. common（工具类、线程池、Promise 等）

#### 🧠 一、通用工具类

| 类名                        | 说明                                      |
|---------------------------|-----------------------------------------|
| `ObjectUtil`              | 提供常见的 null 检查等工具方法，例如 `checkNotNull()`。 |
| `InternalThreadLocal`     | Netty 自定义的 ThreadLocal，性能更优，带对象池化特性。    |
| `ThreadPerTaskExecutor`   | 每个任务一个线程的执行器，用于简单场景测试。                  |
| `PromiseNotificationUtil` | 工具类，用于安全地通知 Promise 成功或失败。              |
| `ResourceLeakDetector`    | 内存泄露检测器，帮助定位 ByteBuf 泄露。                |
| `PlatformDependent`       | Netty 底层平台工具类，封装了操作系统相关判断、本地内存操作等。      |

---

#### ⛓ 二、Promise / Future 体系（异步回调）

| 类名                    | 说明                                  |
|-----------------------|-------------------------------------|
| `Future<V>`           | Netty 自定义的 Future 接口，支持异步监听器和链式操作。  |
| `Promise<V>`          | Future 的子接口，支持手动设置结果（成功或失败）。        |
| `DefaultPromise<V>`   | Promise 的默认实现类，具备线程安全和回调机制。         |
| `SucceededFuture<V>`  | 已完成且成功的 Future 实现，适合快速返回成功结果。       |
| `FailedFuture<V>`     | 已完成但失败的 Future 实现。                  |
| `GlobalEventExecutor` | 全局事件执行器（单线程），用于处理默认任务（如关闭 promise）。 |

---

#### 🌀 三、线程和执行器

| 类名                              | 说明                                                             |
|---------------------------------|----------------------------------------------------------------|
| `MultithreadEventExecutorGroup` | 多线程执行器组的抽象基类，EventLoopGroup 和 DefaultEventExecutorGroup 都继承自它。 |
| `DefaultEventExecutor`          | 默认的任务执行器（非 IO），用于调度非 IO 操作，常用于 Promise 回调。                     |
| `FastThreadLocal`               | 高性能 ThreadLocal 替代方案，避免哈希冲突和 ThreadLocalMap 访问。                |
| `FastThreadLocalThread`         | 支持 FastThreadLocal 的线程实现。                                      |

---

#### 🔁 四、集合/对象池相关

| 类名              | 说明                              |
|-----------------|---------------------------------|
| `IntObjectMap`  | 高性能 int -> Object 的映射接口。        |
| `Recycler<T>`   | 对象池实现，提升对象复用率，广泛用于 ByteBuf、任务等。 |
| `ObjectPool<T>` | 通用对象池接口，比 Recycler 更灵活。         |

---

#### 🛠 五、其他实用类

| 类名                   | 说明                                             |
|----------------------|------------------------------------------------|
| `ReferenceCountUtil` | 引用计数工具类，统一管理 ByteBuf 等引用对象的 retain/release 操作。 |
| `NetUtil`            | 提供网络地址解析、获取本机 IP 等方法。                          |
| `SystemPropertyUtil` | 获取 JVM 系统属性的工具类。                               |

---

### ✅ 2. buffer（ByteBuf 与内存分配）

#### 🔧 一、核心类（ByteBuf 体系）

| 类名                                | 说明                                                      |
|-----------------------------------|---------------------------------------------------------|
| `ByteBuf`                         | 核心接口，表示一个可读写的字节缓冲区，支持读写索引、slice、retain 等。               |
| `AbstractByteBuf`                 | ByteBuf 抽象基类，实现大部分通用逻辑。                                 |
| `AbstractReferenceCountedByteBuf` | 实现了引用计数相关逻辑的 ByteBuf 抽象类。                               |
| `Unpooled`                        | 提供创建非池化 ByteBuf 的静态工厂类，如 `copiedBuffer()`、`buffer()` 等。 |
| `PooledByteBufAllocator`          | 默认池化内存分配器，性能高，用于大部分实际生产场景。                              |
| `UnpooledByteBufAllocator`        | 非池化分配器，适合调试、测试或小场景使用。                                   |

---

#### 🧠 二、引用计数机制

| 类名                         | 说明                                    |
|----------------------------|---------------------------------------|
| `ReferenceCounted`         | 引用计数接口，定义 `retain()` / `release()` 等。 |
| `AbstractReferenceCounted` | 引用计数基类，提供线程安全的计数操作实现。                 |
| `ReferenceCountUtil`       | 引用计数工具类，提供自动释放和安全释放等辅助方法。             |

---

#### 🚀 三、池化实现与内存管理

| 类名                | 说明                            |
|-------------------|-------------------------------|
| `PoolArena`       | 管理内存块分配与回收的核心模块，负责分配具体内存。     |
| `PoolChunk`       | 表示一个大块内存区域，内部使用位图管理小块分配。      |
| `PoolSubpage`     | 负责分配小内存块（< pageSize）的子页单位。    |
| `PoolThreadCache` | 每个线程绑定的缓存区，提升小对象申请速度。         |
| `PoolChunkList`   | 管理多个 PoolChunk，按使用率分层，提高回收效率。 |

---

#### 🔁 四、复用与工具类

| 类名                  | 说明                                  |
|---------------------|-------------------------------------|
| `ByteBufUtil`       | 提供常用的 ByteBuf 工具方法，如转 hex、比较、复制等。   |
| `CompositeByteBuf`  | 组合多个 ByteBuf，对外表现为一个整体（零拷贝拼接）。      |
| `DuplicatedByteBuf` | 创建一个共享同一内存区域的新 ByteBuf，索引独立（浅拷贝）。   |
| `SlicedByteBuf`     | 基于原始 ByteBuf 创建一个切片视图，不复制数据，适合局部处理。 |

---

### ✅ 3. transport（通信核心组件）

#### 🧱 一、Bootstrap 启动相关

| 类名                        | 说明                                       |
|---------------------------|------------------------------------------|
| `AbstractBootstrap`       | 启动器基类，封装通用配置逻辑（group、channel、handler 等）。 |
| `Bootstrap`               | 客户端启动器，继承自 `AbstractBootstrap`，用于连接远程服务。 |
| `ServerBootstrap`         | 服务端启动器，支持 childGroup、childHandler 初始化逻辑。 |
| `AbstractBootstrapConfig` | 启动配置快照，只读，用于内部传递配置信息。                    |

#### 🎯 二、Channel 核心体系

| 类名                       | 说明                                               |
|--------------------------|--------------------------------------------------|
| `Channel`                | 接口，表示一个网络连接，定义 bind、connect、write、flush 等操作。     |
| `AbstractChannel`        | Channel 抽象基类，提供生命周期、状态管理等通用实现。                   |
| `Channel.Unsafe`         | Channel 的低级接口，执行真正的网络操作，如 register、bind、write 等。 |
| `ChannelOutboundBuffer`  | 出站缓冲区，暂存待写数据，实现聚合、写入控制等。                         |
| `DefaultChannelPipeline` | Channel 的处理链，管理所有的 handler。                      |
| `ChannelHandlerContext`  | 用于在 pipeline 中传播事件的上下文对象。                        |
| `ChannelPromise`         | 用于监听 Channel 异步操作完成情况的回调对象。                      |
| `DefaultChannelPromise`  | ChannelPromise 的默认实现。                            |
| `VoidChannelPromise`     | 不需要回调的空 Promise。                                 |

#### 🔁 三、EventLoop / 线程模型

| 类名                      | 说明                             |
|-------------------------|--------------------------------|
| `EventExecutorGroup`    | 执行器组接口，定义线程池行为。                |
| `EventLoopGroup`        | 专门用于 I/O 的执行器组，管理多个 EventLoop。 |
| `EventExecutor`         | 单线程任务执行器接口。                    |
| `EventLoop`             | 实际执行 Channel I/O 操作的线程循环。      |
| `SingleThreadEventLoop` | 单线程 EventLoop 抽象实现。            |

#### 📦 四、其他辅助类

| 类名                              | 说明                                           |
|---------------------------------|----------------------------------------------|
| `WriteTask`                     | 延迟执行的写任务，带有 flush 标记和内存估算。                   |
| `AbstractCoalescingBufferQueue` | 出站数据合并队列，聚合多个 ByteBuf 减少系统调用。                |
| `PendingWriteQueue`             | 封装写任务队列，包装成 promise 并支持回调。                   |
| `ChannelInitializer`            | Channel 初始化器，用户可自定义 pipeline 构建逻辑。           |
| `ChannelOption`                 | Channel 可选配置项（如 TCP_NODELAY、SO_REUSEADDR 等）。 |
| `AttributeKey`                  | 用于设置 Channel 的自定义属性。                         |

---

### ✅ 4. codec（编解码框架）

#### 📦 一、编解码器核心接口与抽象类

| 类名                                   | 说明                       |
|--------------------------------------|--------------------------|
| `ChannelHandler`                     | Netty 所有编解码器的顶层接口。       |
| `ChannelInboundHandlerAdapter`       | 入站事件处理器的适配器，通常用于解码器基类。   |
| `ChannelOutboundHandlerAdapter`      | 出站事件处理器的适配器，通常用于编码器基类。   |
| `MessageToByteEncoder<I>`            | 编码器：将消息对象编码为 `ByteBuf`。  |
| `ByteToMessageDecoder`               | 解码器：从 `ByteBuf` 解码为消息对象。 |
| `MessageToMessageDecoder<I>`         | 解码器：从一种消息类型解码为另一种消息类型。   |
| `MessageToMessageEncoder<I>`         | 编码器：从一种消息类型编码为另一种消息类型。   |
| `CombinedChannelDuplexHandler<I, O>` | 同时组合一个解码器与编码器的双工处理器。     |

---

#### 🔢 二、内置协议支持（常用可选）

| 类名                             | 说明                           |
|--------------------------------|------------------------------|
| `LengthFieldBasedFrameDecoder` | 解码器：基于长度字段拆包，解决 TCP 粘包/拆包问题。 |
| `LengthFieldPrepender`         | 编码器：在消息前添加长度字段，对应上面的解码器。     |
| `DelimiterBasedFrameDecoder`   | 解码器：基于分隔符拆包（如换行符）。           |
| `LineBasedFrameDecoder`        | 解码器：按行拆包（以 \n 或 \r\n 结尾）。    |
| `FixedLengthFrameDecoder`      | 解码器：固定长度拆包器。                 |

---

#### 📚 三、文本/HTTP/JSON 编解码器（子包）

| 模块/类                         | 说明                                                           |
|------------------------------|--------------------------------------------------------------|
| `codec.string.StringDecoder` | 字符串解码器，通常将 ByteBuf 解码为 String。                               |
| `codec.string.StringEncoder` | 字符串编码器，将 String 编码为 ByteBuf。                                 |
| `codec.http.*`               | HTTP 编解码器子包，包含 `HttpRequestDecoder`、`HttpResponseEncoder` 等。 |
| `codec.json.*`               | JSON 编解码器支持（可选依赖）。                                           |

---

#### 🧰 四、其他实用类

| 类名                        | 说明                  |
|---------------------------|---------------------|
| `CodecException`          | 编解码异常基类。            |
| `CorruptedFrameException` | 解码失败时抛出，用于标识帧结构错误。  |
| `TooLongFrameException`   | 帧过大时抛出，用于防止攻击或异常数据。 |

---

### ✅ 5. handler（常用功能 Handler）

#### 🔐 一、安全与空闲检测

| 类名                    | 说明                                   |
|-----------------------|--------------------------------------|
| `IdleStateHandler`    | 空闲检测（读/写/全通道空闲），触发 `IdleStateEvent`。 |
| `ReadTimeoutHandler`  | 指定时间内未收到数据则关闭连接。                     |
| `WriteTimeoutHandler` | 指定时间内未完成写操作则触发异常。                    |
| `TimeoutException`    | 超时处理抛出的异常。                           |

---

#### 📦 二、流控与压缩

| 类名                            | 说明                                              |
|-------------------------------|-------------------------------------------------|
| `TrafficShapingHandler`       | 流量整形（限速、延迟），适用于全局或单连接限流。                        |
| `GlobalTrafficShapingHandler` | 全局限流器（多个连接共享流控）。                                |
| `CompressionHandler`（子包）      | 支持 GZIP、ZLIB 压缩/解压缩（依赖 `codec-compression` 模块）。 |

---

#### 📡 三、日志与调试

| 类名                             | 说明                         |
|--------------------------------|----------------------------|
| `LoggingHandler`               | 将 Channel 中所有事件打印到日志，调试常用。 |
| `ChannelTrafficShapingHandler` | 基于通道限流的 Handler，可动态调控带宽。   |

---

#### 🔁 四、编解码链相关 Handler

| 类名                          | 说明                         |
|-----------------------------|----------------------------|
| `FlushConsolidationHandler` | 合并 flush 调用，减少系统调用次数，优化吞吐。 |
| `ChunkedWriteHandler`       | 分块写入大文件/大数据，用于下载、文件传输等。    |

---

#### 🧩 五、HA/心跳/桥接等特殊 Handler

| 类名                        | 说明                          |
|---------------------------|-----------------------------|
| `ChannelDuplexHandler`    | 同时处理入站和出站事件的基类。             |
| `BridgeHandler`           | 桥接两个 ChannelPipeline（例如代理）。 |
| `HeartbeatHandler`（自定义场景） | 心跳检测 Handler，一般用户自定义实现。     |

---

#### 📚 六、异常与工具类

| 类名             | 说明                                            |
|----------------|-----------------------------------------------|
| `Decoders`（子包） | 包含各种基础解码器，如 Base64Decoder、ByteArrayDecoder 等。 |
| `Encoders`（子包） | 包含各种基础编码器，如 Base64Encoder、ByteArrayEncoder 等。 |

---

