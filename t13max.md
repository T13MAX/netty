# 

## 🔷 Netty 阅读方式（底层逻辑导向）

1. **先看执行流程**（从 `ServerBootstrap` 启动开始）
2. **聚焦核心类**（如 `Channel`, `EventLoop`, `ByteBuf`, `Pipeline`）
3. **看类的行为实现**，少用调试，多看 `run()`、`fireXxx()` 等方法链
4. **多画流程图和对象图**（帮助理解 Pipeline 和事件流）

适合“**按调用链条追踪**”的阅读方式。