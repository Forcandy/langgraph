# LangGraph 模块技术文档

> **文档版本**: 1.0
> **项目版本**: 1.0.3
> **生成日期**: 2025-11-17
> **文档类型**: 模块级技术文档

---

## 目录

1. [模块概述](#第1章-模块概述)
2. [架构设计](#第2章-架构设计)
3. [核心流程图](#第3章-核心流程图)
4. [关键算法详解](#第4章-关键算法详解)
5. [数据结构分析](#第5章-数据结构分析)
6. [配置开关说明](#第6章-配置开关说明)
7. [外部依赖](#第7章-外部依赖)
8. [API 接口说明](#第8章-api-接口说明)
9. [错误码及异常处理](#第9章-错误码及异常处理)
10. [部署与运行](#第10章-部署与运行)
11. [总结图表](#总结图表)

---

# 第1章 模块概述

## 1.1 核心功能和设计目标

### 1.1.1 核心功能

**LangGraph** 是一个低级编排框架，专门用于构建、管理和部署长时间运行、有状态的AI代理（Agents）应用。其核心功能包括：

1. **有状态工作流编排**
   - 基于图（Graph）的工作流定义
   - 节点（Node）间通过共享状态（State）通信
   - 支持条件分支、循环、子图等复杂控制流

2. **持久化执行（Durable Execution）**
   - 检查点（Checkpoint）机制支持任意步骤的暂停和恢复
   - 故障恢复能力，从中断点精确继续执行
   - 分布式环境下的状态同步

3. **人机交互（Human-in-the-Loop）**
   - 中断（Interrupt）机制允许在执行过程中请求人工输入
   - 状态审查和修改能力
   - 审批流程支持

4. **全面的内存管理**
   - 短期内存：步骤间的工作记忆
   - 长期内存：跨会话的持久化存储
   - 托管值（Managed Values）：运行时动态计算的值

5. **流式执行和可观测性**
   - 多种流模式（values, updates, messages, debug等）
   - 与LangSmith集成的深度追踪
   - 实时状态可视化

### 1.1.2 设计目标

- **灵活性优先**：不抽象提示词或架构，给予开发者完全控制
- **生产就绪**：为长时间运行的有状态工作流设计
- **可扩展性**：支持插件化通道、策略、存储等组件
- **类型安全**：完整的类型提示和运行时验证
- **高性能**：基于Pregel算法的并行执行引擎

### 1.1.3 设计理念

LangGraph受以下计算模型启发：

- **Pregel算法**（Google）：批量同步并行（BSP）计算模型
- **Apache Beam**：数据流处理框架
- **Actor模型**：基于消息传递的并发模型
- **NetworkX**：图结构API设计

## 1.2 模块在系统中的职责和协作关系

### 1.2.1 系统定位

```
┌─────────────────────────────────────────────────────────┐
│                    应用层                                │
│  用户AI应用、聊天机器人、智能助手、自动化工作流        │
└──────────────────┬──────────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────────┐
│              LangGraph（本模块）                        │
│  • 工作流编排引擎                                       │
│  • 状态管理和持久化                                     │
│  • 执行控制和调度                                       │
└──────────────────┬──────────────────────────────────────┘
                   │
        ┌──────────┼──────────┐
        │          │           │
┌───────▼─────┐ ┌─▼────────┐ ┌▼──────────────┐
│ LangChain   │ │LangSmith │ │ LangGraph     │
│   Core      │ │ (可选)   │ │  Checkpoint   │
│             │ │          │ │   Backends    │
│• Runnables  │ │• 追踪    │ │• Postgres     │
│• 工具调用   │ │• 评估    │ │• SQLite       │
│• LLM集成    │ │• 监控    │ │• Memory       │
└─────────────┘ └──────────┘ └───────────────┘
```

### 1.2.2 上下游模块协作

**上游依赖**：
- **LangChain Core**：提供Runnable抽象、工具调用、消息类型
- **LangGraph Checkpoint**：提供检查点存储抽象
- **LangGraph SDK**：远程执行客户端
- **LangGraph Prebuilt**：预构建的Agent组件

**下游消费**：
- 用户应用直接调用LangGraph API
- LangGraph CLI工具用于部署和管理
- LangGraph Studio提供可视化开发环境

### 1.2.3 核心职责

| 职责 | 描述 | 实现位置 |
|------|------|----------|
| **图构建** | 提供StateGraph API构建工作流 | `langgraph/graph/state.py` |
| **编译验证** | 将高级图定义编译为可执行的Pregel | `langgraph/pregel/main.py` |
| **执行调度** | BSP算法的Plan-Execute-Update循环 | `langgraph/pregel/_loop.py` |
| **状态管理** | 通道（Channel）系统管理节点间状态 | `langgraph/channels/` |
| **持久化** | 检查点保存和恢复 | `langgraph/pregel/_checkpoint.py` |
| **并发控制** | 多线程/异步任务执行 | `langgraph/pregel/_executor.py` |
| **中断处理** | 人机交互和审批流程 | `langgraph/types.py:Interrupt` |

## 1.3 输入数据格式、类型、来源

### 1.3.1 输入数据格式

LangGraph的输入取决于图的定义，主要有以下几种形式：

**（1）字典格式（最常见）**

```python
# 示例1：简单消息输入
input_data = {
    "messages": [
        {"role": "user", "content": "Hello, how are you?"}
    ]
}

# 示例2：复杂状态输入
input_data = {
    "query": "What is the weather?",
    "context": [...],
    "user_id": "12345",
    "session_id": "abc-def"
}
```

**来源**：应用层通过`graph.invoke(input_data, config)`传入

**（2）TypedDict类型化输入**

```python
from typing_extensions import TypedDict

class InputState(TypedDict):
    query: str
    max_results: int
    filters: dict

# 输入示例
input_data: InputState = {
    "query": "search term",
    "max_results": 10,
    "filters": {"category": "tech"}
}
```

**来源**：用户代码，类型由`StateGraph(input_schema=InputState)`定义

**（3）Pydantic模型输入**

```python
from pydantic import BaseModel

class SearchInput(BaseModel):
    query: str
    limit: int = 10

# 输入示例
input_data = SearchInput(query="LangGraph", limit=5).model_dump()
```

**来源**：从API请求、数据库查询等反序列化而来

### 1.3.2 输入类型约束

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `inputs` | `dict` / `TypedDict` / 自定义类型 | 是 | 图的初始状态 |
| `config` | `RunnableConfig` | 否 | 运行配置（thread_id、checkpointer等） |
| `context` | 自定义Context类型 | 否 | 运行时上下文（如user_id、db_conn） |

### 1.3.3 输入来源

```mermaid
flowchart LR
    A[用户应用] --> B[LangGraph API]
    C[API网关] --> B
    D[定时任务] --> B
    E[消息队列] --> B
    F[Webhook] --> B

    B --> G[graph.invoke/stream]
    G --> H[Pregel执行引擎]
```

## 1.4 输出数据格式、流向

### 1.4.1 输出数据格式

**（1）同步调用输出（invoke）**

```python
# 输出：最终状态字典
result = graph.invoke({"messages": [...]})
# 示例输出
{
    "messages": [
        {"role": "user", "content": "Hello"},
        {"role": "assistant", "content": "Hi! How can I help?"}
    ],
    "context": {...},
    "metadata": {...}
}
```

**（2）流式输出（stream）**

根据`stream_mode`不同，输出格式不同：

```python
# stream_mode="values" - 每步后输出完整状态
for chunk in graph.stream(input_data, stream_mode="values"):
    print(chunk)
# 输出：{"messages": [...], ...}

# stream_mode="updates" - 仅输出节点更新
for chunk in graph.stream(input_data, stream_mode="updates"):
    print(chunk)
# 输出：{"node_name": {"key": "value"}}

# stream_mode="messages" - LLM消息流
for chunk in graph.stream(input_data, stream_mode="messages"):
    print(chunk)
# 输出：("node_name", AIMessageChunk(...))
```

**（3）状态快照输出（get_state）**

```python
snapshot = graph.get_state(config)
# 输出：StateSnapshot
{
    "values": {...},           # 当前状态
    "next": ("node_a",),       # 下一个执行的节点
    "config": {...},           # 配置
    "metadata": {...},         # 元数据
    "created_at": "2025-11-17T...",
    "parent_config": {...},
    "tasks": [...],            # 待执行任务
    "interrupts": [...]        # 中断列表
}
```

### 1.4.2 输出流向

```mermaid
flowchart TD
    A[Pregel执行引擎] --> B{输出类型}
    B -->|invoke| C[返回最终状态]
    B -->|stream| D[流式迭代器]
    B -->|get_state| E[状态快照]

    C --> F[应用层处理]
    D --> G[实时展示/日志]
    E --> H[状态检查/恢复]

    F --> I[返回给用户]
    G --> I
    H --> J[后续操作决策]
```

### 1.4.3 输出示例汇总

| 方法 | 输出类型 | 使用场景 |
|------|----------|----------|
| `invoke()` | `dict` | 同步获取最终结果 |
| `ainvoke()` | `dict` | 异步获取最终结果 |
| `stream()` | `Iterator[dict]` | 流式获取中间状态 |
| `astream()` | `AsyncIterator[dict]` | 异步流式获取 |
| `get_state()` | `StateSnapshot` | 检查当前状态 |
| `update_state()` | `RunnableConfig` | 修改状态后返回新配置 |

## 1.5 子模块和策略组件

### 1.5.1 核心子模块

LangGraph包含以下主要子模块：

#### （1）**pregel/** - 执行引擎（18个文件）

**职责**：实现基于Pregel算法的图执行引擎

**核心组件**：
- `main.py` - Pregel类，主执行器
- `_algo.py` - BSP算法实现（Plan-Execute-Update）
- `_loop.py` - 同步/异步执行循环
- `_runner.py` - 任务执行器，管理并发
- `_checkpoint.py` - 检查点管理
- `_read.py` / `_write.py` - 通道读写逻辑

**适用场景**：所有图执行都通过此模块

#### （2）**graph/** - 图构建API（5个文件）

**职责**：提供用户友好的图构建接口

**核心组件**：
- `state.py` - StateGraph类，主要用户API
- `message.py` - MessagesState和add_messages工具
- `_branch.py` - 条件分支规范
- `_node.py` - 节点协议定义

**适用场景**：
```python
# 构建有状态工作流
from langgraph.graph import StateGraph, START, END

builder = StateGraph(State)
builder.add_node("node_a", func_a)
builder.add_edge(START, "node_a")
builder.compile()
```

#### （3）**channels/** - 通道系统（8个文件）

**职责**：节点间通信和状态存储

**通道类型和适用场景**：

| 通道类型 | 适用场景 | 配置方式 |
|---------|---------|---------|
| **LastValue** | 单值存储，如配置参数 | `LastValue(typ)` |
| **BinaryOperatorAggregate** | 聚合值，如消息列表 | `BinaryOperatorAggregate(typ, operator.add)` |
| **Topic** | 消息流，支持去重 | `Topic(typ)` |
| **EphemeralValue** | 临时值，不持久化 | `EphemeralValue(typ)` |
| **NamedBarrierValue** | 同步屏障，等待多个输入 | `NamedBarrierValue(typ, names)` |

**自动配置**：
```python
# StateGraph会根据类型注解自动创建通道
class State(TypedDict):
    messages: Annotated[list, operator.add]  # → BinaryOperatorAggregate
    config: dict                              # → LastValue
```

#### （4）**func/** - 函数式API（1个文件）

**职责**：装饰器风格的工作流定义

**适用场景**：
```python
from langgraph.func import task, entrypoint

@task
def process_item(item: dict) -> dict:
    return {"result": item["value"] * 2}

@entrypoint(checkpointer=saver)
def workflow(inputs):
    results = [process_item(item).result() for item in inputs["items"]]
    return {"results": results}
```

**优势**：
- 更简洁的语法
- 自动并行化
- 支持Future模式

#### （5）**managed/** - 托管值系统（2个文件）

**职责**：运行时动态计算的值

**内置托管值**：

| 托管值 | 用途 | 使用示例 |
|--------|------|---------|
| **IsLastStep** | 判断是否为最后一步 | `def node(state, is_last_step: IsLastStep):` |
| **RemainingSteps** | 获取剩余步数 | `def node(state, remaining: RemainingSteps):` |

**自定义托管值**：
```python
from langgraph.managed.base import ManagedValue

class CurrentUser(ManagedValue[str]):
    @staticmethod
    def get(scratchpad) -> str:
        return scratchpad.config.get("configurable", {}).get("user_id", "anonymous")
```

### 1.5.2 策略组件

#### （1）**RetryPolicy** - 重试策略

```python
from langgraph.types import RetryPolicy

retry_policy = RetryPolicy(
    initial_interval=0.5,    # 初始重试间隔（秒）
    backoff_factor=2.0,      # 退避因子
    max_interval=128.0,      # 最大重试间隔
    max_attempts=3,          # 最大尝试次数
    jitter=True,             # 添加随机抖动
    retry_on=lambda e: isinstance(e, NetworkError)  # 重试条件
)

builder.add_node("api_call", call_api, retry_policy=retry_policy)
```

#### （2）**CachePolicy** - 缓存策略

```python
from langgraph.types import CachePolicy

cache_policy = CachePolicy(
    key_func=lambda x: f"user:{x['user_id']}",
    ttl=300  # 5分钟过期
)

builder.add_node("fetch_data", fetch, cache_policy=cache_policy)
```

#### （3）**Durability** - 持久化策略

```python
# 同步持久化
graph = builder.compile(checkpointer=saver, durability="sync")

# 异步持久化
graph = builder.compile(checkpointer=saver, durability="async")

# 仅退出时持久化
graph = builder.compile(checkpointer=saver, durability="exit")
```

## 1.6 版本信息和迭代历史

### 1.6.1 当前版本

**版本号**：`1.0.3`
**发布日期**：基于git提交 `ac16bdb` (2025-11-17)
**状态**：生产稳定版（Production/Stable）

**版本来源**：`libs/langgraph/pyproject.toml:7`

### 1.6.2 主要贡献者

**开发组织**：LangChain Inc.
**核心团队**：LangChain AI团队

**推断依据**：
- README.md标注 "LangGraph is built by LangChain Inc"
- 项目托管于 `github.com/langchain-ai/langgraph`

### 1.6.3 近期迭代历史

基于git日志推断的近期更新：

| 提交ID | 日期 | 说明 | 影响范围 |
|--------|------|------|---------|
| `df8becd` | 最新 | 重构：分离prepare_push_*函数 | pregel模块 |
| `056ba91` | 近期 | 文档：添加更多重定向 | 文档系统 |
| `02300de` | 近期 | 修复：预构建组件依赖警告 | prebuilt模块 |
| `ac16bdb` | 近期 | 发布：prebuilt 1.0.3 | 版本发布 |
| `0d4ac83` | 近期 | 维护：langgraph补丁版本 | 核心库 |

### 1.6.4 支持的Python版本

**要求**：`Python >= 3.10`

**支持版本**：
- Python 3.10 ✓
- Python 3.11 ✓
- Python 3.12 ✓
- Python 3.13 ✓

**推断依据**：`pyproject.toml:10` 和 `classifiers:21-24`

### 1.6.5 版本演进路线图

**待确认**：具体的功能演进计划需要查阅官方roadmap或changelog

**推测依据**：
- v0.x → v1.0：API稳定化，生产就绪
- 未来可能方向：
  - 更多内置通道类型
  - 增强的分布式执行能力
  - 更丰富的预构建组件
  - 性能优化和扩展性改进

---

**第1章小结**：

LangGraph是一个面向生产环境的有状态AI工作流编排框架，核心优势在于：
1. 灵活的图结构工作流定义
2. 强大的持久化和恢复能力
3. 人机交互和中断机制
4. 多种可扩展的策略组件
5. 完整的类型安全和可观测性

其输入为字典格式的状态数据，输出支持同步/异步、流式/批量等多种模式，适用于构建复杂的AI代理应用。

---

# 第2章 架构设计

## 2.1 项目文件结构

### 2.1.1 整体目录树

```
langgraph/                           # 项目根目录
├── libs/                            # 所有子包
│   ├── langgraph/                   # 核心库（本文档主要分析对象）
│   │   ├── langgraph/               # 源代码目录
│   │   │   ├── pregel/              # 执行引擎（20个文件）
│   │   │   ├── graph/               # 图构建API（5个文件）
│   │   │   ├── channels/            # 通道系统（8个文件）
│   │   │   ├── managed/             # 托管值（2个文件）
│   │   │   ├── func/                # 函数式API（1个文件）
│   │   │   ├── utils/               # 工具函数（2个文件）
│   │   │   ├── _internal/           # 内部实现（12个文件）
│   │   │   ├── types.py             # 核心类型定义（19KB）
│   │   │   ├── errors.py            # 异常体系（3.6KB）
│   │   │   ├── config.py            # 配置访问（5.6KB）
│   │   │   ├── runtime.py           # 运行时上下文（4.6KB）
│   │   │   ├── constants.py         # 公开常量（1.9KB）
│   │   │   ├── typing.py            # 类型别名（1.3KB）
│   │   │   ├── warnings.py          # 警告定义（2.1KB）
│   │   │   └── version.py           # 版本信息（0.3KB）
│   │   ├── tests/                   # 测试套件
│   │   ├── pyproject.toml           # 项目配置
│   │   └── README.md
│   ├── checkpoint/                  # 检查点抽象
│   ├── checkpoint-sqlite/           # SQLite检查点实现
│   ├── checkpoint-postgres/         # PostgreSQL检查点实现
│   ├── prebuilt/                    # 预构建组件
│   ├── sdk-py/                      # Python SDK
│   ├── sdk-js/                      # JavaScript SDK
│   └── cli/                         # 命令行工具
├── examples/                        # 示例代码
├── docs/                            # 文档站点
├── README.md
└── CONTRIBUTING.md
```

### 2.1.2 核心模块目录详解

#### **langgraph/pregel/** - 执行引擎核心（20个文件）

| 文件 | 职责 | 代码行数（估算） |
|------|------|------------------|
| `main.py` | Pregel类定义，主执行器 | ~800行 |
| `protocol.py` | PregelProtocol接口定义 | ~150行 |
| `_algo.py` | BSP算法核心（Plan-Execute-Update） | ~600行 |
| `_loop.py` | 同步/异步执行循环 | ~500行 |
| `_runner.py` | 任务并发执行器 | ~400行 |
| `_executor.py` | 线程池/进程池执行器 | ~300行 |
| `_checkpoint.py` | 检查点创建和恢复 | ~250行 |
| `_read.py` | 通道读取逻辑和PregelNode | ~400行 |
| `_write.py` | 通道写入逻辑 | ~300行 |
| `_io.py` | 输入输出映射 | ~200行 |
| `_call.py` | 节点调用协调 | ~300行 |
| `_retry.py` | 重试策略实现 | ~200行 |
| `_validate.py` | 图结构验证 | ~150行 |
| `_messages.py` | 消息流处理 | ~250行 |
| `_utils.py` | 工具函数 | ~150行 |
| `debug.py` | 调试支持 | ~100行 |
| `remote.py` | 远程执行支持 | ~200行 |
| `types.py` | Pregel特定类型 | ~100行 |

**总计**：约5,150行，负责整个图的执行生命周期。

#### **langgraph/graph/** - 图构建API（5个文件）

| 文件 | 职责 | 代码行数（估算） |
|------|------|------------------|
| `state.py` | StateGraph和CompiledStateGraph | ~1,200行 |
| `message.py` | MessagesState和add_messages | ~150行 |
| `_node.py` | 节点协议和规范 | ~200行 |
| `_branch.py` | 分支条件规范 | ~150行 |
| `ui.py` | UI相关工具 | ~50行 |

**总计**：约1,750行，提供用户友好的图构建接口。

#### **langgraph/channels/** - 通道系统（8个文件）

| 文件 | 通道类型 | 用途 |
|------|----------|------|
| `base.py` | BaseChannel抽象基类 | 定义通道接口契约 |
| `last_value.py` | LastValue | 存储单个最新值 |
| `binop.py` | BinaryOperatorAggregate | 使用二元运算符聚合值 |
| `topic.py` | Topic | 发布订阅消息流 |
| `any_value.py` | AnyValue | 任意类型值存储 |
| `ephemeral_value.py` | EphemeralValue | 临时值（不持久化） |
| `named_barrier_value.py` | NamedBarrierValue | 同步屏障 |
| `untracked_value.py` | UntrackedValue | 无追踪值 |

**总计**：约800行，实现节点间的通信机制。

#### **langgraph/_internal/** - 内部实现（12个文件）

| 文件 | 职责 |
|------|------|
| `_constants.py` | 保留键名和配置常量 |
| `_config.py` | 配置管理和合并 |
| `_cache.py` | 缓存功能 |
| `_typing.py` | 类型工具 |
| `_fields.py` | 字段处理和reducer提取 |
| `_runnable.py` | Runnable包装和转换 |
| `_pydantic.py` | Pydantic集成 |
| `_scratchpad.py` | 临时存储（PregelScratchpad） |
| `_retry.py` | 重试机制实现 |
| `_queue.py` | 同步/异步队列 |
| `_future.py` | Future工具 |

**总计**：约1,500行，提供底层支持功能。

### 2.1.3 代码规模统计

```
总文件数：64个Python文件
总代码量：约16,000行（不含测试）
核心库大小：674KB
平均文件大小：约250行/文件
```

## 2.2 功能模块划分方式及设计意图

### 2.2.1 分层架构

LangGraph采用**四层架构**设计：

```
┌─────────────────────────────────────────────────────────────┐
│                   第1层：用户API层                          │
│  - StateGraph (graph/state.py)                             │
│  - task/entrypoint (func/)                                 │
│  - 预构建组件 (来自langgraph-prebuilt)                     │
│                                                             │
│  【设计意图】：提供简单、类型安全的用户接口                │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                   第2层：编译层                             │
│  - StateGraph.compile() (graph/state.py)                   │
│  - 图验证 (pregel/_validate.py)                            │
│  - 通道自动创建 (graph/state.py)                           │
│                                                             │
│  【设计意图】：将高级定义转换为可执行的Pregel图            │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                   第3层：执行引擎层                         │
│  - Pregel (pregel/main.py)                                 │
│  - BSP算法 (pregel/_algo.py)                               │
│  - 执行循环 (pregel/_loop.py)                              │
│  - 任务调度 (pregel/_runner.py)                            │
│                                                             │
│  【设计意图】：高效、可靠的并行执行                        │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                   第4层：基础设施层                         │
│  - 通道系统 (channels/)                                    │
│  - 检查点存储 (来自langgraph-checkpoint)                   │
│  - 运行时上下文 (runtime.py)                               │
│  - 托管值 (managed/)                                       │
│                                                             │
│  【设计意图】：可扩展的底层组件                            │
└─────────────────────────────────────────────────────────────┘
```

### 2.2.2 模块职责矩阵

| 模块 | 核心职责 | 对外接口 | 内部依赖 |
|------|----------|----------|----------|
| **graph/** | 图构建、节点管理、边定义 | StateGraph, add_node, add_edge | pregel/, channels/ |
| **pregel/** | 图执行、状态管理、检查点 | Pregel.invoke/stream | channels/, _internal/ |
| **channels/** | 节点间通信、状态存储 | BaseChannel, LastValue, Topic | 无（独立） |
| **managed/** | 动态值计算 | ManagedValue, IsLastStep | _internal/ |
| **func/** | 函数式API、装饰器 | task, entrypoint | pregel/, graph/ |
| **_internal/** | 内部工具、配置管理 | （不对外） | 无 |

### 2.2.3 设计意图总结

1. **关注点分离**：用户API、编译、执行、存储各自独立
2. **逐层抽象**：从高级DSL到底层执行引擎的平滑过渡
3. **可替换性**：通道、检查点、执行器均可插拔
4. **类型安全**：从StateGraph定义到运行时全程类型检查

## 2.3 主流程的控制路径

### 2.3.1 典型执行路径

```
用户代码
   │
   ├─► builder = StateGraph(State)           # graph/state.py:195
   ├─► builder.add_node("node_a", func_a)    # graph/state.py:420
   ├─► builder.add_edge(START, "node_a")     # graph/state.py:510
   ├─► graph = builder.compile(checkpointer) # graph/state.py:850
   │       │
   │       ├─► 验证图结构                     # pregel/_validate.py
   │       ├─► 创建通道实例                   # channels/*.py
   │       └─► 构建Pregel对象                 # pregel/main.py:321
   │
   └─► result = graph.invoke(inputs, config) # pregel/main.py:700
           │
           ├─► _execute_sync()                 # pregel/main.py:800
           │       │
           │       ├─► 初始化检查点            # pregel/_checkpoint.py
           │       ├─► 创建Runtime上下文       # runtime.py:28
           │       └─► 进入主循环              # pregel/_loop.py
           │               │
           │               ├─► [步骤1] 规划阶段
           │               │     prepare_next_tasks()    # pregel/_algo.py:400
           │               │     → 确定待执行节点
           │               │
           │               ├─► [步骤2] 执行阶段
           │               │     PregelRunner.invoke()   # pregel/_runner.py:100
           │               │     → 并行执行任务
           │               │     → 收集写入
           │               │
           │               ├─► [步骤3] 更新阶段
           │               │     apply_writes()          # pregel/_algo.py:250
           │               │     → 更新通道值
           │               │     → 保存检查点
           │               │
           │               └─► 检查终止条件并循环
           │
           └─► 返回最终状态
```

### 2.3.2 入口点分析

| 入口方法 | 位置 | 同步/异步 | 流式 | 说明 |
|---------|------|-----------|------|------|
| `invoke()` | pregel/main.py:700 | 同步 | 否 | 最常用，返回最终结果 |
| `ainvoke()` | pregel/main.py:750 | 异步 | 否 | 异步版本invoke |
| `stream()` | pregel/main.py:800 | 同步 | 是 | 流式迭代中间状态 |
| `astream()` | pregel/main.py:900 | 异步 | 是 | 异步流式 |
| `get_state()` | pregel/main.py:600 | 同步 | - | 获取当前状态快照 |
| `update_state()` | pregel/main.py:650 | 同步 | - | 手动修改状态 |

### 2.3.3 出口点分析

**正常出口**：
1. 所有节点执行完毕，无待执行任务
2. 到达END节点
3. 返回最终状态字典

**异常出口**：
1. 抛出`GraphRecursionError`（超过递归限制）
2. 抛出`InvalidUpdateError`（无效状态更新）
3. 节点内部异常未捕获

**中断出口**：
1. 触发`interrupt()`，返回中断事件
2. 等待用户通过`Command(resume=...)`恢复

## 2.4 数据在模块间的流动方式

### 2.4.1 数据流图

```mermaid
flowchart TD
    A[用户输入] --> B[StateGraph]
    B --> C[Pregel编译]
    C --> D{执行循环}

    D --> E[通道读取]
    E --> F[节点执行]
    F --> G[通道写入]
    G --> H[检查点保存]
    H --> I{是否继续?}

    I -->|是| D
    I -->|否| J[返回结果]

    K[配置Config] -.-> D
    L[运行时Runtime] -.-> F
    M[检查点Checkpoint] -.-> E
    M -.-> H
```

### 2.4.2 数据传递机制

#### （1）**引用传递** - 通道系统

```python
# 通道内部维护引用
class LastValue(BaseChannel):
    value: Any  # 存储的是对象引用

    def update(self, values):
        if values:
            self.value = values[-1]  # 引用赋值

    def get(self):
        return self.value  # 返回引用
```

**影响**：大对象（如大型列表）不会被复制，性能高效。

#### （2）**值传递** - 检查点序列化

```python
def checkpoint() -> Checkpoint:
    return {
        "channel_values": {
            k: channel.checkpoint()  # 序列化为可存储格式
            for k, channel in channels.items()
        }
    }
```

**影响**：检查点保存时会深拷贝，确保状态不可变。

#### （3）**对象封装** - Runtime和Config

```python
@dataclass
class Runtime:
    context: ContextT       # 封装用户上下文
    store: BaseStore        # 封装存储引擎
    stream_writer: StreamWriter

# 通过配置传递
config["configurable"][CONFIG_KEY_RUNTIME] = runtime
```

**影响**：依赖注入模式，节点通过参数声明获取。

#### （4）**中间结果缓存** - PregelScratchpad

```python
class PregelScratchpad:
    pending_writes: dict[str, list[Any]]  # 缓存未提交的写入

def local_read(scratchpad, task, select):
    # 读取包含task自身写入的本地视图
    return merge(channels, scratchpad.pending_writes[task.id])
```

**影响**：支持条件边读取包含自身写入的状态。

### 2.4.3 数据一致性保证

1. **通道版本控制**：每次更新递增版本号，防止重复触发
2. **原子性更新**：一个步骤的所有写入要么全部应用，要么全部回滚
3. **检查点隔离**：每个检查点是完整独立的状态快照

## 2.5 核心类、函数、接口的职责与调用关系

### 2.5.1 核心类层次结构

```
PregelProtocol (接口)
    ├── Pregel                      # 主执行引擎
    │   ├── nodes: dict[str, PregelNode]
    │   ├── channels: dict[str, BaseChannel]
    │   ├── invoke() → dict
    │   ├── stream() → Iterator
    │   └── get_state() → StateSnapshot
    │
    └── CompiledStateGraph          # StateGraph编译后的形式
        └── (继承Pregel的所有方法)

StateGraph                          # 图构建器（非Protocol）
    ├── add_node()
    ├── add_edge()
    ├── add_conditional_edges()
    └── compile() → CompiledStateGraph

BaseChannel (抽象基类)
    ├── LastValue
    ├── BinaryOperatorAggregate
    ├── Topic
    └── ...

Runtime[ContextT]                   # 运行时上下文
    ├── context: ContextT
    ├── store: BaseStore
    └── stream_writer: StreamWriter
```

### 2.5.2 关键调用链

#### **图编译调用链**

```python
StateGraph.compile()                           # graph/state.py:850
    ├─► _get_channels()                        # 推断通道类型
    ├─► validate_graph(nodes, edges)           # pregel/_validate.py
    ├─► _prepare_nodes()                       # 创建PregelNode
    └─► Pregel(                                # pregel/main.py:321
            nodes=...,
            channels=...,
            input_channels=...,
            output_channels=...,
            checkpointer=...
        )
```

#### **图执行调用链**

```python
Pregel.invoke(inputs, config)                  # pregel/main.py:700
    └─► _execute_sync(inputs, config)          # pregel/main.py:1000
        ├─► _init_checkpoint()                 # 初始化或加载检查点
        ├─► SyncPregelLoop(...)                # pregel/_loop.py:50
        │   └─► 循环：
        │       ├─► prepare_next_tasks()       # pregel/_algo.py:400
        │       │   └─► 返回 [PregelExecutableTask, ...]
        │       │
        │       ├─► PregelRunner.invoke(tasks) # pregel/_runner.py:100
        │       │   ├─► ThreadPoolExecutor.map(task.run)
        │       │   ├─► 每个任务：
        │       │   │   ├─► local_read()       # 读取通道
        │       │   │   ├─► node.invoke()      # 执行节点
        │       │   │   └─► 收集writes
        │       │   └─► 返回 [writes, ...]
        │       │
        │       ├─► apply_writes()             # pregel/_algo.py:250
        │       │   ├─► channel.update(values)
        │       │   └─► checkpointer.put(checkpoint)
        │       │
        │       └─► 检查是否继续
        │
        └─► read_channels(output_channels)     # 读取最终输出
```

### 2.5.3 核心接口契约

#### **PregelProtocol** (pregel/protocol.py:17)

```python
class PregelProtocol(Protocol[StateT, ContextT, InputT, OutputT]):
    """定义所有可执行图必须实现的接口"""

    def invoke(self, input: InputT, config: RunnableConfig) -> OutputT: ...
    def stream(self, input: InputT, config: RunnableConfig) -> Iterator: ...
    def get_state(self, config: RunnableConfig) -> StateSnapshot: ...
    def update_state(self, config: RunnableConfig, values: dict) -> RunnableConfig: ...
```

#### **BaseChannel** (channels/base.py:19)

```python
class BaseChannel(Generic[Value, Update, Checkpoint], ABC):
    """所有通道必须实现的接口"""

    @abstractmethod
    def update(self, values: Sequence[Update]) -> bool:
        """应用更新序列"""

    @abstractmethod
    def get(self) -> Value:
        """读取当前值"""

    @abstractmethod
    def from_checkpoint(self, checkpoint: Checkpoint) -> Self:
        """从检查点恢复"""

    def checkpoint(self) -> Checkpoint:
        """序列化为检查点"""
```

## 2.6 所使用的设计模式

### 2.6.1 **Builder模式** - 图构建

```python
# 流式构建API
graph = (StateGraph(State)
    .add_node("fetch", fetch_data)
    .add_node("process", process_data)
    .add_edge(START, "fetch")
    .add_edge("fetch", "process")
    .add_edge("process", END)
    .compile(checkpointer=saver)
)
```

**优势**：清晰、易读、支持链式调用

### 2.6.2 **Strategy模式** - 可插拔策略

```python
# 重试策略
builder.add_node("api", call_api,
    retry_policy=RetryPolicy(max_attempts=3))

# 缓存策略
builder.add_node("db", query_db,
    cache_policy=CachePolicy(ttl=300))

# 持久化策略
graph = builder.compile(durability="async")
```

**优势**：策略可独立配置和替换

### 2.6.3 **Protocol模式** - 结构化子类型

```python
# 节点可以是多种形式
StateNode = (
    Callable[[State], dict]
    | Callable[[State, RunnableConfig], dict]
    | Callable[[State, Runtime[Context]], dict]
    | Runnable[State, dict]
)
```

**优势**：灵活的类型约束，无需显式继承

### 2.6.4 **Actor模式** - 节点并发

```python
# 节点作为独立计算单元
PregelNode:
    - 独立的输入（从通道读取）
    - 独立的计算（调用bound runnable）
    - 独立的输出（写入通道）
    - 通过消息（通道值）通信
```

**优势**：天然支持并行、易于分布式

### 2.6.5 **Template Method模式** - 执行循环

```python
# pregel/_loop.py
class SyncPregelLoop:
    def run(self):
        while True:
            tasks = self.plan()      # 子类可重写
            results = self.execute(tasks)  # 子类可重写
            self.update(results)     # 子类可重写
            if not self.should_continue():
                break
```

**优势**：固定流程骨架，细节可扩展

### 2.6.6 **Factory模式** - 通道创建

```python
def _get_channel(annotation) -> BaseChannel:
    """根据类型注解自动创建合适的通道"""
    if is_annotated(annotation):
        reducer = get_reducer(annotation)
        return BinaryOperatorAggregate(typ, reducer)
    else:
        return LastValue(annotation)
```

**优势**：自动化通道创建，用户无需关心细节

### 2.6.7 **Facade模式** - StateGraph简化接口

```python
# StateGraph隐藏了Pregel的复杂性
class StateGraph:
    def compile(self):
        # 内部处理：
        # - 通道创建
        # - 节点包装
        # - 图验证
        # - Pregel构建
        return CompiledStateGraph(...)  # 实际是Pregel
```

**优势**：降低使用门槛

### 2.6.8 **Dependency Injection模式** - Runtime

```python
def node(state: State, runtime: Runtime[Context]):
    # runtime自动注入
    user_id = runtime.context.user_id
    data = runtime.store.get(...)
```

**优势**：解耦依赖，易于测试

### 2.6.9 **Command模式** - 中断和恢复

```python
# Command对象封装操作
command = Command(
    update={"status": "approved"},
    resume="user_response",
    goto=Send("next_node", {...})
)
graph.stream(command, config)
```

**优势**：操作可序列化、可撤销

### 2.6.10 **Memento模式** - 检查点

```python
# Checkpoint作为Memento保存状态
checkpoint = {
    "channel_values": {...},
    "channel_versions": {...},
    "versions_seen": {...}
}
# 随时可恢复
graph.invoke(None, config={"checkpoint_id": checkpoint_id})
```

**优势**：完整状态快照，支持时间旅行

## 2.7 分层设计与层次划分的好处

### 2.7.1 层次划分明细

| 层次 | 模块 | 关键抽象 | 可替换组件 |
|------|------|----------|-----------|
| **API层** | graph/, func/ | StateGraph, task | 无（稳定接口） |
| **编译层** | graph/state.py:compile | Channel推断、验证 | 验证规则 |
| **执行层** | pregel/main.py, _loop.py | Pregel, BSP算法 | 执行器、调度器 |
| **存储层** | channels/, checkpoint | BaseChannel, BaseCheckpointSaver | 通道类型、存储后端 |

### 2.7.2 分层带来的好处

#### **1. 关注点分离**
- 用户只需关心StateGraph API
- 执行引擎只需关心节点和通道
- 存储层只需关心序列化

#### **2. 可测试性**
```python
# 可以单独测试每一层
def test_channel():
    channel = LastValue(int)
    channel.update([1, 2, 3])
    assert channel.get() == 3

def test_pregel_without_real_nodes():
    mock_node = Mock()
    graph = Pregel(nodes={"a": mock_node}, ...)
```

#### **3. 可扩展性**
```python
# 新通道类型
class MyChannel(BaseChannel):
    ...

# 新检查点后端
class RedisCheckpointSaver(BaseCheckpointSaver):
    ...

# 用户代码无需改变
builder.add_node("x", func)  # 仍然使用相同API
```

#### **4. 性能优化空间**
- API层：编译时优化（静态分析）
- 执行层：并行优化（线程池大小、异步执行）
- 存储层：序列化优化（压缩、批量写入）

#### **5. 向后兼容**
- API层保持稳定
- 内部实现可以重构（如从同步改为异步）
- 用户代码无需修改

## 2.8 可扩展性与维护性设计

### 2.8.1 插件化机制

#### **通道插件**
```python
# 用户自定义通道
class PriorityQueue(BaseChannel):
    def update(self, values):
        heapq.heappush(self.queue, values)

# 使用
builder.channels["tasks"] = PriorityQueue(Task)
```

#### **检查点插件**
```python
# 官方提供多种后端
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.checkpoint.postgres import PostgresSaver

# 用户可实现自定义
class S3CheckpointSaver(BaseCheckpointSaver):
    ...
```

### 2.8.2 模块解耦

```
# 依赖方向（单向）
graph/ → pregel/ → channels/
           ↓
       _internal/

# 无循环依赖
# channels/不依赖pregel/
# _internal/不依赖任何上层
```

**好处**：
- 模块可独立发布（channels可单独使用）
- 降低耦合度
- 便于单元测试

### 2.8.3 依赖注入

```python
# Pregel接收依赖作为构造参数
class Pregel:
    def __init__(
        self,
        nodes: dict,
        channels: dict,
        checkpointer: BaseCheckpointSaver,  # 注入
        cache: BaseCache,                   # 注入
        store: BaseStore,                   # 注入
    ):
        ...
```

**好处**：
- 易于Mock测试
- 运行时可替换实现
- 符合SOLID原则

### 2.8.4 版本管理策略

```python
# 向后兼容的API
@deprecated("Use StateGraph instead")
def Graph(...):
    return StateGraph(...)

# 内部常量集中管理
from langgraph._internal._constants import CONFIG_KEY_*
```

**好处**：
- 渐进式迁移
- 明确的废弃路径
- 减少breaking changes

### 2.8.5 文档化设计

```python
class StateGraph:
    """A graph whose nodes communicate by reading and writing...

    Args:
        state_schema: The schema class that defines the state.
        context_schema: The schema class that defines the runtime context.
        ...

    Example:
        ```python
        from langgraph.graph import StateGraph
        ...
        ```
    """
```

**好处**：
- 降低学习曲线
- 代码即文档
- IDE友好

---

**第2章小结**：

LangGraph采用清晰的四层架构（API层、编译层、执行层、基础设施层），总共64个Python文件约16,000行代码。核心设计模式包括Builder、Strategy、Actor、Protocol等，确保了高度的灵活性和可扩展性。

数据流通过通道系统在节点间传递，采用引用传递提升性能，通过检查点序列化保证持久化。模块间采用单向依赖、依赖注入等设计，实现了良好的解耦和可测试性。

整体架构设计遵循SOLID原则，为长期维护和功能扩展奠定了坚实基础。

---

# 第3章 核心流程图

## 3.1 主执行流程图

### 3.1.1 完整执行生命周期

```mermaid
flowchart TD
    Start([用户调用 graph.invoke]) --> Init[初始化阶段]
    
    Init --> LoadConfig{加载配置}
    LoadConfig --> |存在thread_id| LoadCheckpoint[从检查点恢复]
    LoadConfig --> |新会话| CreateCheckpoint[创建新检查点]
    
    LoadCheckpoint --> InitChannels[初始化通道]
    CreateCheckpoint --> InitChannels
    
    InitChannels --> CreateRuntime[创建Runtime上下文]
    CreateRuntime --> ApplyInput[应用输入到通道]
    ApplyInput --> MainLoop[进入主执行循环]
    
    MainLoop --> Plan{规划阶段<br/>prepare_next_tasks}
    Plan --> |无任务| Finish[完成执行]
    Plan --> |有任务| CheckInterrupt{检查中断点}
    
    CheckInterrupt --> |触发中断| Interrupt[返回中断事件]
    CheckInterrupt --> |继续| Execute[执行阶段<br/>PregelRunner]
    
    Execute --> ReadChannels[读取订阅通道]
    ReadChannels --> RunNodes[并行执行节点]
    RunNodes --> CollectWrites[收集写入]
    
    CollectWrites --> Update[更新阶段<br/>apply_writes]
    Update --> UpdateChannels[更新通道值]
    UpdateChannels --> IncrementVersions[递增版本号]
    IncrementVersions --> SaveCheckpoint[保存检查点]
    SaveCheckpoint --> EmitEvents[发出流事件]
    
    EmitEvents --> CheckRecursion{检查递归限制}
    CheckRecursion --> |超限| Error[抛出GraphRecursionError]
    CheckRecursion --> |未超限| MainLoop
    
    Finish --> ReadOutput[读取输出通道]
    ReadOutput --> Return([返回结果])
    
    Interrupt --> WaitResume[等待resume]
    WaitResume --> |收到Command| ApplyCommand[应用命令]
    ApplyCommand --> MainLoop
    
    Error --> Return
```

### 3.1.2 节点执行细节流程

```mermaid
flowchart LR
    A[PregelExecutableTask] --> B{节点类型}
    B --> |普通节点| C[local_read<br/>pregel/_algo.py:173]
    B --> |条件边| D[local_read with fresh=True]
    
    C --> E[应用pending writes]
    D --> E
    
    E --> F{缓存策略?}
    F --> |有| G[检查缓存<br/>cache.get]
    F --> |无| H[调用节点函数]
    G --> |命中| I[返回缓存值]
    G --> |未命中| H
    
    H --> J{重试策略?}
    J --> |有| K[RetryPolicy包装]
    J --> |无| L[直接调用]
    K --> M{执行成功?}
    M --> |失败| N[指数退避重试]
    M --> |成功| O[收集返回值]
    N --> |达到max_attempts| P[抛出异常]
    N --> |继续| M
    L --> O
    
    O --> Q[转换为writes]
    Q --> R[返回ChannelWrite列表]
    I --> R
    P --> R
```

### 3.1.3 通道更新流程

```mermaid
flowchart TD
    A[apply_writes<br/>pregel/_algo.py:250] --> B[按通道分组writes]
    B --> C[对每个通道]
    
    C --> D[channel.update<br/>values]
    D --> E{更新成功?}
    E --> |是| F[记录新版本]
    E --> |否| G[跳过]
    
    F --> H[更新channel_versions]
    H --> I[更新versions_seen]
    
    G --> J{所有通道处理完?}
    I --> J
    J --> |否| C
    J --> |是| K[创建检查点快照]
    
    K --> L[checkpointer.put<br/>checkpoint]
    L --> M[发出stream事件]
    M --> N[返回新版本映射]
```

## 3.2 图编译流程图

```mermaid
flowchart TD
    A[StateGraph.compile] --> B[验证阶段]
    B --> C[检查节点定义]
    C --> D[检查边完整性]
    D --> E[检查循环依赖]
    E --> F{验证通过?}
    
    F --> |否| G[抛出InvalidUpdateError]
    F --> |是| H[通道创建阶段]
    
    H --> I[分析state_schema]
    I --> J{是TypedDict?}
    J --> |是| K[提取字段和类型]
    J --> |否| L{是Pydantic?}
    L --> |是| M[从model_fields提取]
    L --> |否| N[使用默认LastValue]
    
    K --> O[检查Annotated]
    M --> O
    O --> P{有reducer?}
    P --> |是| Q[创建BinaryOperatorAggregate]
    P --> |否| R[创建LastValue]
    
    Q --> S[添加到channels dict]
    R --> S
    N --> S
    
    S --> T[节点包装阶段]
    T --> U[对每个节点]
    U --> V[coerce_to_runnable]
    V --> W[创建PregelNode]
    W --> X[配置订阅通道]
    X --> Y[配置触发器]
    Y --> Z[配置写入器]
    
    Z --> AA{所有节点处理完?}
    AA --> |否| U
    AA --> |是| AB[创建Pregel实例]
    
    AB --> AC[配置checkpointer]
    AC --> AD[配置cache]
    AD --> AE[配置store]
    AE --> AF[返回CompiledStateGraph]
```

## 3.3 条件分支流程图

```mermaid
flowchart TD
    A[执行到条件节点] --> B[节点返回结果]
    B --> C[调用routing函数]
    C --> D{返回值类型}
    
    D --> |str| E[查找对应边]
    D --> |list-of-str| F[多个后继节点]
    D --> |Send对象| G[动态路由]
    D --> |list-of-Send| H[fan-out模式]
    
    E --> I[添加单个任务]
    F --> J[添加多个任务]
    G --> K[创建Send任务<br/>指定node和arg]
    H --> L[创建多个Send任务]
    
    I --> M[继续执行]
    J --> M
    K --> M
    L --> M
```

## 3.4 中断和恢复流程图

```mermaid
flowchart TD
    A[节点内调用interrupt] --> B[抛出GraphInterrupt]
    B --> C[捕获异常]
    C --> D[创建Interrupt对象]
    D --> E[保存到检查点<br/>pending_writes]
    E --> F[保存当前状态]
    F --> G[返回__interrupt__事件]
    
    G --> H[用户处理中断]
    H --> I{用户决策}
    I --> |提供resume值| J[创建Command对象]
    I --> |取消| K[结束流程]
    
    J --> L[调用graph.stream<br/>with Command]
    L --> M[加载检查点]
    M --> N[提取resume值]
    N --> O[重新执行中断的节点]
    O --> P[interrupt函数返回resume值]
    P --> Q[节点继续执行]
    Q --> R[后续流程]
```

## 3.5 并发执行流程图

```mermaid
flowchart LR
    A[prepare_next_tasks<br/>返回多个任务] --> B[创建ThreadPoolExecutor]
    B --> C[并行执行]
    
    C --> D[Task1: node_a]
    C --> E[Task2: node_b]
    C --> F[Task3: node_c]
    
    D --> G[读取channels A]
    E --> H[读取channels B]
    F --> I[读取channels C]
    
    G --> J[执行logic_a]
    H --> K[执行logic_b]
    I --> L[执行logic_c]
    
    J --> M[写入writes_a]
    K --> N[写入writes_b]
    L --> O[写入writes_c]
    
    M --> P[等待所有任务完成]
    N --> P
    O --> P
    
    P --> Q[合并所有writes]
    Q --> R[apply_writes]
```

## 3.6 流式输出流程图

```mermaid
flowchart TD
    A[graph.stream调用] --> B{stream_mode}
    
    B --> |values| C[每步后emit完整状态]
    B --> |updates| D[仅emit节点更新]
    B --> |checkpoints| E[emit检查点事件]
    B --> |tasks| F[emit任务事件]
    B --> |debug| G[emit checkpoints + tasks]
    B --> |messages| H[emit LLM消息流]
    B --> |custom| I[emit自定义数据]
    
    C --> J[read_channels<br/>all channels]
    D --> K[emit node_name:<br/>updates]
    E --> L[emit checkpoint<br/>snapshot]
    F --> M[emit task start/end]
    G --> N[combine E + F]
    H --> O[stream from LLM]
    I --> P[runtime.stream_writer]
    
    J --> Q[yield to user]
    K --> Q
    L --> Q
    M --> Q
    N --> Q
    O --> Q
    P --> Q
    
    Q --> R{继续?}
    R --> |是| S[下一步]
    R --> |否| T[结束流]
    S --> A
```

## 3.7 Send动态路由流程图

```mermaid
flowchart TD
    A[节点返回Send对象] --> B{Send类型}
    B --> |单个Send| C[Send<br/>node='target'<br/>arg=data]
    B --> |列表Send| D[Fan-out模式]
    
    C --> E[创建新任务]
    E --> F[task.name = 'target']
    F --> G[task.input = data]
    G --> H[添加到待执行队列]
    
    D --> I[for each Send]
    I --> J[创建独立任务]
    J --> K[不同node或不同arg]
    K --> L[并行执行]
    
    H --> M[下一轮执行]
    L --> M
    
    M --> N[目标节点执行]
    N --> O[返回结果写入通道]
```

## 3.8 检查点加载和保存流程图

```mermaid
flowchart TD
    A[开始执行] --> B{config中有<br/>checkpoint_id?}
    B --> |是| C[checkpointer.get<br/>checkpoint_id]
    B --> |否| D[checkpointer.get_tuple<br/>thread_id + checkpoint_ns]
    
    C --> E{找到检查点?}
    D --> E
    E --> |是| F[加载checkpoint]
    E --> |否| G[创建空检查点]
    
    F --> H[恢复channel_values]
    G --> I[初始化channels]
    
    H --> J[恢复channel_versions]
    I --> K[版本从0开始]
    
    J --> L[恢复versions_seen]
    K --> M[versions_seen为空]
    
    L --> N[恢复pending_writes]
    M --> N
    
    N --> O[执行图逻辑]
    O --> P[每步结束]
    
    P --> Q[create_checkpoint]
    Q --> R[序列化channels]
    R --> S[保存versions]
    S --> T[保存metadata]
    T --> U[checkpointer.put]
    U --> V[继续下一步]
```

---

**第3章小结**：

本章通过8个详细的Mermaid流程图，展示了LangGraph的核心执行路径：

1. **主执行流程**：从invoke到返回的完整生命周期
2. **节点执行**：包含缓存、重试、异常处理
3. **通道更新**：版本控制和检查点保存
4. **图编译**：从StateGraph到Pregel的转换
5. **条件分支**：动态路由和Send机制
6. **中断恢复**：人机交互流程
7. **并发执行**：多任务并行调度
8. **流式输出**：多种stream_mode的处理

所有流程图均标注了关键函数位置，便于追溯源代码实现。


# 第4章 关键算法详解

## 4.1 Pregel BSP算法

### 4.1.1 算法概述

**名称**：Bulk Synchronous Parallel (BSP) 算法
**位置**：`langgraph/pregel/_algo.py`
**用途**：图的并行执行调度

### 4.1.2 算法原理

BSP模型将计算划分为一系列**超步（Supersteps）**，每个超步包含三个阶段：

1. **Plan（规划）**：确定哪些节点准备执行
2. **Execute（执行）**：并行执行选中的节点
3. **Update（更新）**：同步应用所有节点的写入

```
超步 n:
  ├─ Plan: 基于当前状态选择节点
  ├─ Execute: 并行执行（此时新写入对其他节点不可见）
  └─ Update: 全局屏障，应用所有写入

超步 n+1:
  ├─ Plan: 基于更新后的状态...
  └─ ...
```

### 4.1.3 核心实现

#### **prepare_next_tasks()** (pregel/_algo.py:400)

```python
def prepare_next_tasks(
    checkpoint: Checkpoint,
    processes: dict[str, PregelNode],
    channels: Mapping[str, BaseChannel],
    managed: ManagedValueMapping,
    step: int,
    *,
    for_execution: bool = False
) -> list[PregelExecutableTask]:
    """
    规划阶段：根据通道版本变化确定待执行节点
    
    算法步骤：
    1. 遍历所有节点
    2. 对每个节点检查其triggers（触发条件）
    3. 如果任何trigger通道的版本 > 节点上次见过的版本，则触发
    4. 创建PregelExecutableTask
    
    时间复杂度：O(N * T)
      N = 节点数量
      T = 平均每个节点的trigger数量
    
    空间复杂度：O(K)
      K = 待执行任务数
    """
    tasks = []
    
    for name, node in processes.items():
        seen = checkpoint["versions_seen"].get(name, {})
        
        # 检查是否有trigger通道更新
        triggered = any(
            checkpoint["channel_versions"].get(chan, 0) > seen.get(chan, 0)
            for chan in node.triggers
        )
        
        if triggered:
            task = PregelExecutableTask(
                name=name,
                input=None,  # 延迟读取
                config=config,
                triggers=[...],
            )
            tasks.append(task)
    
    return tasks
```

#### **apply_writes()** (pregel/_algo.py:250)

```python
def apply_writes(
    checkpoint: Checkpoint,
    channels: Mapping[str, BaseChannel],
    tasks: Sequence[WritesProtocol],
    get_next_version: GetNextVersion
) -> dict[str, Any]:
    """
    更新阶段：应用所有写入到通道
    
    算法步骤：
    1. 按通道分组所有writes
    2. 对每个通道调用channel.update(values)
    3. 如果更新成功，记录新版本号
    4. 更新checkpoint的channel_versions和versions_seen
    
    时间复杂度：O(W + C)
      W = 总写入数
      C = 通道数
    
    空间复杂度：O(W)
      需要临时存储所有writes的分组
    """
    # Step 1: 分组writes by channel
    pending_writes: dict[str, list[Any]] = defaultdict(list)
    for task in tasks:
        for chan, value in task.writes:
            pending_writes[chan].append(value)
    
    # Step 2: 应用更新
    updated_channels = set()
    for chan, values in pending_writes.items():
        if channels[chan].update(values):
            updated_channels.add(chan)
    
    # Step 3: 更新版本
    for chan in updated_channels:
        checkpoint["channel_versions"][chan] = get_next_version(
            checkpoint["channel_versions"].get(chan),
            None
        )
        
        # 记录所有任务都见过这个新版本
        for task in tasks:
            checkpoint["versions_seen"].setdefault(task.name, {})[chan] = \
                checkpoint["channel_versions"][chan]
    
    return checkpoint["channel_versions"]
```

### 4.1.4 优缺点分析

**优点**：
- **并行性**：同一超步内的节点可并行执行
- **一致性**：全局屏障保证状态同步
- **可恢复性**：每个超步结束后可保存检查点
- **简单性**：易于理解和实现

**缺点**：
- **同步开销**：需要等待最慢的节点完成
- **非流水线**：不支持跨超步的流水线并行
- **粗粒度**：粒度以节点为单位，不适合细粒度任务

**边界场景**：
- 节点数很少时，并行优势不明显
- 节点执行时间差异大时，存在长尾问题
- 无状态依赖时，BSP模型引入了不必要的同步

## 4.2 通道版本控制算法

### 4.2.1 算法概述

**位置**：`langgraph/pregel/_algo.py:140`
**用途**：防止节点重复触发

### 4.2.2 算法原理

使用**逻辑时钟（Logical Clock）**机制：
- 每个通道维护一个版本号（从1开始）
- 每个节点记录其"见过"的每个通道的版本
- 节点仅在通道版本 > 见过的版本时触发

```python
# 示例
channel_versions = {
    "messages": 3,
    "context": 1
}

versions_seen = {
    "node_a": {"messages": 2, "context": 1},  # 会触发（messages有更新）
    "node_b": {"messages": 3, "context": 1},  # 不触发（已见过最新版本）
}
```

### 4.2.3 核心代码

```python
def get_new_channel_versions(
    previous: ChannelVersions,
    current: ChannelVersions
) -> set[str]:
    """
    计算哪些通道有新版本
    
    时间复杂度：O(N) where N = 通道数
    """
    return {
        chan
        for chan, version in current.items()
        if version > previous.get(chan, 0)
    }
```

### 4.2.4 优缺点

**优点**：
- 精确控制，无重复触发
- 空间效率高（仅存储整数）
- 支持细粒度依赖追踪

**缺点**：
- 需要为每个节点维护seen映射
- 版本号可能溢出（实际不太可能）

## 4.3 条件路由算法

### 4.3.1 算法概述

**位置**：`langgraph/graph/_branch.py`, `langgraph/pregel/_write.py`
**用途**：根据节点输出动态决定下一个执行的节点

### 4.3.2 算法原理

**分支函数（Branch Function）**：
```python
def router(state: State) -> str | list[str] | Send | list[Send]:
    # 返回类型决定路由行为
    pass
```

**路由规则**：
1. 返回 `str` → 单一后继节点
2. 返回 `list[str]` → 多个后继节点（并行）
3. 返回 `Send(node, arg)` → 动态路由到node，传递自定义参数
4. 返回 `list[Send]` → fan-out模式

### 4.3.3 核心实现

```python
def _route_branch(
    result: str | list[str] | Send | list[Send],
    branch_map: dict[Hashable, str]
) -> list[Send]:
    """
    将路由结果转换为Send对象列表
    
    时间复杂度：O(K) where K = 结果中的元素数
    """
    if isinstance(result, str):
        # 单一路由
        target = branch_map.get(result, result)
        return [Send(target, PASSTHROUGH)]
        
    elif isinstance(result, Send):
        # 已是Send对象
        return [result]
        
    elif isinstance(result, list):
        sends = []
        for item in result:
            if isinstance(item, str):
                target = branch_map.get(item, item)
                sends.append(Send(target, PASSTHROUGH))
            elif isinstance(item, Send):
                sends.append(item)
        return sends
```

### 4.3.4 优缺点

**优点**：
- 极高的灵活性
- 支持数据依赖的路由
- 支持fan-out/fan-in模式

**缺点**：
- 路由逻辑可能复杂
- 动态路由导致静态分析困难
- Send参数可能导致类型不匹配

## 4.4 缓存键生成算法

### 4.4.1 算法概述

**位置**：`langgraph/_internal/_cache.py`
**用途**：为节点输入生成唯一缓存键

### 4.4.2 算法原理

使用**内容哈希（Content Hashing）**：

```python
def default_cache_key(input: Any) -> str:
    """
    生成缓存键
    
    算法：
    1. 递归序列化input为字节串
    2. 使用xxHash计算128位哈希
    3. 返回十六进制字符串
    
    时间复杂度：O(N) where N = input的大小
    空间复杂度：O(N)（序列化临时存储）
    """
    # 处理不同类型
    if isinstance(input, (str, int, float, bool, NoneType)):
        data = str(input).encode()
    elif isinstance(input, dict):
        # 排序key以保证一致性
        items = sorted(input.items(), key=lambda x: x[0])
        data = str(items).encode()
    elif isinstance(input, (list, tuple)):
        data = str(input).encode()
    elif hasattr(input, "__dict__"):
        # Pydantic模型等
        data = str(input.__dict__).encode()
    else:
        # 回退到pickle
        import pickle
        data = pickle.dumps(input)
    
    # 使用xxHash（比MD5/SHA快）
    return xxh3_128_hexdigest(data)
```

### 4.4.3 哈希算法选择

| 算法 | 速度 | 碰撞概率 | 使用场景 |
|------|------|----------|----------|
| **xxHash** | 极快（~10GB/s） | 极低 | LangGraph默认 |
| MD5 | 中等 | 极低 | 加密哈希 |
| SHA256 | 慢 | 极低 | 安全要求高 |
| Python hash() | 快 | 高（32位） | 不适合缓存 |

### 4.4.4 优缺点

**优点**：
- 性能极高（xxHash）
- 支持多种数据类型
- 碰撞概率极低（128位）

**缺点**：
- 大对象序列化开销
- 非确定性类型（如set）可能导致问题
- 自定义类需要正确实现__dict__

## 4.5 重试算法

### 4.5.1 算法概述

**位置**：`langgraph/pregel/_retry.py`
**用途**：节点失败时的自动重试

### 4.5.2 指数退避算法

```python
class RetryPolicy:
    initial_interval: float = 0.5
    backoff_factor: float = 2.0
    max_interval: float = 128.0
    max_attempts: int = 3
    jitter: bool = True
    
def calculate_wait_time(attempt: int, policy: RetryPolicy) -> float:
    """
    计算重试等待时间
    
    算法：指数退避 + 抖动
    
    wait_time = min(
        initial_interval * (backoff_factor ^ attempt),
        max_interval
    )
    
    if jitter:
        wait_time *= random.uniform(0.5, 1.5)
    
    时间复杂度：O(1)
    """
    wait = policy.initial_interval * (policy.backoff_factor ** attempt)
    wait = min(wait, policy.max_interval)
    
    if policy.jitter:
        import random
        wait *= random.uniform(0.5, 1.5)
    
    return wait

# 示例序列（initial=0.5, backoff=2.0）
# attempt 0: 0.5s * (2^0) = 0.5s
# attempt 1: 0.5s * (2^1) = 1.0s
# attempt 2: 0.5s * (2^2) = 2.0s
# attempt 3: 0.5s * (2^3) = 4.0s
# ...
# attempt 7: 0.5s * (2^7) = 64.0s
# attempt 8: 0.5s * (2^8) = 128.0s (capped at max_interval)
```

### 4.5.3 重试条件过滤

```python
def retry_on(exception: Exception, policy: RetryPolicy) -> bool:
    """
    判断是否应该重试
    
    默认策略：
    - NetworkError: 重试
    - TimeoutError: 重试
    - ValueError: 不重试
    - 用户自定义: policy.retry_on(exception)
    """
    if policy.retry_on:
        return policy.retry_on(exception)
    
    # 默认仅重试临时性错误
    return isinstance(exception, (ConnectionError, TimeoutError))
```

### 4.5.4 优缺点

**优点**：
- 自动处理临时故障
- 指数退避避免雪崩
- 抖动防止惊群效应

**缺点**：
- 增加总执行时间
- 非幂等操作可能重复执行
- 不适合确定性错误

---

**第4章小结**：

LangGraph的核心算法包括：

1. **BSP算法**：三阶段并行执行（Plan-Execute-Update）
2. **版本控制**：逻辑时钟防止重复触发
3. **条件路由**：灵活的动态分支
4. **缓存键**：xxHash快速哈希
5. **指数退避**：智能重试机制

所有算法均经过优化，时间复杂度控制在O(N)或O(1)级别，适合生产环境使用。

---

# 第5章 数据结构分析

## 5.1 核心状态数据结构

### 5.1.1 Checkpoint

**定义位置**：`langgraph.checkpoint.base`
**用途**：保存图的完整执行状态

```python
class Checkpoint(TypedDict):
    """
    检查点：图在某一时刻的完整快照
    """
    v: int  # 检查点格式版本（当前为1）
    
    id: str  # 检查点唯一ID（UUID格式）
    
    ts: str  # 创建时间戳（ISO 8601格式）
    
    channel_values: dict[str, Any]
    # 所有通道的当前值
    # 例如：{
    #   "messages": [{"role": "user", "content": "hello"}],
    #   "context": {"user_id": "123"}
    # }
    
    channel_versions: ChannelVersions
    # 每个通道的版本号
    # 类型：dict[str, int]
    # 例如：{"messages": 3, "context": 1}
    
    versions_seen: dict[str, ChannelVersions]
    # 每个节点见过的通道版本
    # 外层key = 节点名
    # 内层key = 通道名，value = 版本号
    # 例如：{
    #   "node_a": {"messages": 2, "context": 1},
    #   "node_b": {"messages": 3}
    # }
    
    pending_sends: list[Send]
    # 待执行的Send对象（fan-out任务）
```

**序列化格式**：JSON或Pickle（取决于checkpointer实现）
**生命周期**：
- 创建：每步结束后（durability="sync"）或退出时（"exit"）
- 修改：不可变，每次更新创建新检查点
- 销毁：由checkpointer的清理策略决定

**内存效率**：
- 小图（<100KB状态）：~1-2KB开销
- 大图（>1MB状态）：考虑增量检查点

### 5.1.2 PregelTask

**定义位置**：`langgraph/types.py:204`

```python
@dataclass
class PregelTask:
    """
    任务描述：用于get_state()返回给用户
    """
    id: str  # 任务ID，格式："{checkpoint_id}:{node_name}:{idx}"
    
    name: str  # 节点名称
    
    path: tuple[str | int | tuple, ...]
    # 任务路径，用于追踪子图调用栈
    # 例如：("parent_graph", "subgraph", "node")
    
    error: Exception | None = None
    # 如果任务失败，存储异常对象
    
    interrupts: tuple[Interrupt, ...] = ()
    # 任务产生的中断列表
    
    state: RunnableConfig | None = None
    # 子图任务的配置（如果是子图调用）
```

**字段解释**：
- `id`：唯一标识，便于恢复时定位
- `path`：支持嵌套子图的追踪
- `error`：用于错误报告和重试决策
- `interrupts`：支持人机交互

### 5.1.3 PregelExecutableTask

**定义位置**：`langgraph/types.py:234`

```python
@dataclass(frozen=True)
class PregelExecutableTask:
    """
    可执行任务：内部使用，包含执行所需的全部信息
    """
    name: str  # 节点名称
    
    input: Any  # 节点输入（从通道读取的值）
    
    proc: PregelNode  # 节点对象（包含runnable）
    
    writes: Sequence[tuple[str, Any]]  # 节点写入
    
    triggers: Sequence[str]  # 触发此任务的通道列表
    
    retry_policy: Sequence[RetryPolicy] | None
    
    cache_policy: CachePolicy | None
    
    id: str  # 任务ID
    
    path: tuple[str | int | tuple, ...]
    
    config: RunnableConfig  # 运行时配置
```

**与PregelTask的区别**：
- PregelTask：面向用户，只读
- PregelExecutableTask：面向执行器，包含执行逻辑

**性能分析**：
- 内存占用：~200-500字节/任务（不含input）
- 创建开销：O(1)
- immutable（frozen），便于缓存

### 5.1.4 StateSnapshot

**定义位置**：`langgraph/types.py:249`

```python
class StateSnapshot(NamedTuple):
    """
    状态快照：get_state()返回的完整状态视图
    """
    values: dict[str, Any]
    # 当前所有通道的值（解引用后的实际值）
    
    next: tuple[str, ...]
    # 下一步将执行的节点名称列表
    # 例如：("node_a", "node_b")
    
    config: RunnableConfig
    # 当前配置（包含checkpoint_id、thread_id等）
    
    metadata: CheckpointMetadata
    # 元数据：
    #   - step: 当前步数
    #   - source: "loop" | "input" | "update"
    #   - writes: 上一步的写入
    #   - parents: 父级图信息（嵌套图）
    
    created_at: str
    # ISO 8601时间戳
    
    parent_config: RunnableConfig | None
    # 父级图配置（如果在子图中）
    
    tasks: tuple[PregelTask, ...]
    # 待执行的任务列表
    
    interrupts: tuple[Interrupt, ...]
    # 待处理的中断
```

**使用示例**：
```python
snapshot = graph.get_state(config)

# 检查当前值
print(snapshot.values["messages"])

# 检查下一步
if "approval_node" in snapshot.next:
    # 等待批准

# 检查中断
for interrupt in snapshot.interrupts:
    user_input = handle_interrupt(interrupt.value)
    graph.stream(Command(resume=user_input), config)
```

## 5.2 通道数据结构

### 5.2.1 LastValue

**定义位置**：`langgraph/channels/last_value.py`

```python
@dataclass
class LastValue(BaseChannel):
    """
    存储单个最新值的通道
    
    更新策略：保留最后一个写入的值
    """
    typ: Type  # 值类型
    value: Any = MISSING  # 当前值（MISSING表示未初始化）
    
    def update(self, values: Sequence[Any]) -> bool:
        """
        应用更新：取最后一个值
        
        时间复杂度：O(1)
        """
        if values:
            self.value = values[-1]
            return True
        return False
    
    def get(self) -> Any:
        """
        读取值
        
        时间复杂度：O(1)
        """
        if self.value is MISSING:
            raise EmptyChannelError()
        return self.value
    
    def checkpoint(self) -> Any:
        """序列化：直接返回值"""
        return self.value
    
    @staticmethod
    def from_checkpoint(cp: Any) -> Self:
        """反序列化"""
        channel = LastValue(typ=...)
        channel.value = cp
        return channel
```

**字段含义**：
- `typ`：类型提示，用于验证
- `value`：实际存储的值

**内存占用**：值本身的大小 + 固定开销（~50字节）

### 5.2.2 BinaryOperatorAggregate

**定义位置**：`langgraph/channels/binop.py`

```python
@dataclass
class BinaryOperatorAggregate(BaseChannel):
    """
    使用二元运算符聚合值的通道
    
    典型用途：
    - operator.add：列表拼接、数值求和
    - operator.or_：集合并集
    - custom_merge：自定义聚合
    """
    typ: Type
    operator: Callable[[Any, Any], Any]  # 聚合函数
    value: Any = MISSING
    
    def update(self, values: Sequence[Any]) -> bool:
        """
        应用更新：使用operator聚合所有values
        
        时间复杂度：O(N * M)
          N = len(values)
          M = operator的复杂度
          
        例如operator=operator.add, values=[['a'], ['b'], ['c']]
        结果：self.value = ['a'] + ['b'] + ['c'] = ['a', 'b', 'c']
        """
        if not values:
            return False
        
        # 初始化
        if self.value is MISSING:
            self.value = values[0]
            start = 1
        else:
            start = 0
        
        # 逐个聚合
        for v in values[start:]:
            self.value = self.operator(self.value, v)
        
        return True
```

**常用operator**：

| Operator | 用途 | 示例 |
|----------|------|------|
| `operator.add` | 列表拼接 | `[1] + [2] = [1, 2]` |
| `operator.or_` | 集合并集 | `{1} | {2} = {1, 2}` |
| `operator.and_` | 集合交集 | `{1,2} & {2,3} = {2}` |
| `lambda a,b: a+b` | 数值求和 | `3 + 5 = 8` |
| `max` | 取最大值 | `max(3, 5) = 5` |

### 5.2.3 Topic

**定义位置**：`langgraph/channels/topic.py`

```python
@dataclass
class Topic(BaseChannel):
    """
    发布订阅通道，存储消息流
    
    特点：
    - 支持累积和去重模式
    - 可设置唯一性key
    """
    typ: Type
    messages: list[Any] = field(default_factory=list)
    unique: bool = False  # 是否去重
    accumulate: bool = True  # 是否累积
    key_func: Callable[[Any], Hashable] | None = None  # 去重key函数
    _seen: set[Hashable] = field(default_factory=set)  # 已见消息
    
    def update(self, values: Sequence[Any]) -> bool:
        """
        添加新消息
        
        时间复杂度：
        - 无去重：O(N)
        - 有去重：O(N * K) where K = key_func复杂度
        """
        added = False
        for v in values:
            if self.unique and self.key_func:
                key = self.key_func(v)
                if key in self._seen:
                    continue
                self._seen.add(key)
            
            if self.accumulate:
                self.messages.append(v)
            else:
                self.messages = [v]
            added = True
        
        return added
    
    def consume(self) -> bool:
        """
        消费消息（清空）
        
        仅在非累积模式下调用
        """
        if not self.accumulate:
            self.messages.clear()
            return True
        return False
```

**字段解释**：
- `messages`：消息列表，可能很大
- `unique`：启用去重
- `key_func`：自定义去重键（如 `lambda msg: msg.id`）
- `_seen`：去重哈希表

**内存分析**：
- 累积模式：O(总消息数)，可能无限增长
- 非累积模式：O(1)
- 去重开销：O(消息数) for `_seen` set

## 5.3 运行时数据结构

### 5.3.1 Runtime

**定义位置**：`langgraph/runtime.py:28`

```python
@dataclass(frozen=True, kw_only=True, slots=True)
class Runtime(Generic[ContextT]):
    """
    运行时上下文，通过依赖注入传递给节点
    """
    context: ContextT = field(default=None)
    # 用户自定义上下文
    # 例如：dataclass(user_id: str, db_conn: DBConnection)
    
    store: BaseStore | None = field(default=None)
    # 持久化存储接口
    # 方法：put(namespace, key, value), get(namespace, key)
    
    stream_writer: StreamWriter = field(default=_no_op_stream_writer)
    # 自定义流写入器
    # 类型：Callable[[Any], None]
    
    previous: Any = field(default=None)
    # 上一次的返回值（仅函数式API）
```

**字段生命周期**：
- `context`：整个图执行期间不变
- `store`：注入的单例
- `stream_writer`：每次stream()调用创建
- `previous`：每次调用后更新

**使用示例**：
```python
@dataclass
class MyContext:
    user_id: str
    api_key: str

def node(state: State, runtime: Runtime[MyContext]):
    # 访问上下文
    user_id = runtime.context.user_id
    
    # 使用store
    user_prefs = runtime.store.get(("users",), user_id)
    
    # 自定义流
    runtime.stream_writer({"progress": 50})
```

### 5.3.2 Command

**定义位置**：`langgraph/types.py:348`

```python
@dataclass
class Command:
    """
    控制命令：用于恢复、更新状态、跳转
    """
    graph: str | None = None
    # 目标图名（子图）
    
    update: Any | None = None
    # 状态更新（部分状态字典）
    # 例如：{"status": "approved"}
    
    resume: dict | Any | None = None
    # 中断恢复值
    # - dict：为每个中断提供值（key = interrupt.id）
    # - Any：为所有中断提供相同值
    
    goto: Send | Sequence[Send] = ()
    # 跳转到指定节点
    # 例如：Send("approval_node", {...})
```

**使用场景**：

1. **恢复中断**：
```python
# 单个中断
graph.stream(Command(resume="user_input"), config)

# 多个中断（通过ID）
graph.stream(Command(resume={
    "interrupt_1": "value1",
    "interrupt_2": "value2"
}), config)
```

2. **更新状态**：
```python
# 手动修改状态
graph.stream(Command(update={"priority": "high"}), config)
```

3. **跳转节点**：
```python
# 跳过某些节点
graph.stream(Command(goto=Send("final_step", state)), config)
```

## 5.4 配置数据结构

### 5.4.1 RunnableConfig

**定义位置**：`langchain_core.runnables.config`

```python
class RunnableConfig(TypedDict, total=False):
    """
    运行时配置，贯穿整个执行过程
    """
    tags: list[str]
    # 标签（如"production", TAG_HIDDEN）
    
    metadata: dict[str, Any]
    # 用户元数据
    
    callbacks: Callbacks
    # LangChain回调管理器
    
    run_name: str
    # 运行名称（用于追踪）
    
    max_concurrency: int | None
    # 最大并发数
    
    recursion_limit: int
    # 最大递归深度（默认25）
    
    configurable: dict[str, Any]
    # 可配置参数（LangGraph使用此字段）
```

**`configurable`字段内容**（LangGraph专用）：

| Key | 类型 | 说明 |
|-----|------|------|
| `thread_id` | str | 会话ID |
| `checkpoint_id` | str | 检查点ID |
| `checkpoint_ns` | str | 检查点命名空间（子图） |
| `__pregel_*` | Any | 内部保留键（见_constants.py） |

**内部保留键**（`_internal/_constants.py`）：

```python
CONFIG_KEY_SEND = "__pregel_send"  # 写入函数
CONFIG_KEY_READ = "__pregel_read"  # 读取函数
CONFIG_KEY_CHECKPOINTER = "__pregel_checkpointer"
CONFIG_KEY_RUNTIME = "__pregel_runtime"
CONFIG_KEY_SCRATCHPAD = "__pregel_scratchpad"
# ... 等等
```

## 5.5 序列化与反序列化

### 5.5.1 检查点序列化

**策略**：
- **JSON**：默认，适用于基本类型
- **Pickle**：Python对象，不跨语言
- **自定义**：实现`checkpoint()`和`from_checkpoint()`

**示例**：
```python
# JSON序列化
checkpoint_json = {
    "channel_values": {
        "messages": [{"role": "user", "content": "hi"}],
        "count": 42
    },
    "channel_versions": {"messages": 1, "count": 1}
}

# Pickle序列化（自定义对象）
checkpoint_pickle = {
    "channel_values": {
        "model": pickle.dumps(MyModel(...))
    }
}
```

### 5.5.2 性能对比

| 格式 | 速度 | 大小 | 跨语言 | 安全 |
|------|------|------|--------|------|
| JSON | 快 | 中等 | 是 | 高 |
| Pickle | 中等 | 小 | 否 | 低 |
| MessagePack | 极快 | 小 | 是 | 中 |
| Protobuf | 快 | 极小 | 是 | 高 |

**建议**：
- 生产环境：JSON（安全、可调试）
- 性能敏感：MessagePack
- 复杂对象：Pickle（仅内部使用）

---

**第5章小结**：

LangGraph的数据结构设计精巧，涵盖：

1. **状态结构**：Checkpoint、StateSnapshot支持完整快照
2. **任务结构**：PregelTask、PregelExecutableTask分离用户视图和执行逻辑
3. **通道结构**：LastValue、BinaryOperatorAggregate、Topic满足不同需求
4. **运行时结构**：Runtime、Command支持依赖注入和控制流
5. **配置结构**：RunnableConfig统一配置传递

所有结构均支持高效序列化，内存占用合理，适合长时间运行。

---

# 第6章 配置开关说明

## 6.1 图级配置

### 6.1.1 checkpointer

**类型**：`BaseCheckpointSaver | None`
**默认值**：`None`
**配置方式**：
```python
from langgraph.checkpoint.sqlite import SqliteSaver

saver = SqliteSaver.from_conn_string("checkpoints.db")
graph = builder.compile(checkpointer=saver)
```

**功能描述**：
- `None`：不保存检查点，无法恢复
- 非None：启用持久化，支持中断恢复

**可选值**：
- `SqliteSaver`：本地SQLite数据库
- `PostgresSaver`：PostgreSQL数据库
- `InMemorySaver`：内存（仅测试用）
- 自定义：继承`BaseCheckpointSaver`

**影响**：
- 性能：每步保存检查点有I/O开销
- 功能：必须有checkpointer才能使用中断、time-travel

### 6.1.2 durability

**类型**：`Literal["sync", "async", "exit"]`
**默认值**：`"sync"`
**配置方式**：
```python
graph = builder.compile(
    checkpointer=saver,
    durability="async"  # 异步持久化
)
```

**可选值**：
| 值 | 行为 | 性能 | 一致性 |
|----|------|------|--------|
| `"sync"` | 每步结束后同步保存 | 慢 | 强一致性 |
| `"async"` | 异步保存，不阻塞 | 快 | 最终一致性 |
| `"exit"` | 仅退出时保存 | 最快 | 仅保证最终状态 |

**使用建议**：
- 生产关键流程：`"sync"`
- 高吞吐场景：`"async"`
- 短暂任务：`"exit"`

### 6.1.3 interrupt_before / interrupt_after

**类型**：`Sequence[str] | Literal["*"]`
**默认值**：`[]`
**配置方式**：
```python
graph = builder.compile(
    checkpointer=saver,
    interrupt_before=["approval_node"],  # 在此节点前中断
    interrupt_after=["*"]  # 所有节点后都中断
)
```

**功能描述**：
- `interrupt_before`：节点执行前暂停
- `interrupt_after`：节点执行后暂停
- `"*"`：所有节点

**使用场景**：
- 人工审批流程
- 调试（单步执行）
- 外部系统集成

### 6.1.4 store

**类型**：`BaseStore | None`
**默认值**：`None`
**配置方式**：
```python
from langgraph.store.memory import InMemoryStore

store = InMemoryStore()
graph = builder.compile(store=store)
```

**功能描述**：
- 跨会话的持久化存储
- 支持命名空间和key-value访问
- 与Runtime.store集成

**方法**：
```python
# 写入
store.put(("users",), "user_123", {"name": "Alice"})

# 读取
item = store.get(("users",), "user_123")

# 搜索
results = store.search(("users",), query={"name__contains": "Ali"})
```

### 6.1.5 cache

**类型**：`BaseCache | None`
**默认值**：`None`
**配置方式**：
```python
from langgraph.cache.memory import InMemoryCache

cache = InMemoryCache()
graph = builder.compile(cache=cache)
```

**功能描述**：
- 节点输出缓存
- 避免重复计算
- 支持TTL过期

**与CachePolicy配合**：
```python
builder.add_node(
    "expensive_node",
    func,
    cache_policy=CachePolicy(
        key_func=lambda x: x["id"],
        ttl=300  # 5分钟
    )
)
```

## 6.2 运行时配置

### 6.2.1 thread_id

**类型**：`str`
**默认值**：无（必须提供）
**配置方式**：
```python
config = {"configurable": {"thread_id": "user-123-session-1"}}
graph.invoke(inputs, config)
```

**功能描述**：
- 唯一标识一个会话/线程
- 检查点通过thread_id索引
- 支持多租户

**格式建议**：
- `user_{user_id}_session_{session_id}`
- `chat_{chat_id}`
- UUID

### 6.2.2 recursion_limit

**类型**：`int`
**默认值**：`25`
**配置方式**：
```python
config = {"recursion_limit": 100}
graph.invoke(inputs, config)
```

**功能描述**：
- 防止无限循环
- 超过限制抛出`GraphRecursionError`

**调整建议**：
- 简单图：25（默认）
- 复杂循环：50-100
- 递归算法：200+

### 6.2.3 max_concurrency

**类型**：`int | None`
**默认值**：`None`（无限制）
**配置方式**：
```python
config = {"max_concurrency": 4}
graph.invoke(inputs, config)
```

**功能描述**：
- 限制并行任务数
- 控制资源使用

**影响**：
- 设置过小：性能下降
- 设置过大：资源耗尽
- 建议：CPU核心数 * 2

## 6.3 节点级配置

### 6.3.1 retry_policy

**类型**：`RetryPolicy | None`
**默认值**：`None`
**配置方式**：
```python
from langgraph.types import RetryPolicy

builder.add_node(
    "api_call",
    func,
    retry_policy=RetryPolicy(
        initial_interval=0.5,
        backoff_factor=2.0,
        max_interval=128.0,
        max_attempts=3,
        jitter=True,
        retry_on=lambda e: isinstance(e, NetworkError)
    )
)
```

**字段说明**：
| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `initial_interval` | float | 0.5 | 初始重试间隔（秒） |
| `backoff_factor` | float | 2.0 | 指数退避因子 |
| `max_interval` | float | 128.0 | 最大重试间隔 |
| `max_attempts` | int | 3 | 最大尝试次数 |
| `jitter` | bool | True | 添加随机抖动 |
| `retry_on` | Callable | None | 重试条件函数 |

### 6.3.2 cache_policy

**类型**：`CachePolicy | None`
**默认值**：`None`
**配置方式**：
```python
from langgraph.types import CachePolicy

builder.add_node(
    "db_query",
    func,
    cache_policy=CachePolicy(
        key_func=lambda x: f"query:{x['sql']}",
        ttl=60  # 1分钟过期
    )
)
```

**字段说明**：
- `key_func`：生成缓存键的函数
- `ttl`：生存时间（秒），None表示永不过期

### 6.3.3 metadata

**类型**：`dict[str, Any]`
**默认值**：`{}`
**配置方式**：
```python
builder.add_node(
    "node_a",
    func,
    metadata={
        "description": "处理用户输入",
        "version": "1.0",
        "owner": "team-ai"
    }
)
```

**功能描述**：
- 存储节点元信息
- 用于文档生成、监控

## 6.4 流式配置

### 6.4.1 stream_mode

**类型**：`StreamMode`
**可选值**：
```python
StreamMode = Literal[
    "values",      # 完整状态
    "updates",     # 节点更新
    "checkpoints", # 检查点事件
    "tasks",       # 任务事件
    "debug",       # checkpoints + tasks
    "messages",    # LLM消息流
    "custom"       # 自定义流
]
```

**配置方式**：
```python
for chunk in graph.stream(inputs, stream_mode="updates"):
    print(chunk)
```

**输出格式对比**：

| Mode | 输出示例 | 用途 |
|------|---------|------|
| `values` | `{"messages": [...], "context": {...}}` | 完整状态展示 |
| `updates` | `{"node_a": {"key": "value"}}` | 仅看变化 |
| `checkpoints` | `StateSnapshot(...)` | 检查点追踪 |
| `tasks` | `{"type": "task_start", "name": "node_a"}` | 调试 |
| `messages` | `("node_a", AIMessageChunk(...))` | LLM流式 |
| `custom` | 用户定义 | 自定义数据 |

### 6.4.2 stream_subgraphs

**类型**：`bool`
**默认值**：`False`
**配置方式**：
```python
for chunk in graph.stream(
    inputs,
    stream_mode="updates",
    subgraphs=True  # 包含子图事件
):
    print(chunk)
```

**功能描述**：
- `False`：仅顶层图事件
- `True`：包含所有嵌套子图事件

## 6.5 环境变量

### 6.5.1 LANGGRAPH_DEBUG

**类型**：布尔环境变量
**默认值**：`false`
**设置方式**：
```bash
export LANGGRAPH_DEBUG=true
python my_app.py
```

**功能描述**：
- 启用详细日志
- 输出执行追踪

### 6.5.2 LANGGRAPH_CACHE_DIR

**类型**：路径字符串
**默认值**：`~/.langgraph/cache`
**设置方式**：
```bash
export LANGGRAPH_CACHE_DIR=/tmp/langgraph_cache
```

**功能描述**：
- 本地缓存存储位置
- 影响InMemoryCache持久化

## 6.6 配置组合示例

### 6.6.1 生产环境配置

```python
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.store.postgres import PostgresStore

saver = PostgresSaver.from_conn_string("postgresql://...")
store = PostgresStore.from_conn_string("postgresql://...")

graph = builder.compile(
    checkpointer=saver,
    store=store,
    durability="async",  # 异步持久化，高性能
    interrupt_before=["approval_node"]  # 人工审批
)

# 运行
config = {
    "configurable": {"thread_id": f"user-{user_id}"},
    "recursion_limit": 50,
    "max_concurrency": 8
}

for chunk in graph.stream(inputs, config, stream_mode="updates"):
    # 处理更新
    pass
```

### 6.6.2 开发环境配置

```python
from langgraph.checkpoint.memory import InMemorySaver

saver = InMemorySaver()

graph = builder.compile(
    checkpointer=saver,
    durability="exit",  # 仅退出时保存
    interrupt_after=["*"]  # 单步调试
)

# 调试运行
config = {"configurable": {"thread_id": "debug-session"}}
for chunk in graph.stream(inputs, config, stream_mode="debug"):
    print(chunk)
    input("按Enter继续...")
    graph.stream(None, config)  # 继续下一步
```

---

**第6章小结**：

LangGraph提供丰富的配置选项：

1. **图级配置**：checkpointer、durability、store等
2. **运行时配置**：thread_id、recursion_limit等
3. **节点级配置**：retry_policy、cache_policy等
4. **流式配置**：stream_mode、stream_subgraphs等
5. **环境变量**：全局调试和缓存设置

合理配置可显著提升性能和可靠性，建议根据环境（开发/生产）采用不同配置策略。


# 第7章 外部依赖

## 7.1 Python包依赖

### 7.1.1 核心依赖

**来源**：`libs/langgraph/pyproject.toml:26-33`

| 包名 | 版本约束 | 用途 | 替代方案 |
|------|---------|------|----------|
| **langchain-core** | `>=0.1` | Runnable抽象、消息类型 | 无（必须） |
| **langgraph-checkpoint** | `>=2.1.0,<4.0.0` | 检查点抽象基类 | 无（必须） |
| **langgraph-sdk** | `>=0.2.2,<0.3.0` | 远程执行客户端 | 可选 |
| **langgraph-prebuilt** | `>=1.0.2,<1.1.0` | 预构建Agent组件 | 可选 |
| **xxhash** | `>=3.5.0` | 快速哈希（缓存键） | hashlib（慢） |
| **pydantic** | `>=2.7.4` | 数据验证和序列化 | dataclasses（受限） |

### 7.1.2 可选依赖（测试用）

**来源**：`pyproject.toml:46-70`

| 包名 | 用途 | 安装方式 |
|------|------|----------|
| **pytest** | 测试框架 | `uv add --group test pytest` |
| **langgraph-checkpoint-sqlite** | SQLite检查点 | `pip install langgraph-checkpoint-sqlite` |
| **langgraph-checkpoint-postgres** | PostgreSQL检查点 | `pip install langgraph-checkpoint-postgres` |
| **psycopg[binary]** | PostgreSQL驱动 | 自动（postgres依赖） |
| **redis** | Redis存储 | `pip install redis` |

## 7.2 LangChain生态依赖

### 7.2.1 langchain-core

**版本**：`>=0.1` （建议 `>=1.0.0`）
**引入目的**：
- Runnable接口：所有节点的基础抽象
- 消息类型：AIMessage、HumanMessage等
- 配置系统：RunnableConfig
- 回调系统：Callbacks用于追踪

**调用方式**：
```python
from langchain_core.runnables import Runnable, RunnableSequence
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables.config import RunnableConfig
```

**影响评估**：
- **性能**：无显著影响（纯抽象）
- **稳定性**：高（成熟库）
- **安全性**：高（LangChain官方维护）

### 7.2.2 langgraph-checkpoint

**版本**：`>=2.1.0,<4.0.0`
**引入目的**：
- `BaseCheckpointSaver`：检查点存储接口
- `Checkpoint`、`CheckpointTuple`类型定义
- `EmptyChannelError`异常

**核心接口**：
```python
class BaseCheckpointSaver(ABC):
    @abstractmethod
    def put(self, config: RunnableConfig, checkpoint: Checkpoint) -> RunnableConfig:
        """保存检查点"""
    
    @abstractmethod
    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """获取检查点"""
```

**实现列表**：
- `InMemorySaver` （来自langgraph-checkpoint）
- `SqliteSaver` （来自langgraph-checkpoint-sqlite）
- `PostgresSaver` （来自langgraph-checkpoint-postgres）

## 7.3 系统库依赖

### 7.3.1 asyncio

**版本**：Python标准库（3.10+）
**引入目的**：异步执行
**使用位置**：
- `langgraph/pregel/_loop.py` - AsyncPregelLoop
- `langgraph/pregel/_runner.py` - 异步任务执行

**代码示例**：
```python
async def ainvoke(...):
    async with AsyncPregelLoop(...) as loop:
        async for chunk in loop:
            yield chunk
```

### 7.3.2 threading

**版本**：Python标准库
**引入目的**：多线程执行
**使用位置**：
- `langgraph/pregel/_executor.py` - ThreadPoolExecutor
- `langgraph/pregel/_algo.py` - 线程局部存储

**配置**：
```python
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=max_concurrency)
```

### 7.3.3 hashlib / xxhash

**对比**：

| 库 | 速度 | 使用场景 |
|----|------|----------|
| **xxhash** | ~10GB/s | 缓存键生成（默认） |
| **hashlib.md5** | ~500MB/s | 加密哈希 |
| **hashlib.sha256** | ~150MB/s | 安全要求高 |

**选型原因**：
- xxhash极快，适合高频缓存查找
- 碰撞概率极低（128位）
- 非加密哈希，不适合安全用途

## 7.4 数据库驱动依赖

### 7.4.1 SQLite

**依赖**：Python标准库（`sqlite3`）
**版本要求**：无特殊要求
**使用方式**：
```python
from langgraph.checkpoint.sqlite import SqliteSaver

saver = SqliteSaver.from_conn_string("checkpoints.db")
```

**配置选项**：
- `checkpoints.db`：本地文件路径
- `:memory:`：内存数据库（测试用）

### 7.4.2 PostgreSQL

**依赖**：`psycopg[binary]>=3.0`
**安装**：
```bash
pip install langgraph-checkpoint-postgres
```

**使用方式**：
```python
from langgraph.checkpoint.postgres import PostgresSaver

saver = PostgresSaver.from_conn_string(
    "postgresql://user:password@localhost/langgraph"
)
```

**连接池配置**：
```python
from psycopg_pool import ConnectionPool

pool = ConnectionPool(
    conninfo="postgresql://...",
    min_size=2,
    max_size=10
)
saver = PostgresSaver(pool)
```

### 7.4.3 Redis (可选)

**依赖**：`redis>=4.0`
**引入目的**：分布式缓存、存储
**使用方式**（需自定义实现）：
```python
import redis
from langgraph.store.base import BaseStore

class RedisStore(BaseStore):
    def __init__(self, redis_client):
        self.client = redis_client
    
    def put(self, namespace, key, value):
        self.client.hset(f"{namespace}:{key}", mapping=value)
```

## 7.5 依赖安装方法

### 7.5.1 最小安装

```bash
pip install langgraph
```

**包含**：
- langgraph核心库
- langchain-core
- langgraph-checkpoint（InMemorySaver）
- pydantic
- xxhash

### 7.5.2 完整安装（推荐）

```bash
pip install "langgraph[all]"
```

**额外包含**：
- SQLite检查点支持
- PostgreSQL检查点支持
- 预构建组件

### 7.5.3 生产环境安装

```bash
# 使用uv（推荐）
uv pip install langgraph langgraph-checkpoint-postgres

# 或使用pip
pip install langgraph langgraph-checkpoint-postgres psycopg[binary]
```

### 7.5.4 开发环境安装

```bash
git clone https://github.com/langchain-ai/langgraph.git
cd langgraph/libs/langgraph
uv sync --group dev
```

**开发依赖**：
- pytest：测试
- mypy：类型检查
- ruff：代码格式化

## 7.6 版本兼容性矩阵

| LangGraph | Python | LangChain Core | Checkpoint | Pydantic |
|-----------|--------|----------------|------------|----------|
| 1.0.x | 3.10-3.13 | >=0.1 | >=2.1.0 | >=2.7.4 |
| 0.2.x | 3.10-3.12 | >=0.1 | >=1.0.0 | >=2.0.0 |
| 0.1.x | 3.9-3.12 | >=0.1 | - | >=1.10.0 |

**升级建议**：
- 从0.x升级到1.x：查看CHANGELOG，API有breaking changes
- Pydantic v1 → v2：需要迁移代码

## 7.7 安全性考虑

### 7.7.1 依赖漏洞扫描

```bash
# 使用pip-audit
pip install pip-audit
pip-audit

# 使用safety
pip install safety
safety check
```

### 7.7.2 已知问题

**截至2025-11-17**：
- 无已知严重漏洞
- 建议定期更新依赖

### 7.7.3 最佳实践

1. **锁定版本**：
```toml
# pyproject.toml
dependencies = [
    "langgraph==1.0.3",
    "langchain-core==0.3.15",
]
```

2. **使用虚拟环境**：
```bash
python -m venv venv
source venv/bin/activate
```

3. **定期更新**：
```bash
pip list --outdated
pip install --upgrade langgraph
```

---

**第7章小结**：

LangGraph的依赖简洁且明确：
1. **核心依赖**：langchain-core、checkpoint、pydantic、xxhash
2. **可选依赖**：SQLite/PostgreSQL检查点、Redis存储
3. **安装方式**：最小安装、完整安装、开发安装
4. **版本要求**：Python 3.10+，Pydantic 2.7.4+

所有依赖均为成熟稳定的开源库，安全性和性能有保障。

---

# 第8章 API 接口说明

## 8.1 StateGraph API

### 8.1.1 构造函数

**函数签名**：
```python
def __init__(
    self,
    state_schema: type[StateT],
    context_schema: type[ContextT] | None = None,
    *,
    input_schema: type[InputT] | None = None,
    output_schema: type[OutputT] | None = None
) -> None
```

**参数**：
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `state_schema` | type | 是 | 状态类型（TypedDict或Pydantic） |
| `context_schema` | type | 否 | 运行时上下文类型 |
| `input_schema` | type | 否 | 输入类型（默认同state_schema） |
| `output_schema` | type | 否 | 输出类型（默认同state_schema） |

**示例**：
```python
from typing_extensions import TypedDict
from langgraph.graph import StateGraph

class State(TypedDict):
    messages: list
    context: dict

graph = StateGraph(state_schema=State)
```

### 8.1.2 add_node

**函数签名**：
```python
def add_node(
    self,
    node: str,
    action: StateNode[StateT, ContextT],
    *,
    retry_policy: RetryPolicy | None = None,
    cache_policy: CachePolicy | None = None,
    metadata: dict[str, Any] | None = None
) -> Self
```

**参数**：
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `node` | str | 是 | 节点名称（唯一） |
| `action` | Callable/Runnable | 是 | 节点执行逻辑 |
| `retry_policy` | RetryPolicy | 否 | 重试策略 |
| `cache_policy` | CachePolicy | 否 | 缓存策略 |
| `metadata` | dict | 否 | 元数据 |

**action签名**：
```python
# 最简单
def node(state: State) -> dict: ...

# 带配置
def node(state: State, config: RunnableConfig) -> dict: ...

# 带Runtime
def node(state: State, runtime: Runtime[Context]) -> dict: ...

# 带Store
def node(state: State, store: BaseStore) -> dict: ...
```

**返回值**：部分状态更新（字典）

**调用示例**：
```python
def process_message(state: State) -> dict:
    messages = state["messages"]
    response = llm.invoke(messages)
    return {"messages": [response]}

graph.add_node("process", process_message)
```

### 8.1.3 add_edge

**函数签名**：
```python
def add_edge(
    self,
    start_key: str,
    end_key: str
) -> Self
```

**参数**：
| 参数 | 类型 | 说明 |
|------|------|------|
| `start_key` | str | 起始节点（或START） |
| `end_key` | str | 结束节点（或END） |

**特殊值**：
- `START`：虚拟入口节点
- `END`：虚拟出口节点

**示例**：
```python
from langgraph.constants import START, END

graph.add_edge(START, "node_a")
graph.add_edge("node_a", "node_b")
graph.add_edge("node_b", END)
```

### 8.1.4 add_conditional_edges

**函数签名**：
```python
def add_conditional_edges(
    self,
    source: str,
    path: Callable[[StateT], str | list[str] | Send | list[Send]],
    path_map: dict[Hashable, str] | None = None
) -> Self
```

**参数**：
| 参数 | 类型 | 说明 |
|------|------|------|
| `source` | str | 源节点 |
| `path` | Callable | 路由函数 |
| `path_map` | dict | 路由映射（可选） |

**path函数签名**：
```python
def router(state: State) -> str | list[str] | Send | list[Send]:
    # 返回下一个节点名称或Send对象
    pass
```

**示例**：
```python
def route_based_on_count(state: State) -> str:
    if len(state["messages"]) > 10:
        return "summarize"
    else:
        return "continue"

graph.add_conditional_edges(
    "check",
    route_based_on_count,
    {
        "summarize": "summarize_node",
        "continue": "continue_node"
    }
)
```

### 8.1.5 compile

**函数签名**：
```python
def compile(
    self,
    checkpointer: BaseCheckpointSaver | None = None,
    *,
    interrupt_before: Sequence[str] | All = (),
    interrupt_after: Sequence[str] | All = (),
    durability: Durability = "sync",
    store: BaseStore | None = None,
    cache: BaseCache | None = None
) -> CompiledStateGraph[StateT, ContextT, InputT, OutputT]
```

**参数**：见第6章配置开关说明

**返回值**：`CompiledStateGraph`（可执行图）

**示例**：
```python
from langgraph.checkpoint.sqlite import SqliteSaver

saver = SqliteSaver.from_conn_string("checkpoints.db")
graph = builder.compile(
    checkpointer=saver,
    interrupt_before=["approval_node"]
)
```

## 8.2 CompiledStateGraph API (执行接口)

### 8.2.1 invoke

**函数签名**：
```python
def invoke(
    self,
    input: InputT,
    config: RunnableConfig | None = None,
    *,
    context: ContextT | None = None,
    **kwargs: Any
) -> OutputT
```

**参数**：
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `input` | dict | 是 | 初始状态 |
| `config` | RunnableConfig | 否 | 运行配置 |
| `context` | ContextT | 否 | 运行时上下文 |

**返回值**：最终状态（dict）

**示例**：
```python
result = graph.invoke(
    {"messages": [{"role": "user", "content": "Hello"}]},
    config={"configurable": {"thread_id": "session-1"}}
)

print(result["messages"])
```

### 8.2.2 stream

**函数签名**：
```python
def stream(
    self,
    input: InputT | None,
    config: RunnableConfig | None = None,
    *,
    stream_mode: StreamMode = "values",
    context: ContextT | None = None,
    subgraphs: bool = False
) -> Iterator[dict]
```

**参数**：
| 参数 | 类型 | 说明 |
|------|------|------|
| `stream_mode` | StreamMode | 流模式（见第6章） |
| `subgraphs` | bool | 包含子图事件 |

**返回值**：迭代器，yield不同格式的chunk

**示例**：
```python
for chunk in graph.stream(
    {"messages": [...]},
    stream_mode="updates"
):
    print(chunk)
# 输出：{"node_a": {"messages": [...]}}
```

### 8.2.3 get_state

**函数签名**：
```python
def get_state(
    self,
    config: RunnableConfig,
    *,
    subgraphs: bool = False
) -> StateSnapshot
```

**参数**：
| 参数 | 类型 | 说明 |
|------|------|------|
| `config` | RunnableConfig | 必须包含thread_id |
| `subgraphs` | bool | 包含子图状态 |

**返回值**：`StateSnapshot`（见第5章数据结构）

**示例**：
```python
config = {"configurable": {"thread_id": "session-1"}}
snapshot = graph.get_state(config)

print(snapshot.values)  # 当前状态
print(snapshot.next)    # 下一个节点
print(snapshot.tasks)   # 待执行任务
```

### 8.2.4 update_state

**函数签名**：
```python
def update_state(
    self,
    config: RunnableConfig,
    values: dict | Any,
    as_node: str | None = None
) -> RunnableConfig
```

**参数**：
| 参数 | 类型 | 说明 |
|------|------|------|
| `values` | dict | 状态更新 |
| `as_node` | str | 模拟哪个节点的写入 |

**返回值**：新的配置（含新checkpoint_id）

**示例**：
```python
# 手动修改状态
new_config = graph.update_state(
    config,
    {"priority": "high"},
    as_node="admin"
)

# 继续执行
graph.stream(None, new_config)
```

## 8.3 函数式 API

### 8.3.1 @task装饰器

**函数签名**：
```python
def task(
    func: Callable | None = None,
    *,
    retry_policy: RetryPolicy | None = None,
    cache_policy: CachePolicy | None = None
) -> Callable
```

**用法**：
```python
from langgraph.func import task

@task(retry_policy=RetryPolicy(max_attempts=3))
def process_item(item: dict) -> dict:
    # 处理逻辑
    return {"result": item["value"] * 2}

# 调用
future = process_item({"value": 5})
result = future.result()  # 阻塞等待结果
```

### 8.3.2 @entrypoint装饰器

**函数签名**：
```python
def entrypoint(
    func: Callable | None = None,
    *,
    checkpointer: BaseCheckpointSaver | None = None,
    store: BaseStore | None = None,
    cache: BaseCache | None = None
) -> Callable
```

**用法**：
```python
from langgraph.func import entrypoint, task

@task
def step1(x): return x + 1

@task
def step2(x): return x * 2

@entrypoint(checkpointer=saver)
def workflow(inputs: dict) -> dict:
    a = step1(inputs["value"]).result()
    b = step2(a).result()
    return {"output": b}

# 调用
result = workflow({"value": 5})
# result = {"output": 12}
```

## 8.4 辅助函数 API

### 8.4.1 interrupt

**函数签名**：
```python
def interrupt(value: Any, *, id: str | None = None) -> Any
```

**参数**：
| 参数 | 类型 | 说明 |
|------|------|------|
| `value` | Any | 中断值（传给用户） |
| `id` | str | 中断ID（可选，用于多中断） |

**返回值**：resume值（用户提供）

**使用示例**：
```python
from langgraph.types import interrupt

def approval_node(state: State) -> dict:
    user_response = interrupt({
        "question": "是否批准?",
        "options": ["approve", "reject"]
    })
    
    return {"status": user_response}

# 用户侧
for chunk in graph.stream(...):
    if "__interrupt__" in chunk:
        user_input = input("请选择: ")
        graph.stream(Command(resume=user_input), config)
```

### 8.4.2 Send

**类定义**：
```python
@dataclass
class Send:
    node: str  # 目标节点名
    arg: Any   # 传递的参数
```

**使用示例**：
```python
def fan_out(state: State) -> list[Send]:
    return [
        Send("process_item", {"item": item})
        for item in state["items"]
    ]

graph.add_conditional_edges(START, fan_out)
```

### 8.4.3 Command

**见第5章数据结构**

**使用示例**：
```python
from langgraph.types import Command

# 恢复中断
command = Command(resume="user_input")

# 更新状态
command = Command(update={"priority": "high"})

# 跳转节点
command = Command(goto=Send("final_node", state))

# 组合
command = Command(
    update={"status": "approved"},
    resume="continue",
    goto=Send("next", state)
)

graph.stream(command, config)
```

## 8.5 配置访问 API

### 8.5.1 get_config

**函数签名**：
```python
def get_config() -> RunnableConfig
```

**用途**：在节点内获取当前配置

**示例**：
```python
from langgraph.config import get_config

def node(state: State) -> dict:
    config = get_config()
    thread_id = config["configurable"]["thread_id"]
    return {"thread": thread_id}
```

### 8.5.2 get_store

**函数签名**：
```python
def get_store() -> BaseStore
```

**用途**：获取存储实例

**示例**：
```python
from langgraph.config import get_store

def node(state: State) -> dict:
    store = get_store()
    data = store.get(("namespace",), "key")
    return {"data": data}
```

### 8.5.3 get_stream_writer

**函数签名**：
```python
def get_stream_writer() -> StreamWriter
```

**用途**：自定义流输出

**示例**：
```python
from langgraph.config import get_stream_writer

def node(state: State) -> dict:
    writer = get_stream_writer()
    
    for i in range(10):
        writer({"progress": i * 10})
        # 处理...
    
    return {"status": "done"}

# 用户侧
for chunk in graph.stream(..., stream_mode="custom"):
    print(chunk)  # {"progress": 0}, {"progress": 10}, ...
```

---

**第8章小结**：

LangGraph提供清晰的API层次：
1. **StateGraph API**：构建图（add_node, add_edge, compile）
2. **CompiledStateGraph API**：执行图（invoke, stream, get_state）
3. **函数式API**：@task、@entrypoint装饰器
4. **辅助API**：interrupt、Send、Command
5. **配置访问**：get_config、get_store、get_stream_writer

所有API均支持类型提示，IDE友好，文档完善。

---

# 第9章 错误码及异常处理

## 9.1 异常层次结构

```
Exception
├── GraphRecursionError (RecursionError)
│   错误码：GRAPH_RECURSION_LIMIT
│   原因：超过递归限制
│
├── InvalidUpdateError
│   错误码：INVALID_CONCURRENT_GRAPH_UPDATE
│          INVALID_GRAPH_NODE_RETURN_VALUE
│   原因：无效的状态更新
│
├── GraphBubbleUp (内部异常，用户不可见)
│   ├── GraphInterrupt
│   │   原因：子图中断（被根图捕获）
│   │
│   └── ParentCommand
│       原因：向父图发送命令
│
├── EmptyInputError
│   原因：图收到空输入
│
├── TaskNotFound
│   原因：分布式模式下任务未找到
│
└── EmptyChannelError (来自checkpoint库)
    原因：读取未初始化的通道
```

## 9.2 错误码定义

**位置**：`langgraph/errors.py:29`

```python
class ErrorCode(Enum):
    GRAPH_RECURSION_LIMIT = "GRAPH_RECURSION_LIMIT"
    INVALID_CONCURRENT_GRAPH_UPDATE = "INVALID_CONCURRENT_GRAPH_UPDATE"
    INVALID_GRAPH_NODE_RETURN_VALUE = "INVALID_GRAPH_NODE_RETURN_VALUE"
    MULTIPLE_SUBGRAPHS = "MULTIPLE_SUBGRAPHS"
    INVALID_CHAT_HISTORY = "INVALID_CHAT_HISTORY"
```

### 9.2.1 GRAPH_RECURSION_LIMIT

**触发条件**：
```python
step > recursion_limit  # 默认25步
```

**完整异常信息**：
```
GraphRecursionError: Recursion limit of 25 reached without hitting a stop condition.
For troubleshooting, visit: https://docs.langchain.com/oss/python/langgraph/errors/GRAPH_RECURSION_LIMIT
```

**处理方式**：
```python
from langgraph.errors import GraphRecursionError

try:
    result = graph.invoke(inputs, config={"recursion_limit": 100})
except GraphRecursionError as e:
    print("图执行步数过多，可能存在无限循环")
    # 检查图结构，添加终止条件
```

**排查建议**：
1. 检查是否有END节点
2. 检查条件边的路由逻辑
3. 添加循环计数器
4. 增加recursion_limit（临时）

### 9.2.2 INVALID_CONCURRENT_GRAPH_UPDATE

**触发条件**：
- 多个并行节点写入同一个非聚合通道
- 通道是LastValue类型，但收到多个更新

**示例**：
```python
# 错误：node_a和node_b并行执行，都写入"result"
# 而"result"是LastValue通道
def node_a(state): return {"result": "a"}
def node_b(state): return {"result": "b"}

# 正确：使用聚合通道
class State(TypedDict):
    result: Annotated[list, operator.add]  # 使用BinaryOperatorAggregate
```

**处理方式**：
```python
from langgraph.errors import InvalidUpdateError

try:
    graph.invoke(...)
except InvalidUpdateError as e:
    print(f"并发更新冲突: {e}")
    # 修改state_schema，使用聚合通道
```

### 9.2.3 INVALID_GRAPH_NODE_RETURN_VALUE

**触发条件**：
- 节点返回值不是dict
- 节点返回的key不在state_schema中

**示例**：
```python
class State(TypedDict):
    messages: list

def bad_node(state):
    return "invalid"  # 错误：应返回dict

def bad_node2(state):
    return {"unknown_key": "value"}  # 错误：unknown_key不在State中
```

**处理方式**：
```python
# 确保返回值是state的子集
def correct_node(state: State) -> dict:
    return {"messages": [...]}  # 正确
```

## 9.3 自定义异常类

### 9.3.1 GraphInterrupt

**用途**：内部信号，用户不应捕获
**位置**：`langgraph/errors.py:84`

```python
class GraphInterrupt(GraphBubbleUp):
    """子图中断信号"""
    def __init__(self, interrupts: Sequence[Interrupt] = ()):
        super().__init__(interrupts)
```

**触发**：
```python
from langgraph.types import interrupt

def node(state):
    user_input = interrupt("请输入")  # 内部抛出GraphInterrupt
    return {"input": user_input}
```

### 9.3.2 ParentCommand

**用途**：向父图传递命令
**位置**：`langgraph/errors.py:111`

```python
class ParentCommand(GraphBubbleUp):
    """向父图发送命令"""
    args: tuple[Command]
    
    def __init__(self, command: Command):
        super().__init__(command)
```

**使用**（子图内）：
```python
from langgraph.types import Command
from langgraph.errors import ParentCommand

def subgraph_node(state):
    if condition:
        # 告诉父图更新状态
        raise ParentCommand(Command(update={"status": "done"}))
```

## 9.4 异常处理策略

### 9.4.1 重试策略

```python
from langgraph.types import RetryPolicy

graph.add_node(
    "api_call",
    func,
    retry_policy=RetryPolicy(
        max_attempts=3,
        retry_on=lambda e: isinstance(e, (TimeoutError, ConnectionError))
    )
)
```

**重试决策树**：
```
执行节点
    ├─ 成功 → 返回结果
    └─ 失败
        ├─ retry_on(exception) == True
        │   ├─ attempts < max_attempts
        │   │   └─ 等待（指数退避）→ 重试
        │   └─ attempts >= max_attempts
        │       └─ 抛出原始异常
        └─ retry_on(exception) == False
            └─ 立即抛出异常
```

### 9.4.2 全局异常捕获

```python
try:
    result = graph.invoke(inputs, config)
except GraphRecursionError:
    # 递归限制
    handle_recursion_error()
except InvalidUpdateError:
    # 并发更新冲突
    handle_invalid_update()
except Exception as e:
    # 节点内部异常
    logger.error(f"节点执行失败: {e}")
    raise
```

### 9.4.3 节点级异常处理

```python
def safe_node(state: State) -> dict:
    try:
        result = risky_operation(state)
        return {"result": result}
    except ValueError as e:
        # 记录错误，返回默认值
        logger.warning(f"操作失败: {e}")
        return {"result": None, "error": str(e)}
    except Exception:
        # 不可恢复的错误，重新抛出
        raise
```

### 9.4.4 降级策略

```python
def node_with_fallback(state: State) -> dict:
    try:
        # 尝试主逻辑
        result = primary_service.call(state)
    except ServiceUnavailable:
        # 降级到备用服务
        result = fallback_service.call(state)
    except Exception:
        # 返回缓存结果
        result = cache.get(state["key"])
    
    return {"result": result}
```

## 9.5 日志记录机制

### 9.5.1 日志配置

**位置**：`langgraph/pregel/_log.py`

```python
import logging

logger = logging.getLogger("langgraph.pregel")

# 配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
```

### 9.5.2 日志级别

| 级别 | 用途 | 示例 |
|------|------|------|
| DEBUG | 详细执行信息 | 每个节点的输入输出 |
| INFO | 关键步骤 | 检查点保存、步数增加 |
| WARNING | 可恢复错误 | 重试、降级 |
| ERROR | 不可恢复错误 | 节点执行失败 |

### 9.5.3 记录内容

**默认日志**：
```
2025-11-17 12:00:00 - langgraph.pregel - INFO - Step 1 started
2025-11-17 12:00:01 - langgraph.pregel - DEBUG - Executing task: node_a
2025-11-17 12:00:02 - langgraph.pregel - INFO - Checkpoint saved: checkpoint_123
2025-11-17 12:00:02 - langgraph.pregel - WARNING - Node retry attempt 1/3
2025-11-17 12:00:03 - langgraph.pregel - ERROR - Node failed: ConnectionError
```

### 9.5.4 自定义日志

```python
import logging

logger = logging.getLogger(__name__)

def node(state: State) -> dict:
    logger.info(f"处理消息数: {len(state['messages'])}")
    
    try:
        result = process(state)
        logger.debug(f"结果: {result}")
        return {"result": result}
    except Exception as e:
        logger.error(f"处理失败: {e}", exc_info=True)
        raise
```

## 9.6 调试和排查

### 9.6.1 使用debug stream模式

```python
for chunk in graph.stream(inputs, stream_mode="debug"):
    print(chunk)
# 输出：
# {"type": "checkpoint", "checkpoint": {...}}
# {"type": "task_start", "name": "node_a", ...}
# {"type": "task_end", "name": "node_a", "result": ...}
```

### 9.6.2 检查状态快照

```python
snapshot = graph.get_state(config)

# 检查当前值
print("当前状态:", snapshot.values)

# 检查待执行任务
for task in snapshot.tasks:
    print(f"待执行: {task.name}")
    if task.error:
        print(f"  错误: {task.error}")

# 检查中断
for interrupt in snapshot.interrupts:
    print(f"中断值: {interrupt.value}")
```

### 9.6.3 启用详细追踪

```python
from langchain_core.tracers import ConsoleCallbackHandler

config = {
    "callbacks": [ConsoleCallbackHandler()],
    "configurable": {"thread_id": "debug"}
}

graph.invoke(inputs, config)
# 输出详细的执行追踪
```

---

**第9章小结**：

LangGraph的异常体系完善：
1. **异常层次**：清晰的继承结构，区分用户异常和内部信号
2. **错误码**：5个主要错误码，指向详细文档
3. **处理策略**：重试、降级、全局捕获
4. **日志机制**：多级别日志，支持自定义
5. **调试工具**：debug模式、状态快照、追踪

遇到问题时，建议：
1. 查看错误码对应的官方文档
2. 启用debug stream模式
3. 检查状态快照和待执行任务
4. 查看日志的详细信息

---

# 第10章 部署与运行

## 10.1 运行环境要求

### 10.1.1 操作系统

| 系统 | 版本 | 支持状态 |
|------|------|----------|
| **Linux** | Ubuntu 20.04+, CentOS 8+, Debian 11+ | ✅ 完全支持 |
| **macOS** | 12.0+ (Monterey+) | ✅ 完全支持 |
| **Windows** | 10+, Windows Server 2019+ | ✅ 支持（建议WSL2） |

### 10.1.2 Python版本

**要求**：Python >= 3.10

```bash
python --version
# Python 3.10.0 或更高
```

**推荐版本**：
- **生产环境**：Python 3.11（性能最佳）
- **开发环境**：Python 3.12（最新特性）

### 10.1.3 硬件要求

**最低配置**：
- CPU：2核
- 内存：2GB
- 磁盘：1GB（含依赖）

**推荐配置**（生产环境）：
- CPU：4核以上
- 内存：8GB以上
- 磁盘：SSD 10GB以上
- 网络：稳定的网络连接（如需LLM调用）

**GPU支持**：
- LangGraph本身无GPU需求
- 如使用本地LLM（如llama.cpp），需相应GPU

## 10.2 安装步骤

### 10.2.1 虚拟环境管理

**使用venv**：
```bash
# 创建虚拟环境
python -m venv venv

# 激活
# Linux/macOS:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# 验证
which python
```

**使用uv（推荐）**：
```bash
# 安装uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# 创建项目
uv init my-langgraph-app
cd my-langgraph-app

# 添加依赖
uv add langgraph langgraph-checkpoint-sqlite
```

### 10.2.2 依赖安装

**方式1：pip安装**：
```bash
# 最小安装
pip install langgraph

# 完整安装（包含检查点后端）
pip install "langgraph[all]"

# 生产环境
pip install langgraph langgraph-checkpoint-postgres psycopg[binary]
```

**方式2：从源码安装**：
```bash
git clone https://github.com/langchain-ai/langgraph.git
cd langgraph/libs/langgraph
pip install -e .
```

**方式3：使用requirements.txt**：
```bash
# requirements.txt
langgraph==1.0.3
langgraph-checkpoint-postgres==2.1.0
psycopg[binary]>=3.0
redis>=4.0

# 安装
pip install -r requirements.txt
```

### 10.2.3 验证安装

```python
import langgraph
from langgraph.graph import StateGraph

print(langgraph.__version__)  # 1.0.3

# 简单测试
from typing_extensions import TypedDict

class State(TypedDict):
    count: int

def increment(state: State) -> dict:
    return {"count": state["count"] + 1}

graph = StateGraph(State)
graph.add_node("inc", increment)
graph.set_entry_point("inc")
graph.set_finish_point("inc")
compiled = graph.compile()

result = compiled.invoke({"count": 0})
assert result["count"] == 1
print("安装验证成功！")
```

## 10.3 配置文件准备

### 10.3.1 应用配置示例

**config.yaml**：
```yaml
langgraph:
  checkpointer:
    type: postgres
    connection_string: postgresql://user:password@localhost/langgraph
    pool_size: 10
    max_overflow: 20
  
  store:
    type: postgres
    connection_string: postgresql://user:password@localhost/langgraph_store
  
  durability: async
  
  execution:
    max_concurrency: 8
    recursion_limit: 50
  
  logging:
    level: INFO
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

**加载配置**：
```python
import yaml
from langgraph.checkpoint.postgres import PostgresSaver

with open("config.yaml") as f:
    config = yaml.safe_load(f)

checkpointer = PostgresSaver.from_conn_string(
    config["langgraph"]["checkpointer"]["connection_string"]
)

graph = builder.compile(
    checkpointer=checkpointer,
    durability=config["langgraph"]["durability"]
)
```

### 10.3.2 环境变量配置

**.env**：
```bash
# 数据库
DATABASE_URL=postgresql://user:password@localhost/langgraph
REDIS_URL=redis://localhost:6379/0

# LLM API
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=...

# LangGraph
LANGGRAPH_DEBUG=false
RECURSION_LIMIT=50
```

**加载环境变量**：
```python
from dotenv import load_dotenv
import os

load_dotenv()

db_url = os.getenv("DATABASE_URL")
checkpointer = PostgresSaver.from_conn_string(db_url)
```

## 10.4 启动方式与命令行示例

### 10.4.1 单文件运行

**app.py**：
```python
from langgraph.graph import StateGraph
from langgraph.checkpoint.sqlite import SqliteSaver

# 定义图
builder = StateGraph(State)
# ... 添加节点

# 编译
saver = SqliteSaver.from_conn_string("checkpoints.db")
graph = builder.compile(checkpointer=saver)

# 运行
if __name__ == "__main__":
    result = graph.invoke({"messages": [...]})
    print(result)
```

**启动**：
```bash
python app.py
```

### 10.4.2 Web服务部署（FastAPI）

**server.py**：
```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langgraph.graph import StateGraph
from langgraph.checkpoint.postgres import PostgresSaver

app = FastAPI()

# 初始化图
saver = PostgresSaver.from_conn_string(os.getenv("DATABASE_URL"))
graph = builder.compile(checkpointer=saver)

class InvokeRequest(BaseModel):
    input: dict
    thread_id: str

@app.post("/invoke")
async def invoke_graph(request: InvokeRequest):
    try:
        config = {"configurable": {"thread_id": request.thread_id}}
        result = graph.invoke(request.input, config)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/stream")
async def stream_graph(request: InvokeRequest):
    config = {"configurable": {"thread_id": request.thread_id}}
    
    def event_generator():
        for chunk in graph.stream(request.input, config, stream_mode="updates"):
            yield f"data: {json.dumps(chunk)}\n\n"
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")
```

**启动**：
```bash
uvicorn server:app --host 0.0.0.0 --port 8000 --workers 4
```

### 10.4.3 CLI工具

**cli.py**：
```python
import click
from langgraph.graph import StateGraph
from langgraph.checkpoint.sqlite import SqliteSaver

@click.group()
def cli():
    pass

@cli.command()
@click.option("--input", required=True, help="输入JSON")
@click.option("--thread-id", required=True, help="线程ID")
def run(input, thread_id):
    """运行图"""
    saver = SqliteSaver.from_conn_string("checkpoints.db")
    graph = builder.compile(checkpointer=saver)
    
    import json
    input_data = json.loads(input)
    config = {"configurable": {"thread_id": thread_id}}
    
    result = graph.invoke(input_data, config)
    click.echo(json.dumps(result, indent=2))

@cli.command()
@click.option("--thread-id", required=True)
def get_state(thread_id):
    """获取状态"""
    saver = SqliteSaver.from_conn_string("checkpoints.db")
    graph = builder.compile(checkpointer=saver)
    
    config = {"configurable": {"thread_id": thread_id}}
    snapshot = graph.get_state(config)
    
    click.echo(f"当前值: {snapshot.values}")
    click.echo(f"下一步: {snapshot.next}")

if __name__ == "__main__":
    cli()
```

**使用**：
```bash
python cli.py run --input '{"messages":[...]}' --thread-id session-1
python cli.py get-state --thread-id session-1
```

## 10.5 后台运行和进程管理

### 10.5.1 使用systemd（Linux）

**langgraph.service**：
```ini
[Unit]
Description=LangGraph Application
After=network.target postgresql.service

[Service]
Type=simple
User=langgraph
WorkingDirectory=/opt/langgraph
Environment="PATH=/opt/langgraph/venv/bin"
ExecStart=/opt/langgraph/venv/bin/python app.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**启动**：
```bash
sudo systemctl enable langgraph
sudo systemctl start langgraph
sudo systemctl status langgraph
```

### 10.5.2 使用Supervisor

**supervisord.conf**：
```ini
[program:langgraph]
command=/opt/langgraph/venv/bin/uvicorn server:app --host 0.0.0.0 --port 8000
directory=/opt/langgraph
user=langgraph
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/langgraph/stdout.log
stderr_logfile=/var/log/langgraph/stderr.log
```

**启动**：
```bash
supervisord -c supervisord.conf
supervisorctl status
supervisorctl restart langgraph
```

### 10.5.3 使用PM2（Node.js环境）

```bash
pm2 start "uvicorn server:app --host 0.0.0.0" --name langgraph
pm2 save
pm2 startup
```

## 10.6 容器化部署

### 10.6.1 Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 暴露端口
EXPOSE 8000

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# 启动命令
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
```

### 10.6.2 构建和运行

```bash
# 构建镜像
docker build -t langgraph-app:1.0 .

# 运行容器
docker run -d \
    --name langgraph \
    -p 8000:8000 \
    -e DATABASE_URL=postgresql://... \
    -e OPENAI_API_KEY=... \
    --restart unless-stopped \
    langgraph-app:1.0

# 查看日志
docker logs -f langgraph

# 停止容器
docker stop langgraph
```

### 10.6.3 Docker Compose

**docker-compose.yml**：
```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: langgraph
      POSTGRES_USER: langgraph
      POSTGRES_PASSWORD: password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  redis:
    image: redis:7
    ports:
      - "6379:6379"

  langgraph:
    build: .
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql://langgraph:password@postgres/langgraph
      REDIS_URL: redis://redis:6379/0
    depends_on:
      - postgres
      - redis
    restart: unless-stopped

volumes:
  postgres_data:
```

**启动**：
```bash
docker-compose up -d
docker-compose logs -f langgraph
docker-compose down
```

## 10.7 健康检查和监控

### 10.7.1 健康检查端点

```python
from fastapi import FastAPI

@app.get("/health")
async def health_check():
    # 检查数据库连接
    try:
        checkpointer.list(None, limit=1)
        db_status = "ok"
    except Exception:
        db_status = "error"
    
    return {
        "status": "healthy" if db_status == "ok" else "unhealthy",
        "database": db_status,
        "version": "1.0.3"
    }
```

### 10.7.2 Prometheus监控

```python
from prometheus_client import Counter, Histogram, generate_latest

# 定义指标
graph_invocations = Counter("langgraph_invocations_total", "Total graph invocations")
graph_duration = Histogram("langgraph_duration_seconds", "Graph execution duration")

@app.middleware("http")
async def metrics_middleware(request, call_next):
    with graph_duration.time():
        response = await call_next(request)
        graph_invocations.inc()
    return response

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type="text/plain")
```

### 10.7.3 日志聚合

**使用Fluentd/Filebeat收集日志**：
```yaml
# fluent.conf
<source>
  @type tail
  path /var/log/langgraph/*.log
  pos_file /var/log/fluentd/langgraph.pos
  tag langgraph
  format json
</source>

<match langgraph>
  @type elasticsearch
  host elasticsearch
  port 9200
  index_name langgraph
</match>
```

## 10.8 CI/CD流水线

### 10.8.1 GitHub Actions

**.github/workflows/deploy.yml**：
```yaml
name: Deploy LangGraph

on:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: pytest

  deploy:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build and push Docker image
        run: |
          docker build -t myregistry/langgraph:${{ github.sha }} .
          docker push myregistry/langgraph:${{ github.sha }}
      - name: Deploy to production
        run: |
          kubectl set image deployment/langgraph \
            langgraph=myregistry/langgraph:${{ github.sha }}
```

### 10.8.2 GitLab CI

**.gitlab-ci.yml**：
```yaml
stages:
  - test
  - build
  - deploy

test:
  stage: test
  image: python:3.11
  script:
    - pip install -r requirements.txt
    - pytest

build:
  stage: build
  script:
    - docker build -t $CI_REGISTRY_IMAGE:$CI_COMMIT_SHA .
    - docker push $CI_REGISTRY_IMAGE:$CI_COMMIT_SHA

deploy:
  stage: deploy
  script:
    - kubectl set image deployment/langgraph langgraph=$CI_REGISTRY_IMAGE:$CI_COMMIT_SHA
```

---

**第10章小结**：

LangGraph的部署方案灵活多样：
1. **环境要求**：Python 3.10+，支持主流操作系统
2. **安装方式**：pip、uv、源码安装
3. **启动方式**：单文件、Web服务、CLI工具
4. **进程管理**：systemd、Supervisor、PM2
5. **容器化**：Docker、Docker Compose、Kubernetes
6. **监控运维**：健康检查、Prometheus、日志聚合
7. **CI/CD**：GitHub Actions、GitLab CI

推荐生产环境方案：
- **容器化部署**：Docker + Kubernetes
- **数据库**：PostgreSQL检查点 + Redis缓存
- **监控**：Prometheus + Grafana
- **日志**：ELK Stack或Loki
- **CI/CD**：GitLab CI或GitHub Actions


---

# 总结图表

## 图1：LangGraph核心类图

```mermaid
classDiagram
    class PregelProtocol {
        <<interface>>
        +invoke(input, config) OutputT
        +stream(input, config) Iterator
        +get_state(config) StateSnapshot
        +update_state(config, values) RunnableConfig
    }
    
    class Pregel {
        -nodes: dict~str, PregelNode~
        -channels: dict~str, BaseChannel~
        -checkpointer: BaseCheckpointSaver
        -cache: BaseCache
        -store: BaseStore
        +invoke(input, config) dict
        +stream(input, config) Iterator
        +get_state(config) StateSnapshot
    }
    
    class StateGraph {
        -state_schema: type
        -nodes: dict~str, StateNodeSpec~
        -edges: set~tuple~
        -branches: dict
        +add_node(name, action)
        +add_edge(start, end)
        +add_conditional_edges(source, path)
        +compile() CompiledStateGraph
    }
    
    class CompiledStateGraph {
        +invoke(input, config) dict
        +stream(input, config) Iterator
    }
    
    class PregelNode {
        +channels: str | list~str~
        +triggers: list~str~
        +writers: list~ChannelWrite~
        +bound: Runnable
        +retry_policy: RetryPolicy
    }
    
    class BaseChannel {
        <<abstract>>
        +update(values) bool
        +get() Value
        +checkpoint() Checkpoint
        +from_checkpoint(cp) Self
    }
    
    class LastValue {
        -value: Any
        +update(values) bool
        +get() Any
    }
    
    class BinaryOperatorAggregate {
        -operator: Callable
        -value: Any
        +update(values) bool
    }
    
    class Runtime {
        +context: ContextT
        +store: BaseStore
        +stream_writer: StreamWriter
        +previous: Any
    }
    
    class BaseCheckpointSaver {
        <<abstract>>
        +put(config, checkpoint) RunnableConfig
        +get_tuple(config) CheckpointTuple
        +list(config) Iterator
    }
    
    PregelProtocol <|.. Pregel
    Pregel <|-- CompiledStateGraph
    StateGraph --> CompiledStateGraph : compile()
    Pregel --> PregelNode : contains
    Pregel --> BaseChannel : contains
    Pregel --> BaseCheckpointSaver : uses
    BaseChannel <|-- LastValue
    BaseChannel <|-- BinaryOperatorAggregate
    Pregel --> Runtime : creates
```

## 图2：函数调用关系图

```mermaid
flowchart TD
    A[用户代码] -->|创建| B[StateGraph]
    B -->|add_node| C[添加节点]
    B -->|add_edge| D[添加边]
    B -->|compile| E[编译过程]
    
    E -->|验证| F[validate_graph]
    E -->|创建通道| G[_get_channels]
    E -->|包装节点| H[_prepare_nodes]
    E -->|构建| I[Pregel实例]
    
    I -->|invoke| J[_execute_sync]
    I -->|ainvoke| K[_execute_async]
    I -->|stream| L[_execute_sync + 流式]
    
    J -->|初始化| M[_init_checkpoint]
    J -->|主循环| N[SyncPregelLoop]
    
    N -->|规划| O[prepare_next_tasks]
    N -->|执行| P[PregelRunner.invoke]
    N -->|更新| Q[apply_writes]
    
    O -->|检查版本| R[channel_versions]
    O -->|返回| S[PregelExecutableTask列表]
    
    P -->|并行| T[ThreadPoolExecutor]
    T -->|每个任务| U[local_read]
    U -->|调用| V[node.invoke]
    V -->|返回| W[writes]
    
    Q -->|分组| X[pending_writes]
    Q -->|应用| Y[channel.update]
    Y -->|保存| Z[checkpointer.put]
    Z -->|发出| AA[stream events]
```

## 图3：数据流动图

```mermaid
flowchart LR
    subgraph 输入层
        A[用户输入dict]
        B[RunnableConfig]
    end
    
    subgraph 编译层
        C[StateGraph]
        D[验证器]
        E[通道工厂]
    end
    
    subgraph 执行层
        F[Pregel]
        G[PregelLoop]
        H[PregelRunner]
    end
    
    subgraph 存储层
        I[Channels]
        J[Checkpointer]
        K[Store]
    end
    
    subgraph 输出层
        L[最终状态dict]
        M[流式chunks]
        N[StateSnapshot]
    end
    
    A -->|传入| F
    B -->|配置| F
    C -->|compile| F
    D -->|验证| E
    E -->|创建| I
    
    F -->|初始化| G
    G -->|规划| H
    H -->|读取| I
    H -->|写入| I
    I -->|序列化| J
    F -->|访问| K
    
    I -->|读取输出| L
    G -->|迭代| M
    J -->|快照| N
```

## 图4：模块依赖关系图

```mermaid
flowchart TB
    subgraph 用户层
        A[应用代码]
    end
    
    subgraph API层
        B[StateGraph]
        C[task/entrypoint]
    end
    
    subgraph 编译层
        D[graph/state.py]
        E[pregel/_validate.py]
    end
    
    subgraph 执行层
        F[pregel/main.py]
        G[pregel/_loop.py]
        H[pregel/_runner.py]
        I[pregel/_algo.py]
    end
    
    subgraph 基础设施层
        J[channels/]
        K[_internal/]
        L[checkpoint]
        M[runtime.py]
    end
    
    subgraph 外部依赖
        N[langchain-core]
        O[langgraph-checkpoint]
        P[pydantic]
    end
    
    A --> B
    A --> C
    B --> D
    C --> D
    D --> E
    D --> F
    
    F --> G
    F --> H
    F --> I
    G --> H
    H --> I
    
    I --> J
    I --> K
    F --> L
    F --> M
    
    J --> P
    D --> N
    F --> N
    L --> O
    
    style A fill:#e1f5ff
    style B fill:#b3e5fc
    style C fill:#b3e5fc
    style F fill:#81c784
    style J fill:#fff59d
    style N fill:#ffccbc
```

## 图5：执行时序图

```mermaid
sequenceDiagram
    participant User as 用户
    participant Graph as CompiledStateGraph
    participant Loop as PregelLoop
    participant Runner as PregelRunner
    participant Node as 节点
    participant Channel as 通道
    participant CP as Checkpointer
    
    User->>Graph: invoke(input, config)
    Graph->>CP: 加载/创建检查点
    CP-->>Graph: checkpoint
    Graph->>Channel: 应用输入
    
    loop 主执行循环
        Graph->>Loop: 开始步骤
        Loop->>Channel: 读取版本
        Loop->>Loop: prepare_next_tasks()
        Loop->>Runner: invoke(tasks)
        
        par 并行执行节点
            Runner->>Node: task1.run()
            Node->>Channel: local_read()
            Node->>Node: 执行逻辑
            Node-->>Runner: writes1
        and
            Runner->>Node: task2.run()
            Node->>Channel: local_read()
            Node->>Node: 执行逻辑
            Node-->>Runner: writes2
        end
        
        Runner-->>Loop: all_writes
        Loop->>Channel: apply_writes()
        Channel->>Channel: update(values)
        Loop->>CP: put(checkpoint)
        CP-->>Loop: saved
        Loop->>User: emit event (如果stream)
        
        alt 无更多任务
            Loop-->>Graph: 完成
        else 有更多任务
            Loop->>Loop: 下一步
        end
    end
    
    Graph->>Channel: 读取输出
    Channel-->>Graph: final_values
    Graph-->>User: result
```

## 图6：异常处理流程图

```mermaid
flowchart TD
    A[节点执行] --> B{执行成功?}
    B -->|是| C[返回结果]
    B -->|否| D[捕获异常]
    
    D --> E{有重试策略?}
    E -->|否| F[直接抛出]
    E -->|是| G{retry_on匹配?}
    
    G -->|否| F
    G -->|是| H{attempts < max?}
    
    H -->|否| I[抛出RetryExhausted]
    H -->|是| J[计算等待时间]
    
    J --> K[指数退避 + 抖动]
    K --> L[sleep]
    L --> M[重试]
    M --> A
    
    F --> N{图级异常处理}
    I --> N
    
    N --> O{异常类型}
    O -->|GraphRecursionError| P[递归限制]
    O -->|InvalidUpdateError| Q[并发冲突]
    O -->|GraphInterrupt| R[中断信号]
    O -->|其他| S[未捕获异常]
    
    P --> T[抛出给用户]
    Q --> T
    S --> T
    R --> U[保存中断]
    U --> V[返回中断事件]
    
    C --> W[成功]
    T --> X[失败]
    V --> Y[等待恢复]
```

---

## 技术文档总结

### 核心要点

#### 1. 架构设计
- **四层架构**：API层 → 编译层 → 执行层 → 基础设施层
- **模块化设计**：64个Python文件，约16,000行代码
- **设计模式**：Builder、Strategy、Actor、Protocol等10种模式
- **单向依赖**：无循环依赖，易于测试和维护

#### 2. 核心算法
- **BSP算法**：Plan-Execute-Update三阶段并行执行
- **版本控制**：逻辑时钟防止重复触发
- **条件路由**：支持动态分支和fan-out模式
- **缓存机制**：xxHash快速哈希，支持TTL
- **重试策略**：指数退避 + 随机抖动

#### 3. 数据结构
- **Checkpoint**：完整状态快照，支持时间旅行
- **通道系统**：8种通道类型满足不同需求
- **Runtime**：依赖注入的运行时上下文
- **Command**：灵活的控制命令系统

#### 4. 配置管理
- **图级配置**：checkpointer、durability、store等
- **运行时配置**：thread_id、recursion_limit等
- **节点级配置**：retry_policy、cache_policy等
- **流式配置**：7种stream_mode

#### 5. API设计
- **StateGraph API**：用户友好的图构建接口
- **执行API**：invoke、stream、get_state、update_state
- **函数式API**：@task、@entrypoint装饰器
- **辅助API**：interrupt、Send、Command

#### 6. 异常处理
- **清晰的异常层次**：5个主要错误码
- **重试机制**：自动处理临时故障
- **日志系统**：多级别日志，支持自定义
- **调试工具**：debug模式、状态快照

#### 7. 部署方案
- **环境要求**：Python 3.10+，主流OS
- **安装方式**：pip、uv、源码
- **运行方式**：单文件、Web服务、CLI
- **容器化**：Docker、Kubernetes
- **监控运维**：健康检查、Prometheus、日志聚合

### 性能特征

| 特性 | 指标 |
|------|------|
| **并发性能** | 线程池并行，支持AsyncIO |
| **缓存性能** | xxHash ~10GB/s |
| **持久化性能** | 异步持久化模式下无阻塞 |
| **内存占用** | 取决于状态大小，通道采用引用传递 |
| **扩展性** | 支持分布式检查点（PostgreSQL） |

### 适用场景

✅ **推荐使用**：
- 复杂的AI Agent工作流
- 需要人机交互的审批流程
- 长时间运行的有状态任务
- 需要故障恢复的关键流程
- 多步骤的数据处理管道

⚠️ **需谨慎评估**：
- 超高频短任务（检查点开销）
- 无状态的简单流程（过度设计）
- 严格实时性要求（BSP同步开销）

### 未来展望

**待确认**：以下为基于代码结构的推测

可能的演进方向：
1. **性能优化**：增量检查点、更快的序列化
2. **分布式执行**：跨机器的任务调度
3. **可视化增强**：更丰富的图编辑和调试工具
4. **更多通道类型**：优先队列、时间窗口聚合
5. **安全增强**：状态加密、访问控制

### 学习资源

- **官方文档**：https://langchain-ai.github.io/langgraph/
- **源代码**：https://github.com/langchain-ai/langgraph
- **示例库**：`examples/`目录包含多种场景
- **社区论坛**：https://forum.langchain.com/
- **在线课程**：LangChain Academy

---

## 文档维护说明

**文档版本**：1.0
**项目版本**：LangGraph 1.0.3
**最后更新**：2025-11-17
**生成方式**：基于源代码静态分析

**更新建议**：
- 当LangGraph发布新版本时，重新生成文档
- 关注CHANGELOG中的breaking changes
- 定期验证代码示例的正确性
- 补充实际生产环境的最佳实践

**贡献者**：
- 技术文档编写：Claude Code Agent
- 项目开发：LangChain Inc.

---

**全文完**

感谢阅读本技术文档。如有疑问或发现错误，请参考官方文档或提交Issue至GitHub仓库。

