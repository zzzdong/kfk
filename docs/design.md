# kfk - Kafka CLI 工具架构设计

## 1. 概述

**kfk** 是一个基于 Rust 的简易 Kafka 命令行工具，灵感来源于 [kaf](https://github.com/birdayz/kaf)。底层依赖 [kafka_client](https://github.com/zzzdong/kafka_client) 纯 Rust Kafka 客户端库，提供主题管理、消息生产消费、消费者组管理、集群信息查看等核心功能。

### 设计目标

- **轻量**：单一二进制，无外部依赖
- **易用**：命令风格类似 kubectl/docker CLI（同 kaf）
- **可扩展**：模块化架构，便于添加新命令
- **安全**：支持 TLS、SASL/PLAIN、SASL/SCRAM 认证

---

## 2. 整体架构

```
┌─────────────────────────────────────────────────────┐
│                    CLI Layer                        │
│  (clap 命令解析、交互、输出格式化)                     │
├─────────────────────────────────────────────────────┤
│                  Command Layer                      │
│  topic  │  produce  │  consume  │  group  │  node   │
│  config │  ...                                     │
├─────────────────────────────────────────────────────┤
│               Client Abstraction Layer              │
│  (对 kafka_client 的封装，提供 CLI 友好的 API)        │
├─────────────────────────────────────────────────────┤
│              kafka_client (外部依赖)                 │
│  KafkaClient │ Producer │ Consumer │ ClusterClient   │
└─────────────────────────────────────────────────────┘
```

### 2.1 层职责

| 层级 | 职责 |
|------|------|
| **CLI Layer** | 命令解析（clap）、全局参数处理、输出格式化（表格/JSON）、退出码管理 |
| **Command Layer** | 各子命令的业务逻辑实现，调用 Client Abstraction 完成任务 |
| **Client Abstraction** | 封装 kafka_client 的复杂 API，提供面向 CLI 场景的简化接口 |
| **kafka_client** | 底层 Kafka 协议实现、连接管理、生产消费 |

---

## 3. 技术栈

| 组件 | 选型 | 说明 |
|------|------|------|
| 命令行框架 | [clap](https://crates.io/crates/clap) v4 | 支持子命令、自动补全、derive 宏 |
| 异步运行时 | tokio（kafka_client 依赖） | 复用已有依赖 |
| Kafka 客户端 | [kafka_client](https://github.com/zzzdong/kafka_client) | 本地纯 Rust 客户端 |
| 序列化 | serde / serde_json | 配置文件和 JSON 输出 |
| 表格输出 | [comfy-table](https://crates.io/crates/comfy-table) | 终端表格格式化 |
| 配置管理 | 自定义 TOML 配置 | 集群连接信息持久化 |

---

## 4. 命令设计

### 4.1 命令树

```
kfk [--brokers <BROKERS>] [--cluster <CLUSTER>] [--verbose]
├── config                      # 集群配置管理
│   ├── add-cluster <NAME>      #   添加集群
│   │   --brokers <BROKERS>
│   │   --tls                   #   启用 TLS
│   │   --sasl-mechanism        #   SASL 机制 (PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512)
│   │   --sasl-username
│   │   --sasl-password
│   ├── remove-cluster <NAME>   #   删除集群
│   ├── select-cluster <NAME>   #   切换当前集群
│   └── list (默认)              #   列出所有集群
│
├── node                        # 集群节点管理
│   └── ls                      #   列出所有 broker 节点
│
├── topic                       # 主题管理
│   ├── ls (默认)                #   列出所有主题
│   ├── describe <TOPIC>        #   描述主题详情（分区、副本、Leader）
│   ├── create <TOPIC>          #   创建主题
│   │   --partitions            #   分区数（默认 1）
│   │   --replication-factor    #   副本因子（默认 1）
│   │   --config <K=V>          #   主题配置
│   └── delete <TOPIC>          #   删除主题
│
├── produce                     # 生产消息
│   [--key <KEY>]
│   [--partition <PARTITION>]
│   [--header <K:V>]
│   [--input <FORMAT>]          #   输入格式：text | json-each-row
│   <TOPIC>                     #   从 stdin 读取消息
│
├── consume                     # 消费消息
│   [--group <GROUP>]
│   [--output <FORMAT>]         #   输出格式：text | json-each-row
│   [--offset <earliest|latest|N>]
│   [--partition <PARTITION>]
│   [--header <K:V>]            #   按 header 过滤
│   [--tail <N>]                #   消费 N 条后退出
│   <TOPIC>
│
├── group                       # 消费者组管理
│   ├── ls (默认)                #   列出所有消费者组
│   ├── describe <GROUP>        #   描述消费者组详情
│   │   --topic <TOPIC>         #   按主题过滤
│   ├── commit <GROUP>          #   提交/重置偏移
│   │   -t/--topic <TOPIC>
│   │   --offset <earliest|latest|N>
│   │   --partition <N>
│   │   --all-partitions
│   └── delete <GROUP>          #   删除消费者组
│
├── completion                  # Shell 自动补全
│   (bash | zsh | fish | powershell)
│
└── help / --help / --version
```

### 4.2 全局参数

| 参数 | 简写 | 说明 |
|------|------|------|
| `--brokers` | `-b` | 指定 broker 地址列表（覆盖配置文件） |
| `--cluster` | `-c` | 临时切换当前集群 |
| `--verbose` | `-v` | 输出调试日志 |

---

## 5. 配置管理

### 5.1 配置文件路径

```
$HOME/.kfk/config.toml
```

### 5.2 配置格式

```toml
current_cluster = "local"

[clusters.local]
brokers = ["127.0.0.1:9092"]
security_protocol = "PLAINTEXT"  # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL

[clusters.local.sasl]
mechanism = "SCRAM-SHA-256"
username = "admin"
password = "admin-secret"

[clusters.remote]
brokers = ["kafka-1:9092", "kafka-2:9092"]
security_protocol = "SSL"

[clusters.remote.tls]
insecure = false
ca_file = "/etc/kafka/ca.pem"
cert_file = "/etc/kafka/client.pem"
key_file = "/etc/kafka/client-key.pem"
```

### 5.3 配置加载优先级

1. 命令行参数 `--brokers` / `--cluster`（最高）
2. 配置文件中的 `current_cluster`
3. 环境变量 `KFK_BROKERS`
4. 默认 `127.0.0.1:9092`

---

## 6. 客户端抽象层设计

封装 `kafka_client` 的复杂 API，为 CLI 层提供简洁接口。

### 6.1 ClientFactory

```rust
pub struct ClientFactory;

impl ClientFactory {
    /// 根据集群配置创建 KafkaClient
    pub async fn create(config: &ClusterConfig) -> Result<KafkaClient>;

    /// 根据集群配置和分组 ID 创建 Consumer
    pub async fn create_consumer(client: &KafkaClient, group_id: &str, offset: AutoOffsetReset) -> Result<Consumer>;

    /// 根据集群配置创建 Producer
    pub async fn create_producer(client: &KafkaClient) -> Result<Producer>;
}
```

### 6.2 AdminClient 封装

```rust
pub struct AdminClient {
    cluster: Arc<ClusterClient>,
}

impl AdminClient {
    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>>;
    pub async fn describe_topic(&self, name: &str) -> Result<TopicDetail>;
    pub async fn create_topic(&self, req: CreateTopicsRequest) -> Result<()>;
    pub async fn delete_topic(&self, name: &str) -> Result<()>;
    pub async fn list_brokers(&self) -> Result<Vec<BrokerInfo>>;
    pub async fn list_groups(&self) -> Result<Vec<GroupInfo>>;
    pub async fn describe_group(&self, group_id: &str) -> Result<GroupDetail>;
    pub async fn commit_offset(&self, req: OffsetCommitRequest) -> Result<()>;
}
```

---

## 7. 数据流

### 7.1 消息生产

```
stdin → kfk produce → Producer.send() → kafka_client → Kafka Broker
```

- 从标准输入逐行读取消息
- 支持 `--key`、`--header`、`--partition` 参数
- 批量发送后调用 `flush()` 确保投递
- 输出发送结果（分区、偏移）

### 7.2 消息消费

```
Kafka Broker → kafka_client → Consumer.poll() → stdout
```

- 订阅指定主题
- 轮询消息并格式输出
- 支持指定分区、偏移起始位置
- 支持 `--tail N` 消费 N 条后自动退出

### 7.3 管理操作

```
kfk topic create → AdminClient → ClusterClient.send_to_any_broker(CreateTopicsRequest)
kfk topic ls → AdminClient → ClusterClient.refresh_metadata → MetadataCache.get_all_topics()
```

---

## 8. 项目结构

```
/home/alex/code/rust/kfk/
├── Cargo.toml
├── src/
│   ├── main.rs                  # 入口：clap 命令树定义
│   ├── cli/                     # CLI 层
│   │   ├── mod.rs
│   │   ├── args.rs              # 全局参数和子命令参数定义
│   │   ├── output.rs            # 输出格式化（表格、JSON、文本）
│   │   └── completion.rs        # Shell 自动补全
│   ├── commands/                # Command 层
│   │   ├── mod.rs
│   │   ├── config.rs            # config 子命令
│   │   ├── node.rs              # node 子命令
│   │   ├── topic.rs             # topic 子命令
│   │   ├── produce.rs           # produce 子命令
│   │   ├── consume.rs           # consume 子命令
│   │   └── group.rs             # group 子命令
│   ├── client/                  # 客户端抽象层
│   │   ├── mod.rs
│   │   ├── factory.rs           # ClientFactory
│   │   ├── admin.rs             # AdminClient
│   │   └── types.rs             # CLI 层使用的数据模型
│   └── config/                  # 配置管理
│       ├── mod.rs
│       ├── loader.rs            # 配置文件加载与写入
│       └── model.rs             # 配置数据结构
└── docs/
    └── design.md                # 本设计文档
```

---

## 9. 关键数据结构

### 9.1 配置文件模型

```rust
#[derive(Debug, Deserialize, Serialize)]
pub struct AppConfig {
    pub current_cluster: Option<String>,
    pub clusters: HashMap<String, ClusterConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ClusterConfig {
    pub brokers: Vec<String>,
    pub security_protocol: SecurityProtocolType,
    pub sasl: Option<SaslConfig>,
    pub tls: Option<TlsConfig>,
}
```

### 9.2 CLI 数据模型

```rust
pub struct TopicInfo {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
}

pub struct TopicDetail {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub configs: HashMap<String, String>,
}

pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub offset_range: (i64, i64),  // (earliest, latest)
}

pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub is_controller: bool,
}

pub struct GroupInfo {
    pub group_id: String,
    pub protocol: String,
    pub state: String,        // Stable | Empty | PreparingRebalance
    pub members: i32,
}

pub struct GroupDetail {
    pub group_id: String,
    pub state: String,
    pub coordinator: BrokerInfo,
    pub members: Vec<GroupMember>,
}

pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignment: Vec<TopicPartition>,
}
```

### 9.3 与 kafka_client 的类型映射

| CLI 类型 | kafka_client 对应 |
|----------|------------------|
| `TopicInfo` | `protocol::MetadataResponseTopic` |
| `BrokerInfo` | `protocol::MetadataResponseBroker` |
| `TopicDetail` | `MetadataCache::get_topic()` 返回值 |
| `ProducerRecord` | `producer::ProducerRecord` |
| `ConsumerRecord` | `consumer::ConsumerRecord` |
| `ConsumerConfig` | `consumer::ConsumerConfig` |

---

## 10. 输出格式

### 10.1 表格输出（默认）

```
$ kfk topic ls
  NAME              PARTITIONS  REPLICATION
  example-topic     3           1
  my-topic          1           1

$ kfk topic describe example-topic
  Name: example-topic
  Partitions: 3
  ┌──────┬────────┬──────────┬─────┐
  │  ID  │ Leader │ Replicas │ ISR │
  ├──────┼────────┼──────────┼─────┤
  │  0   │  1     │ [1]      │ [1] │
  │  1   │  1     │ [1]      │ [1] │
  │  2   │  1     │ [1]      │ [1] │
  └──────┴────────┴──────────┴─────┘
```

### 10.2 JSON 输出（`--output json`）

```json
{
  "topic": "example-topic",
  "partition": 0,
  "offset": 42,
  "timestamp": "2026-06-24T10:00:00.000Z",
  "headers": [{"key": "h1", "value": "v1"}],
  "key": "my-key",
  "payload": "message-body"
}
```

### 10.3 消费输出格式

- `text` — 默认，仅打印消息 value
- `json-each-row` — 每行一个 JSON 对象（含元数据）

---

## 11. 错误处理

```rust
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("Connection failed: {0}")]
    Connection(String),

    #[error("Kafka error: {0}")]
    Kafka(#[from] KafkaError),

    #[error("Config error: {0}")]
    Config(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}
```

所有错误统一向 stderr 输出，退出码：
- 0: 成功
- 1: 一般错误
- 2: 配置错误
- 3: 连接错误

---

## 12. 实施计划

| 阶段 | 里程碑 | 涉及命令 |
|------|--------|---------|
| **P0 - 基础** | CLI 框架搭建 + 配置管理 | `config add-cluster / select-cluster / list` |
| **P0 - 基础** | 集群元数据查询 | `node ls`, `topic ls`, `topic describe` |
| **P1 - 核心** | 消息生产消费 | `produce`, `consume` |
| **P1 - 核心** | 消费者组管理 | `group ls`, `group describe` |
| **P2 - 增强** | Shell 自动补全 | `completion` |
| **P2 - 增强** | 主题管理 | `topic create`, `topic delete` |
| **P2 - 增强** | 偏移管理 | `group commit` |
| **P3 - 完善** | SASL/TLS 支持 | 配置中的安全协议字段 |
| **P3 - 完善** | JSON 输出格式 | 各命令的 `--output json` 选项 |

---

## 13. 依赖清单

```toml
[dependencies]
kafka_client = { path = "../kafka_client" }
clap = { version = "4", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"
comfy-table = "7"
tokio = { version = "1", features = ["full"] }
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
dirs = "6"          # 配置文件路径
```

---

## 14. 与 kaf 的差异

| 特性 | kaf (Go) | kfk (Rust) |
|------|----------|------------|
| 底层库 | sarama (Go) | kafka_client (Rust) |
| Avro 解码 | 支持 | 阶段性不支持 |
| Protobuf 解码 | 支持 | 阶段性不支持 |
| OAuth/OIDC | 支持 | 阶段性不支持 |
| MSK IAM | 支持 | 阶段性不支持 |
| 性能 | 中等 | 预期更好（零成本抽象） |
| 二进制体积 | ~15MB | 预期更小 |
