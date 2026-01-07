# 系统架构设计 (System Architecture Design)

## 1. 概览

本文档描述了高性能分布式即时通讯（IM）系统的架构设计。本系统旨在支持百万级并发连接、高吞吐量消息投递以及水平扩展能力。

## 2. 架构图

```mermaid
graph TD
    User((客户端)) -->|WebSocket / HTTP| LB[负载均衡 (Nginx/Cloud)]
    LB --> Gateway1[网关服务 1]
    LB --> Gateway2[网关服务 2]
    
    subgraph "接入层 (无状态)"
    Gateway1
    Gateway2
    end
    
    Gateway1 -->|gRPC| Logic[逻辑服务集群]
    Gateway2 -->|gRPC| Logic
    
    subgraph "业务层 (有状态逻辑)"
    Logic
    end
    
    Logic -->|异步批量写入| DB[(MySQL)]
    Logic -->|缓存 / 会话| Redis[(Redis)]
    Gateway1 -->|发布/订阅| MQ[(RabbitMQ)]
    Gateway2 -->|发布/订阅| MQ
    MQ -->|消费| Logic
    
    classDef service fill:#f9f,stroke:#333,stroke-width:2px;
    classDef storage fill:#ff9,stroke:#333,stroke-width:2px;
    class Gateway1,Gateway2,Logic service;
    class DB,Redis,MQ storage;
```

## 3. 核心组件

### 3.1 网关服务 (Gateway Service - 接入层)
- **职责**: 管理 WebSocket 连接、用户鉴权、协议升级。
- **核心特性**:
  - **哈希时间轮 (Hashed Timing Wheel)**: 优化百万级连接的心跳管理（复杂度 $O(1)$）。
  - **对象池 (sync.Pool)**: 针对高频消息转发实现零分配内存管理。
  - **无状态**: 可通过负载均衡器进行水平扩展。

### 3.2 逻辑服务 (Logic Service - 业务层)
- **职责**: 处理所有业务逻辑、数据持久化、RPC 请求。
- **核心特性**:
  - **异步批量写入 (Async Batch Writer)**: 缓冲入库消息并批量写入 MySQL，最大化吞吐量。
  - **批量确认 (Batch ACK)**: 仅在 DB 持久化成功后才向 MQ 发送确认，保证零数据丢失。
  - **群消息扩散 (Group Fan-out)**: 实现“写扩散”模式，高效投递群消息。
  - **gRPC 服务**: 暴露内部 API 用于鉴权 (`AuthService`) 和群组管理 (`GroupService`)。

### 3.3 基础设施
- **MySQL**: 持久化存储用户、消息和群组数据。
- **Redis**: 实时会话存储 (映射 `UserID` -> `GatewayID`) 及热点数据缓存。
- **RabbitMQ**: 异步消息总线，解耦网关层与逻辑层，削峰填谷，确保消息可靠投递。

## 4. 数据流

### 4.1 登录与连接
1. 客户端连接 `ws://gateway/ws?token=JWT`。
2. Gateway 通过 gRPC 调用 `Logic.ValidateToken`。
3. 验证通过后，Gateway 将客户端注册到本地 Hub 和 Redis (`UserID` -> `GatewayID`)。

### 4.2 单聊 (P2P)
1. 用户 A 通过 WebSocket 发送消息给 Gateway A。
2. Gateway A 将消息发布到 RabbitMQ (`topic: im.messages`)。
3. Logic Service 消费消息。
4. **持久化**:
   - Logic 验证发送者。
   - 消息推入 `AsyncWriter` 缓冲区。
   - 批量刷盘至 MySQL。
5. **投递**:
   - Logic 查询 Redis 获取用户 B 的位置 (Gateway B)。
   - Logic 发布“下行消息”到 RabbitMQ (`direct: gateway_ID`)。
6. Gateway B 收到消息并通过 WebSocket 推送给用户 B。

### 4.3 群聊
1. Logic 收到 MQ 中的 `type: group` 消息。
2. Logic 查询 `group_members` 表获取所有成员 ID。
3. **扩散 (Fan-out)**: Logic 遍历成员，通过 Redis 查找对应的 Gateway，为每个在线成员发布下行消息。

## 5. 数据库设计

### 用户表 (`users`)
| 字段 | 类型 | 描述 |
|-------|------|-------------|
| id    | BIGINT | 主键 |
| email | VARCHAR | 唯一标识 |
| password | VARCHAR | 哈希密码 |
| nickname | VARCHAR | 昵称 |

### 消息表 (`messages`)
| 字段 | 类型 | 描述 |
|-------|------|-------------|
| id    | BIGINT | 主键, 自增 |
| from_user_id | BIGINT | 发送者 |
| to_user_id | BIGINT | 接收者 (群消息为空) |
| to_group_id | BIGINT | 群 ID (私聊为空) |
| content | TEXT | 消息内容 |
| type | VARCHAR | 'private' 或 'group' |
| created_at | DATETIME | 时间戳 |

### 群组表 (`groups`)
| 字段 | 类型 | 描述 |
|-------|------|-------------|
| id | BIGINT | 主键 |
| name | VARCHAR | 群名 |
| owner_id | BIGINT | 创建者 |

### 群成员表 (`group_members`)
| 字段 | 类型 | 描述 |
|-------|------|-------------|
| id | BIGINT | 主键 |
| group_id | BIGINT | 外键 |
| user_id | BIGINT | 外键 |

## 6. 关键设计决策

| 决策点 | 替代方案 | 选择理由 |
|----------|-------------|-------------------|
| **gRPC** | HTTP/REST | 强类型 (Protobuf)，性能更好 (HTTP/2)，自动生成代码。 |
| **RabbitMQ** | Kafka | RabbitMQ 在实时消息场景延迟更低；Kafka 更适合海量日志流处理。 |
| **写扩散** (群聊) | 读扩散 | 对于中小型群组实现更简单。实时推送体验优于轮询拉取 (读扩散)。 |
| **哈希时间轮** | `time.AfterFunc` | `time.AfterFunc` (最小堆) 在百万连接下开销巨大。时间轮复杂度仅为 $O(1)$。 |
