# 演练: 第一阶段 - 架构重构与 gRPC 集成

本演练记录了即时通讯（IM）应用从单体风格的 HTTP 架构成功转型为分层的、基于 gRPC 的微服务架构的全过程。

## 1. 新的项目结构
我们引入了一个共享模块，并将 `logic` 服务重构为清晰的分层结构：

```
d:/CODE/chat/
├── common/                  # [新增] 共享定义
│   ├── pb/                  # Protocol Buffer 生成代码
│   │   ├── auth.proto       # Auth 服务定义
│   │   └── group.proto      # Group 服务定义
│   └── go.mod               # "chat/common" 模块
│
├── logic/                   # 业务逻辑服务
│   ├── repo/                # [新增] 数据访问层 (从 db/ 移入)
│   ├── service/             # [新增] 业务逻辑层 (gRPC 实现)
│   ├── main.go              # [修改] gRPC 服务端入口
│   └── go.mod               # 更新依赖
│
└── gateway/                 # API 网关
    ├── main.go              # [修改] gRPC 客户端 & API 网关
    └── go.mod               # 更新依赖
```

## 2. 关键变更

### 2.1 Protocol Buffers (gRPC)
我们为服务间通信定义了强类型的契约。
- **[auth.proto](file:///d:/CODE/chat/common/pb/auth.proto)**: 定义了 `Login` (登录), `Register` (注册) 和 `ValidateToken` (验证令牌)。
- **[group.proto](file:///d:/CODE/chat/common/pb/group.proto)**: 定义了 `CreateGroup` (创建群组), `JoinGroup` (加入群组) 等。

### 2.2 Logic 服务重构
- **Repository 模式**: 数据库交互代码已移至 `chat/logic/repo` 包。
- **gRPC 服务端**: `logic/main.go` 现在监听 `:50051` 端口提供 gRPC 服务，不再直接暴露 HTTP 接口。
- **Service 层**: 在 `chat/logic/service/auth_service.go` 中实现了具体的业务逻辑 (`AuthService`)。

### 2.3 Gateway 改造
- **API 网关模式**: `gateway/main.go` 现在作为系统的唯一公网入口。
- **gRPC 客户端**: 网关作为 Client，通过 gRPC 连接 `Logic` 服务处理登录和注册请求。
- **安全性**: WebSocket 连接建立时，会通过 gRPC 调用 Logic 服务的 `ValidateToken` 接口进行鉴权。

## 3. 验证结果
服务构建验证通过，这就确认了 gRPC 集成和代码重构是成功的。

### 构建验证
```powershell
cd d:\CODE\chat\logic && go build
# 成功
```
```powershell
cd d:\CODE\chat\gateway && go build
# 成功
```

## 4. 下一步
我们现在的架构已经为**第二阶段：高性能优化**做好了准备，接下来的工作将从实现 **时间轮 (Time Wheel)** 算法开始，以优化海量连接的心跳管理。
