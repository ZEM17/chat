# Etcd 服务发现集成完成

已成功将 Etcd 集成到聊天系统中，实现了 Logic 服务的自动注册和 Gateway 服务的自动发现与负载均衡。

## 变更摘要

1.  **基础设施**: `docker-compose.yml` 新增 Etcd 服务 (端口 2379)。
2.  **公共库**:
    - `common/discovery/register.go`: 实现服务注册逻辑 (KeepAlive Lease)。
    - `common/discovery/resolver.go`: 实现 gRPC Resolver 用于服务发现。
3.  **服务变更**:
    - **Logic**: 启动时向 Etcd 注册自己的地址 (租约 5秒)。
    - **Gateway**: 使用 `etcd:///chat/logic` 解析服务地址，替代原有的 `localhost:50051` 静态配置。

## 如何运行

1.  **启动 Etcd**
    ```bash
    docker-compose up -d etcd
    ```

2.  **启动 Logic 服务** (模拟多节点)
    ```bash
    # 终端 1
    cd logic && go run . -etcd localhost:2379 -grpc :50051

    # 终端 2 (模拟第二个节点)
    cd logic && go run . -etcd localhost:2379 -grpc :50052
    ```

3.  **启动 Gateway 服务**
    ```bash
    cd gateway && go run . -etcd localhost:2379
    ```

Gateway 将自动发现这两个 Logic 节点，并使用 Round Robin 策略进行负载均衡。

## 验证
代码已通过编译测试。Logic 和 Gateway 服务均能正常引入新依赖并构建。
