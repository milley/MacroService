# gRPC Demo: Rust Server + Node.js Client

一个演示 gRPC 不同调用模式的简单计算器服务。

## 项目结构

```
.
├── proto/
│   └── calculator.proto      # 服务定义
├── server-rust/              # Rust 服务端
│   ├── Cargo.toml
│   ├── build.rs
│   └── src/main.rs
├── client-node/              # Node.js 客户端
│   ├── package.json
│   └── index.js
└── README.md
```

## 服务接口

| 方法 | 类型 | 说明 |
|------|------|------|
| `Add` | 一元调用 | 两数相加 |
| `StreamPrimes` | 服务端流 | 生成质数序列 |
| `SumStream` | 客户端流 | 计算多个数字的和 |

## 运行步骤

### 1. 启动 Rust 服务端

```bash
cd server-rust
cargo run
```

服务将在 `127.0.0.1:50051` 监听。

### 2. 设置 Node.js 客户端

```bash
cd client-node
npm install
npm run generate    # 生成 proto 代码
npm start           # 运行客户端
```

## 预期输出

**服务端：**
```
Calculator gRPC server listening on 127.0.0.1:50051
[Add] 10 + 20
[StreamPrimes] Generating primes < 30
[SumStream] Receiving numbers...
  received: 1, running sum: 1
  received: 2, running sum: 3
  ...
```

**客户端：**
```
=== gRPC Calculator Client Demo ===

1. Unary Call - Add(10, 20)
   Result: 30

2. Server Streaming - Primes < 30
   Primes: 2, 3, 5, 7, 11, 13, 17, 19, 23, 29

3. Client Streaming - Sum of [1, 2, 3, 4, 5]
   Sum: 15, Count: 5

=== Demo Complete ===
```

## 学习要点

1. **Proto 定义**：服务接口与消息类型的声明
2. **代码生成**：Rust 用 `tonic-build`，Node 用 `grpc-tools`
3. **三种调用模式**：一元、服务端流、客户端流
