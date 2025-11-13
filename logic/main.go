package main

import (
	"chat/logic/db" // 导入新的 db 包
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/golang-jwt/jwt/v5" // 导入 JWT 包
	"github.com/streadway/amqp"
)

// ... (const block 保持不变) ...
const (
	exchangeMessages   = "im.messages"
	exchangeDelivery   = "im.delivery"
	queueLogicMessages = "logic.messages.queue"

	exchangeSystem   = "im.system"
	queueLogicSystem = "logic.system.queue"
)

// 新增: JWT 签名密钥 (生产中应使用更复杂且保密的密钥)
var jwtKey = []byte("my_secret_key")

// Claims 结构用于 JWT
type Claims struct {
	UserID   string `json:"user_id"`
	Nickname string `json:"nickname"`
	jwt.RegisteredClaims
}

// *** 新增: 离线消息拉取请求结构体 ***
type OfflinePullRequest struct {
	UserID    string `json:"user_id"`
	GatewayID string `json:"gateway_id"`
}

var (
	// ... (redisAddr 和 amqpAddr 保持不变) ...
	redisAddr = flag.String("redis", "localhost:6379", "Redis address")
	amqpAddr  = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "AMQP (RabbitMQ) address")

	// 新增: MySQL DSN (Data Source Name)
	mysqlDSN = flag.String("mysql", "root:rootpassword@tcp(localhost:3306)/im_db?parseTime=true", "MySQL DSN")
	httpAddr = flag.String("http", ":8081", "HTTP API service address")

	// ... (redisClient, amqpConn, amqpChannel, ctx 保持不变) ...
	redisClient *redis.Client
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
	ctx         = context.Background()
)

// ... (UpstreamMessage 和 DownstreamMessage 结构体保持不变) ...
type UpstreamMessage struct {
	FromUserID string `json:"from_user_id"`
	Payload    []byte `json:"payload"`
	Type       string `json:"type"`
	ToUserID   string `json:"to_user_id"`
	ToGroupID  string `json:"to_group_id"`
}
type DownstreamMessage struct {
	ToUserID string `json:"to_user_id"`
	Payload  []byte `json:"payload"`
}

// ... (failOnError 保持不变) ...
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

// ... (initRedis 和 initAMQP 保持不变) ...
func initRedis() {
	log.Println("Connecting to Redis...")
	redisClient = redis.NewClient(&redis.Options{
		Addr: *redisAddr,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Redis connected.")
}

func initAMQP() {
	log.Println("Connecting to RabbitMQ...")
	var err error
	amqpConn, err = amqp.Dial(*amqpAddr)
	failOnError(err, "Failed to connect to RabbitMQ")
	amqpChannel, err = amqpConn.Channel()
	failOnError(err, "Failed to open RabbitMQ channel")
	log.Println("RabbitMQ connected and channel opened.")
	// (省略 ExchangeDeclare, 因为它们在 startConsumer 中也会被声明)

	// *** 新增: 声明系统交换机 ***
	err = amqpChannel.ExchangeDeclare(exchangeSystem, "topic", true, false, false, false, nil)
	failOnError(err, "Failed to declare 'im.system' exchange")
}

// *** (新增) ***
// initDB 初始化数据库
func initDB() {
	db.InitDB(*mysqlDSN)
}

// *** (新增) ***
// handleRegister 处理用户注册
func handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Nickname string `json:"nickname"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Username == "" || req.Password == "" {
		http.Error(w, "Username and password are required", http.StatusBadRequest)
		return
	}
	if req.Nickname == "" {
		req.Nickname = req.Username // 默认昵称
	}

	id, err := db.CreateUser(req.Username, req.Password, req.Nickname)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create user: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("User created successfully: %s (ID: %d)", req.Username, id)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"message":  "User created successfully",
		"username": req.Username,
	})
}

// *** (新增) ***
// handleLogin 处理用户登录并签发 JWT
func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 1. 验证用户
	userID, nickname, err := db.ValidateUser(req.Username, req.Password)
	if err != nil {
		http.Error(w, fmt.Sprintf("Login failed: %v", err), http.StatusUnauthorized)
		return
	}

	// 2. 创建 JWT
	// (注意：UserID 在 IM 系统中通常是字符串，我们在这里转换一下)
	// (大纲中使用的是 UserA, UserB 这样的字符串 ID, 我们将使用数据库的数字 ID)
	// (为了与之前的 user_id=UserA 兼容, 我们暂时使用 'user:'+ID)
	// (在下一步中，我们将统一使用数据库的数字 ID)
	userIDStr := strconv.FormatInt(userID, 10)

	expirationTime := time.Now().Add(24 * time.Hour)
	claims := &Claims{
		UserID:   userIDStr, // ** 关键 **
		Nickname: nickname,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtKey)
	if err != nil {
		http.Error(w, "Failed to generate token", http.StatusInternalServerError)
		return
	}

	log.Printf("User %s (ID: %s) logged in successfully", req.Username, userIDStr)

	// 3. 返回 Token
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message":  "Login successful",
		"token":    tokenString,
		"user_id":  userIDStr, // 方便客户端使用
		"nickname": nickname,
	})
}

func handleMessage(d amqp.Delivery) {
	var msg UpstreamMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		log.Printf("Failed to unmarshal upstream message (poison message). Discarding: %v", err)
		d.Ack(true) // 丢弃无法解析的毒消息
		return
	}
	log.Printf("Received message from %s, type: %s, to: %s", msg.FromUserID, msg.Type, msg.ToUserID)

	// *** (修改点 1: 修复死循环) ***
	_, err := db.SaveMessage(msg.FromUserID, msg.ToUserID, msg.ToGroupID, msg.Payload, msg.Type)
	if err != nil {
		if errors.Is(err, db.ErrUserNotFound) {
			// 这是我们预期的错误：接收者/发送者不存在
			log.Printf("Failed to save message: %v. Discarding message.", err)
			// TODO: 在这里, 我们可以选择性地给发送者(msg.FromUserID)
			// 发送一条系统消息，告诉他 "用户不存在"
			// 1. 确定是哪个用户不存在 (用于错误消息)
			var errorMsg string
			if strings.Contains(err.Error(), "recipient user") {
				errorMsg = fmt.Sprintf("发送失败: 对方用户 (ID: %s) 不存在。", msg.ToUserID)
			} else if strings.Contains(err.Error(), "sender user") {
				errorMsg = "发送失败: 您的账户似乎无效。" // 这种情况理论上不应发生
			} else {
				errorMsg = "发送失败: 目标用户不存在。"
			}

			// 2. 构建系统消息 payload (这是将发送给客户端的)
			// (客户端可以解析这个 JSON 来显示错误)
			systemPayload := map[string]string{
				"type":           "system_error", // 客户端可以识别的类型
				"content":        errorMsg,
				"ref_to_user_id": msg.ToUserID, // 告诉客户端是发往哪个 ID 的消息失败了
			}
			systemPayloadBytes, jsonErr := json.Marshal(systemPayload)
			if jsonErr != nil {
				log.Printf("Failed to marshal system error message: %v", jsonErr)
				d.Ack(true) // 序列化失败，只能丢弃了
				return
			}

			// 3. 查找发送者 (msg.FromUserID) 在哪个网关
			locationKey := "user:" + msg.FromUserID + ":location"
			gatewayID, redisErr := redisClient.Get(ctx, locationKey).Result()

			if redisErr == redis.Nil {
				// 发送者自己也离线了，那就算了
				log.Printf("Sender %s is offline, cannot deliver system error.", msg.FromUserID)
			} else if redisErr != nil {
				// Redis 查错了
				log.Printf("Failed to get sender %s location from Redis: %v", msg.FromUserID, redisErr)
			} else {
				// 4. 发送者在线，将系统消息投递回去
				downMsg := DownstreamMessage{
					ToUserID: msg.FromUserID,     // 接收者是 *发送者*
					Payload:  systemPayloadBytes, // 内容是 *系统错误*
				}
				msgBody, err := json.Marshal(downMsg)
				if err != nil {
					log.Printf("Failed to marshal downstream system error: %v", err)
				} else {
					// 发布到 im.delivery，路由到发送者所在的网关
					err = amqpChannel.Publish(
						exchangeDelivery,
						gatewayID, // 路由键 = 发送者所在的网关 ID
						false,
						false,
						amqp.Publishing{
							ContentType: "application/json",
							Body:        msgBody,
						})
					if err != nil {
						log.Printf("Failed to publish system error back to sender %s: %v", msg.FromUserID, err)
					} else {
						log.Printf("Published system error back to sender %s (Gateway: %s)", msg.FromUserID, gatewayID)
					}
				}
			}
			d.Ack(true) // 确认并丢弃此消息
		} else {
			// 这是其他错误 (数据库连接、毒消息格式等)
			log.Printf("Failed to save message to DB (poison message?). Discarding: %v. Body: %s", err, string(d.Body))
			d.Ack(true) // 同样丢弃
		}
		return // *** 关键: 停止处理此消息 ***
	}
	// *** (修改点 1 结束) ***

	if msg.Type == "private" {
		locationKey := "user:" + msg.ToUserID + ":location"
		gatewayID, err := redisClient.Get(ctx, locationKey).Result()

		if err == redis.Nil {
			log.Printf("User %s is OFFLINE. (Location key %s not found)", msg.ToUserID, locationKey)

			// *** (修改点 2: 修复死循环) ***
			err := db.SaveOfflineMessage(msg.ToUserID, d.Body)
			if err != nil {
				log.Printf("Failed to save offline message (poison message?). Discarding: %v. Body: %s", err, string(d.Body))
				// 离线存储失败，Nack(false, true) 会导致死循环
				// 我们改为 Ack(true) 丢弃它
				d.Ack(true)
				return
			}
			// *** (修改点 2 结束) ***

			log.Printf("Saved offline message for user %s", msg.ToUserID)
			d.Ack(true) // 确认消息 (已存入离线)
			return
		}
		if err != nil {
			log.Printf("Failed to get user location from Redis: %v. Re-queueing...", err)
			d.Nack(false, true) // Redis 错误，可以重试
			return
		}

		log.Printf("User %s is ONLINE at Gateway %s. Forwarding message...", msg.ToUserID, gatewayID)

		// 封装投递消息
		downMsg := DownstreamMessage{
			ToUserID: msg.ToUserID,
			Payload:  msg.Payload,
		}
		msgBody, err := json.Marshal(downMsg)
		if err != nil {
			log.Printf("Failed to marshal downstream message (poison message). Discarding: %v", err)
			d.Ack(true) // 丢弃
			return
		}

		// 发布到 'im.delivery'
		err = amqpChannel.Publish(
			exchangeDelivery,
			gatewayID,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        msgBody,
			})

		if err != nil {
			log.Printf("Failed to publish message to %s (Key: %s): %v. Re-queueing...", exchangeDelivery, gatewayID, err)
			d.Nack(false, true) // RMQ 发布失败，可以重试
			return
		}

	} else if msg.Type == "group" {
		log.Printf("Received group message for group %s. TODO: Implement group fanout.", msg.ToGroupID)
	} else {
		log.Printf("Received unknown message type: %s", msg.Type)
	}

	d.Ack(true)
}

// ... (startConsumer 保持不变) ...
func startConsumer() {
	// (为确保健壮性，在 consumer 这边也声明 exchanges)
	err := amqpChannel.ExchangeDeclare(exchangeMessages, "topic", true, false, false, false, nil)
	failOnError(err, "Failed to declare 'im.messages' exchange")
	err = amqpChannel.ExchangeDeclare(exchangeDelivery, "direct", true, false, false, false, nil)
	failOnError(err, "Failed to declare 'im.delivery' exchange")

	q, err := amqpChannel.QueueDeclare(queueLogicMessages, true, false, false, false, nil)
	failOnError(err, "Failed to declare 'logic.messages.queue'")
	err = amqpChannel.QueueBind(q.Name, "msg.*", exchangeMessages, false, nil)
	failOnError(err, "Failed to bind queue to 'im.messages' exchange")
	log.Printf("Queue %s bound to exchange %s (Binding Key: %s)", q.Name, exchangeMessages, "msg.*")
	err = amqpChannel.Qos(1, 0, false)
	failOnError(err, "Failed to set QoS")
	msgs, err := amqpChannel.Consume(q.Name, "", false, false, false, false, nil)
	failOnError(err, "Failed to register consumer")
	log.Println("Logic consumer started. Waiting for messages...")
	go func() {
		for d := range msgs {
			handleMessage(d)
		}
	}()
}

// *** (新增) ***
// startSystemConsumer 启动用于处理系统消息 (如下线拉取) 的消费者
func startSystemConsumer() {
	// 声明队列
	q, err := amqpChannel.QueueDeclare(queueLogicSystem, true, false, false, false, nil)
	failOnError(err, "Failed to declare 'logic.system.queue'")

	// 绑定到 im.system 交换机，路由键 "system.offline.pull"
	bindingKey := "system.offline.pull"
	err = amqpChannel.QueueBind(q.Name, bindingKey, exchangeSystem, false, nil)
	failOnError(err, fmt.Sprintf("Failed to bind queue %s to %s", q.Name, exchangeSystem))
	log.Printf("Queue %s bound to exchange %s (Binding Key: %s)", q.Name, exchangeSystem, bindingKey)

	// 设置 QoS
	err = amqpChannel.Qos(1, 0, false)
	failOnError(err, "Failed to set QoS for system queue")

	// 消费消息
	msgs, err := amqpChannel.Consume(q.Name, "", false, false, false, false, nil)
	failOnError(err, "Failed to register system consumer")
	log.Println("System consumer started. Waiting for messages...")

	go func() {
		for d := range msgs {
			handleSystemMessage(d)
		}
	}()
}

func handleSystemMessage(d amqp.Delivery) {
	if d.RoutingKey != "system.offline.pull" {
		log.Printf("Unknown system message routing key: %s", d.RoutingKey)
		d.Ack(true) // 丢弃
		return
	}

	var req OfflinePullRequest
	if err := json.Unmarshal(d.Body, &req); err != nil {
		log.Printf("Failed to unmarshal offline pull request (poison message). Discarding: %v", err)
		d.Ack(true) // 丢弃
		return
	}
	log.Printf("Received offline pull request for User: %s (Gateway: %s)", req.UserID, req.GatewayID)

	// 1. 启动事务
	tx, err := db.DB.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v. Acknowledging request (will retry on next login)", err)
		d.Ack(true) // 确认拉取请求 (失败)，防止循环
		return
	}
	defer tx.Rollback()

	// 2. 在事务中获取并锁定消息
	ids, messages, err := db.GetOfflineMessages(tx, req.UserID)
	if err != nil {
		log.Printf("Failed to get offline messages from DB: %v. Acknowledging request (will retry on next login)", err)
		// 如果表不存在 (Error 1146)，重试也没用。Ack 此拉取请求。
		d.Ack(true)
		return
	}

	if len(messages) == 0 {
		log.Printf("No offline messages found for user %s", req.UserID)
		d.Ack(true)
		return
	}
	log.Printf("Found %d offline messages for user %s. Pushing to gateway %s...", len(messages), req.UserID, req.GatewayID)

	// 3. 循环推送消息到 'im.delivery'
	for _, msgData := range messages {
		var upMsg UpstreamMessage
		if err := json.Unmarshal(msgData, &upMsg); err != nil {
			log.Printf("Failed to parse offline message data (poison data in DB). Skipping: %v", err)
			continue
		}

		downMsg := DownstreamMessage{
			ToUserID: req.UserID,
			Payload:  upMsg.Payload,
		}
		msgBody, err := json.Marshal(downMsg)
		if err != nil {
			log.Printf("Failed to marshal downstream message (poison data in DB). Skipping: %v", err)
			continue
		}

		err = amqpChannel.Publish(
			exchangeDelivery,
			req.GatewayID,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        msgBody,
			})

		if err != nil {
			log.Printf("Failed to publish offline message to %s (Key: %s): %v. Acknowledging (will retry on next login)", exchangeDelivery, req.GatewayID, err)
			d.Ack(true) // 确认拉取请求 (部分失败)
			return
		}
	}

	// 4. 在事务中删除这些消息
	if err := db.ClearOfflineMessages(tx, ids); err != nil {
		log.Printf("Failed to clear offline messages: %v. Acknowledging (will retry on next login)", err)
		d.Ack(true) // 确认拉取请求 (失败)
		return
	}

	// 5. 提交事务
	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v. Acknowledging (will retry on next login)", err)
		d.Ack(true) // 确认拉取请求 (失败)
		return
	}

	// 6. 确认拉取请求
	log.Printf("Successfully pushed %d offline messages for user %s", len(messages), req.UserID)
	d.Ack(true)
}

// *** (修改) ***
// main 函数
func main() {
	flag.Parse()

	// 初始化 (顺序很重要)
	initDB()
	initRedis()
	initAMQP()
	defer amqpConn.Close()
	defer amqpChannel.Close()
	defer db.DB.Close() // 关闭数据库连接
	defer redisClient.Close()

	// 启动 RabbitMQ 消费者
	startConsumer()

	// 启动系统消费者
	go startSystemConsumer()

	// *** (新增) ***
	// 启动 HTTP API 服务
	http.HandleFunc("/api/register", handleRegister)
	http.HandleFunc("/api/login", handleLogin)
	log.Printf("HTTP API server starting on %s", *httpAddr)
	go func() {
		if err := http.ListenAndServe(*httpAddr, nil); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// 优雅停机
	log.Println("Logic service running. Press CTRL+C to exit.")
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
	<-sc

	log.Println("Shutting down Logic service...")
}
