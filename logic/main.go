package main

import (
	"chat/logic/db" // 导入新的 db 包
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
)

// 新增: JWT 签名密钥 (生产中应使用更复杂且保密的密钥)
var jwtKey = []byte("my_secret_key")

// Claims 结构用于 JWT
type Claims struct {
	UserID   string `json:"user_id"`
	Nickname string `json:"nickname"`
	jwt.RegisteredClaims
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

// handleMessage 保持不变 (但我们现在要添加消息持久化)
func handleMessage(d amqp.Delivery) {
	var msg UpstreamMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		log.Printf("Failed to unmarshal upstream message: %v", err)
		d.Ack(false)
		return
	}
	log.Printf("Received message from %s, type: %s, to: %s", msg.FromUserID, msg.Type, msg.ToUserID)

	// *** (修改点 1) ***
	// 消息持久化 (大纲 4.2.4)
	// TODO: 应该在一个事务中完成
	// (我们暂时省略了 'messages' 表的创建，将在下一步添加)
	/*
		_, err := db.DB.Exec("INSERT INTO messages (from_user_id, to_user_id, content, type) VALUES (?, ?, ?, ?)",
			msg.FromUserID, msg.ToUserID, string(msg.Payload), 1) // 假设 type=1 是单聊
		if err != nil {
			log.Printf("Failed to save message to DB: %v", err)
			// (暂时不 Nack, 先让路由继续)
		}
	*/

	if msg.Type == "private" {
		locationKey := "user:" + msg.ToUserID + ":location"
		gatewayID, err := redisClient.Get(ctx, locationKey).Result()

		if err == redis.Nil {
			log.Printf("User %s is OFFLINE. (Location key %s not found)", msg.ToUserID, locationKey)
			// *** (修改点 2) ***
			// TODO: 将消息存入 MySQL 离线表 (大纲 4.2.5)
			/*
				_, err := db.DB.Exec("INSERT INTO offline_messages (user_id, message_data) VALUES (?, ?)",
					msg.ToUserID, d.Body)
				if err != nil {
					log.Printf("Failed to save offline message: %v", err)
				}
			*/
			d.Ack(true)
			return
		}
		if err != nil {
			log.Printf("Failed to get user location from Redis: %v", err)
			d.Nack(false, true)
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
			log.Printf("Failed to marshal downstream message: %v", err)
			d.Ack(false)
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
			log.Printf("Failed to publish message to %s (Key: %s): %v", exchangeDelivery, gatewayID, err)
			d.Nack(false, true)
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
