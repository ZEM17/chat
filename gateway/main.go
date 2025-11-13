package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt" // *** 新增: 导入 fmt 包 ***
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/golang-jwt/jwt/v5" // *** 新增: 导入 JWT 库 ***
	"github.com/gorilla/websocket"
	"github.com/streadway/amqp"
)

// ... (const block: writeWait, pongWait, pingPeriod, maxMessageSize 保持不变) ...
const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

// ... (const block: RabbitMQ Exchange 名称保持不变) ...
const (
	exchangeMessages = "im.messages"
	exchangeDelivery = "im.delivery"
	exchangeSystem   = "im.system"
)

// *** 新增: JWT 签名密钥 ***
// (必须与 logic/main.go 中的 jwtKey 完全一致)
var jwtKey = []byte("my_secret_key")

// *** 新增: Claims 结构用于 JWT ***
// (必须与 logic/main.go 中的 Claims 结构体完全一致)
type Claims struct {
	UserID   string `json:"user_id"`
	Nickname string `json:"nickname"`
	jwt.RegisteredClaims
}

// *** 新增: 离线消息拉取请求结构体 ***
// (逻辑层也需要一个完全一样的)
type OfflinePullRequest struct {
	UserID    string `json:"user_id"`
	GatewayID string `json:"gateway_id"`
}

var (
	// ... (addr, upgrader, gatewayID, redisAddr, amqpAddr 保持不变) ...
	addr     = flag.String("addr", ":8080", "http service address")
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	gatewayID = flag.String("id", "gw1", "gateway instance ID (e.g., gw1, gw2)")
	redisAddr = flag.String("redis", "localhost:6379", "Redis address")
	amqpAddr  = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "AMQP (RabbitMQ) address")

	// ... (全局客户端: redisClient, amqpConn, amqpChannel, ctx 保持不变) ...
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

// Client 结构体保持不变
type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	send   chan []byte
	userID string // 这个 userID 现在将从 JWT 中提取
}

// readPump 保持不变
// (它从 c.userID 获取 FromUserID, c.userID 现在是认证过的)
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	if err := c.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		log.Printf("Failed to set read deadline: %v", err)
		return
	}
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}

		// (临时解析 to_user_id 的逻辑保持不变)
		// 客户端现在必须发送 {"to_user_id": "2", "content": "..."} (使用数据库 ID)
		var clientMsg struct {
			ToUserID string `json:"to_user_id"`
		}
		if err := json.Unmarshal(message, &clientMsg); err != nil {
			log.Printf("Failed to parse client message JSON: %v. Raw: %s", err, string(message))
			continue
		}

		routingKey := "msg.private" // 假设是单聊

		upMsg := UpstreamMessage{
			FromUserID: c.userID, // (来自 JWT)
			Payload:    message,
			Type:       "private",
			ToUserID:   clientMsg.ToUserID, // (来自客户端 JSON)
		}

		msgBody, err := json.Marshal(upMsg)
		if err != nil {
			log.Printf("Failed to marshal upstream message: %v", err)
			continue
		}

		err = amqpChannel.Publish(
			exchangeMessages,
			routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        msgBody,
			})
		if err != nil {
			log.Printf("Failed to publish message to %s: %v", exchangeMessages, err)
		} else {
			log.Printf("Client %s sent message (to: %s), published to %s", c.userID, clientMsg.ToUserID, exchangeMessages)
		}
	}
}

// writePump 保持不变
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				log.Printf("Failed to set write deadline: %v", err)
				return
			}
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				log.Printf("Failed to write message: %v", err)
				return
			}
		case <-ticker.C:
			if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				log.Printf("Failed to set write deadline for ping: %v", err)
				return
			}
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("Failed to send ping: %v", err)
				return
			}
		}
	}
}

// Hub 结构体和 newHub() 保持不变
type Hub struct {
	clients    map[string]*Client
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

func newHub() *Hub {
	return &Hub{
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[string]*Client),
	}
}

// Hub.run() 保持不变
// (它使用 client.userID, 这个 ID 现在是认证过的)
func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			// 1. 在 Redis 中注册: SET user:1:location gw1
			locationKey := "user:" + client.userID + ":location"
			err := redisClient.Set(ctx, locationKey, *gatewayID, 12*time.Hour).Err()
			if err != nil {
				log.Printf("Failed to set user location in Redis: %v", err)
				client.conn.Close()
				continue
			}

			h.mu.Lock()
			h.clients[client.userID] = client
			h.mu.Unlock()
			// *** (日志更新) ***
			log.Printf("Client %s (from token) connected, location set to %s", client.userID, *gatewayID)

			go h.requestOfflineMessages(client.userID, *gatewayID)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.userID]; ok {
				delete(h.clients, client.userID)
				close(client.send)
				// *** (日志更新) ***
				log.Printf("Client %s (from token) disconnected", client.userID)

				locationKey := "user:" + client.userID + ":location"
				val, err := redisClient.Get(ctx, locationKey).Result()
				if err == nil && val == *gatewayID {
					errDel := redisClient.Del(ctx, locationKey).Err()
					if errDel != nil {
						log.Printf("Failed to delete user location from Redis: %v", errDel)
					} else {
						log.Printf("Client %s location deleted from Redis (was %s)", client.userID, *gatewayID)
					}
				} else if err != redis.Nil && err != nil {
					log.Printf("Failed to check user location before deleting: %v", err)
				}
			}
			h.mu.Unlock()
		}
	}
}

// requestOfflineMessages 向 RabbitMQ 发送一个拉取请求
func (h *Hub) requestOfflineMessages(userID, gatewayID string) {
	pullReq := OfflinePullRequest{
		UserID:    userID,
		GatewayID: gatewayID,
	}
	reqBody, err := json.Marshal(pullReq)
	if err != nil {
		log.Printf("Failed to marshal offline pull request: %v", err)
		return
	}

	err = amqpChannel.Publish(
		exchangeSystem,        // exchange
		"system.offline.pull", // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        reqBody,
		})

	if err != nil {
		log.Printf("Failed to publish offline pull request for %s: %v", userID, err)
	} else {
		log.Printf("Published offline pull request for %s (to %s)", userID, exchangeSystem)
	}
}

// Hub.startConsumer() 和 sendMessageToClient() 保持不变
func (h *Hub) startConsumer() {
	log.Println("Starting RabbitMQ Consumer...")
	err := amqpChannel.ExchangeDeclare(exchangeDelivery, "direct", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare 'im.delivery' exchange: %v", err)
	}
	q, err := amqpChannel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare exclusive queue: %v", err)
	}
	log.Printf("Declared exclusive queue: %s", q.Name)
	err = amqpChannel.QueueBind(q.Name, *gatewayID, exchangeDelivery, false, nil)
	if err != nil {
		log.Fatalf("Failed to bind queue %s to exchange %s with key %s: %v", q.Name, exchangeDelivery, *gatewayID, err)
	}
	log.Printf("Queue %s bound to exchange %s (Routing Key: %s)", q.Name, exchangeDelivery, *gatewayID)
	msgs, err := amqpChannel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}
	go func() {
		for d := range msgs {
			log.Printf("Received message from %s for delivery (Routing Key: %s)", exchangeDelivery, d.RoutingKey)
			var msg DownstreamMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("Failed to unmarshal downstream message: %v", err)
				continue
			}
			h.sendMessageToClient(msg.ToUserID, msg.Payload)
		}
	}()
	log.Println("RabbitMQ Consumer running. Waiting for messages.")
}
func (h *Hub) sendMessageToClient(userID string, message []byte) {
	h.mu.RLock()
	client, ok := h.clients[userID]
	h.mu.RUnlock()
	if !ok {
		log.Printf("Failed to send message: Client %s not found on this gateway (%s)", userID, *gatewayID)
		return
	}
	select {
	case client.send <- message:
		log.Printf("Sent message to client %s (on %s)", userID, *gatewayID)
	default:
		log.Printf("Failed to send message: Client %s send channel full (on %s)", userID, *gatewayID)
	}
}

// *** (重大修改) ***
// serveWs 处理来自 peer 的 websocket 请求。
func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {

	// *** 1. 移除旧的 user_id query param 逻辑 ***
	// tempUserID := r.URL.Query().Get("user_id") ... (已删除)

	// *** 2. 新增: 从 query param 获取 token ***
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		http.Error(w, "token is required", http.StatusUnauthorized)
		return
	}

	// *** 3. 新增: 解析和验证 JWT ***
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
		// 确保签名方法是我们期望的
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			// *** 修改: log.Errorf -> fmt.Errorf ***
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtKey, nil
	})

	if err != nil || !token.Valid {
		log.Printf("Invalid token: %v", err)
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}

	// *** 4. 新增: 从 token 中提取 userID ***
	// (这个 userID 现在是 "1", "2" 这样的数据库 ID 字符串)
	userID := claims.UserID
	log.Printf("Token validated. Client UserID: %s, Nickname: %s", userID, claims.Nickname)

	// *** 5. (修改) 使用从 token 中获取的 userID 进行单点登录检查 ***
	locationKey := "user:" + userID + ":location"
	existingGW, err := redisClient.Get(ctx, locationKey).Result()
	if err == nil && existingGW != *gatewayID {
		log.Printf("Client %s attempting to connect to %s, but already connected on %s. Denying.", userID, *gatewayID, existingGW)
		http.Error(w, "User already connected on another gateway", http.StatusConflict)
		return
	}
	if err != nil && err != redis.Nil {
		log.Printf("Redis error checking user location: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// 升级连接
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	// *** 6. (修改) 使用从 token 中获取的 userID 创建 Client ***
	client := &Client{
		hub:    hub,
		conn:   conn,
		send:   make(chan []byte, 256),
		userID: userID, // (使用认证过的 userID)
	}
	client.hub.register <- client

	go client.writePump()
	go client.readPump()
}

// ... (initRedis, initAMQP, main 函数保持不变) ...
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
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	amqpChannel, err = amqpConn.Channel()
	if err != nil {
		log.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}
	log.Println("RabbitMQ connected and channel opened.")
	err = amqpChannel.ExchangeDeclare(
		exchangeMessages,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare 'im.messages' exchange: %v", err)
	}
	log.Printf("Exchange %s declared.", exchangeMessages)

	err = amqpChannel.ExchangeDeclare(
		exchangeSystem,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare 'im.system' exchange: %v", err)
	}
	log.Printf("Exchange %s declared.", exchangeSystem)
}

func main() {
	flag.Parse()
	initRedis()
	initAMQP()
	defer amqpConn.Close()
	defer amqpChannel.Close()
	defer redisClient.Close()

	hub := newHub()
	go hub.run()
	go hub.startConsumer()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	log.Printf("Gateway server (ID: %s) started on %s", *gatewayID, *addr)
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
