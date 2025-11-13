package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/streadway/amqp"
)

// ... (const block 保持不变) ...
const (
	// 允许的 WebSocket 写入等待时间
	writeWait = 10 * time.Second

	// 允许的 Pong 消息等待时间
	pongWait = 60 * time.Second

	// 发送 Ping 消息的周期 (必须小于 pongWait)
	pingPeriod = (pongWait * 9) / 10

	// 允许的最大消息大小
	maxMessageSize = 512
)

// RabbitMQ Exchange 名称
const (
	exchangeMessages = "im.messages" // Topic Exchange, 用于上报消息 (Gateway -> Logic)
	exchangeDelivery = "im.delivery" // Direct Exchange, 用于投递消息 (Logic -> Gateway)
)

var (
	// ... (addr 和 upgrader 保持不变) ...
	addr = flag.String("addr", ":8080", "http service address")

	// upgrader 将 HTTP 连接升级到 WebSocket
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		// 暂时允许所有来源，生产中应进行检查
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	// 新增: 启动参数
	gatewayID = flag.String("id", "gw1", "gateway instance ID (e.g., gw1, gw2)")
	redisAddr = flag.String("redis", "localhost:6379", "Redis address")
	amqpAddr  = flag.String("amqp", "amqp://admin:HZM111@localhost:5672/", "AMQP (RabbitMQ) address")

	// 新增: 全局客户端
	redisClient *redis.Client
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
	ctx         = context.Background()
)

// UpstreamMessage 是 Gateway -> Logic 的消息结构 (发布到 im.messages)
type UpstreamMessage struct {
	FromUserID string `json:"from_user_id"`
	Payload    []byte `json:"payload"`     // 原始 WebSocket 消息
	Type       string `json:"type"`        // "private" or "group" (从 payload 解析)
	ToUserID   string `json:"to_user_id"`  // 单聊目标
	ToGroupID  string `json:"to_group_id"` // 群聊目标
}

// DownstreamMessage 是 Logic -> Gateway 的消息结构 (从 im.delivery 消费)
type DownstreamMessage struct {
	ToUserID string `json:"to_user_id"` // 目标用户
	Payload  []byte `json:"payload"`    // 要发送给客户端的真正消息体
}

// Client 结构体保持不变
// Client 是连接到服务器的 WebSocket 客户端的中间表示。
type Client struct {
	hub *Hub

	// WebSocket 连接.
	conn *websocket.Conn

	//  buffered channel of outbound messages.
	send chan []byte

	// 用户的唯一标识 (将来从 JWT 中获取)
	userID string
}

// readPump 将消息从 WebSocket 连接泵送到 RabbitMQ。
func (c *Client) readPump() {
	// ... (defer 和 SetReadLimit/SetReadDeadline/SetPongHandler 保持不变) ...
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	// 设置 ReadDeadline 以检测掉线
	if err := c.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		log.Printf("Failed to set read deadline: %v", err)
		return
	}
	// 设置 Pong 处理器来重置 ReadDeadline
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		// 读取消息
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}

		// *** (修改点 1) ***
		// 移除: c.hub.broadcast <- message
		// 新增: 将消息发布到 RabbitMQ 'im.messages' (Topic Exchange)

		// 简单的解析 (生产中应使用更健壮的 JSON 解析)
		// 假设客户端发送的消息格式: {"type": "private_message", "payload": {"to_user_id": "UserB", "content": "Hi"}}
		// (为了简化，我们这里只做演示，实际应定义严格的结构体)

		// TODO: 解析 'message' 来确定 routingKey (e.g., 'msg.private' or 'msg.group')
		// 和 'to_user_id' / 'to_group_id'
		// 这里我们暂时硬编码
		routingKey := "msg.private" // 假设是单聊

		// 封装上报消息
		// (生产中, 这里的解析应该更健壮, 例如解析 JSON 来获取 'to_user_id' 等)
		upMsg := UpstreamMessage{
			FromUserID: c.userID,
			Payload:    message,      // 原始消息体
			Type:       "private",    // 假设
			ToUserID:   "UserB_TODO", // TODO: 从 message 中解析
		}

		msgBody, err := json.Marshal(upMsg)
		if err != nil {
			log.Printf("Failed to marshal upstream message: %v", err)
			continue
		}

		// 发布消息
		err = amqpChannel.Publish(
			exchangeMessages, // exchange
			routingKey,       // routing key (e.g., "msg.private", "msg.group")
			false,            // mandatory
			false,            // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        msgBody,
			})
		if err != nil {
			log.Printf("Failed to publish message to %s: %v", exchangeMessages, err)
			// TODO: 处理发布失败 (e.g., 重试, 缓存)
		} else {
			log.Printf("Client %s sent message, published to %s", c.userID, exchangeMessages)
		}
	}
}

// writePump 保持不变
// writePump 将消息从 hub 泵送到 WebSocket 连接。
//
// 每个连接一个 goroutine 运行 writePump。
func (c *Client) writePump() {
	// Ping 计时器
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
				// hub 关闭了此 channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// 将消息写入 WebSocket
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				log.Printf("Failed to write message: %v", err)
				return
			}

		case <-ticker.C:
			// 发送 Ping 消息
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

// Hub 维护一组活动的客户端
type Hub struct {
	// ... (clients, register, unregister, mu 保持不变) ...
	// 注册的客户端.
	clients map[string]*Client // key 是 userID

	// Register requests from the clients.
	register chan *Client

	// Unregister requests from clients.
	unregister chan *Client

	// 确保 clients map 的并发安全
	mu sync.RWMutex

	// 移除: broadcast chan []byte
}

func newHub() *Hub {
	return &Hub{
		// 移除: broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[string]*Client),
	}
}

func (h *Hub) run() {
	// *** (移除) ***
	// 移除 RabbitMQ 消费者逻辑, 移至独立的 startConsumer()

	for {
		select {
		case client := <-h.register:
			// *** (修改点 2) ***
			// 1. 在 Redis 中注册: SET user:A:location gw1
			// (我们假设连接永久有效，生产中应使用 TTL)
			locationKey := "user:" + client.userID + ":location"
			err := redisClient.Set(ctx, locationKey, *gatewayID, 0).Err()
			if err != nil {
				log.Printf("Failed to set user location in Redis: %v", err)
				// TODO: 注册失败，可能需要关闭连接
			}

			// 2. 在 Redis 中更新在线状态 (简化)
			// redisClient.Set(ctx, "user:"+client.userID+":status", "online", 0)

			h.mu.Lock()
			h.clients[client.userID] = client
			h.mu.Unlock()
			log.Printf("Client %s connected, location set to %s", client.userID, *gatewayID)

			// 3. (可选) 向 Logic 发送 "user_online" 事件
			// (可以通过 readPump 向 'im.messages' 发送一个 'system.online' 类型的消息)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.userID]; ok {
				delete(h.clients, client.userID)
				close(client.send)
				log.Printf("Client %s disconnected", client.userID)

				// *** (修改点 3) ***
				// 1. 在 Redis 中删除/更新 location
				locationKey := "user:" + client.userID + ":location"
				err := redisClient.Del(ctx, locationKey).Err()
				if err != nil {
					log.Printf("Failed to delete user location from Redis: %v", err)
				} else {
					log.Printf("Client %s location deleted from Redis", client.userID)
				}

				// 2. 更新 Redis 在线状态 (简化)
				// redisClient.Set(ctx, "user:"+client.userID+":status", "offline", 0)
			}
			h.mu.Unlock()

			// *** (移除) ***
			// 移除: case message := <-h.broadcast:
		}
	}
}

// *** (新增) ***
// startConsumer 启动 RabbitMQ 消费者，监听 'im.delivery' Exchange。
// (大纲 4.1.8 节)
func (h *Hub) startConsumer() {
	log.Println("Starting RabbitMQ Consumer...")

	// 1. 声明 'im.delivery' (Direct Exchange)
	err := amqpChannel.ExchangeDeclare(
		exchangeDelivery, // name
		"direct",         // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare 'im.delivery' exchange: %v", err)
	}

	// 2. 创建一个独占的、自动删除的队列
	q, err := amqpChannel.QueueDeclare(
		"",    // name (让 RabbitMQ 自动生成)
		false, // durable
		true,  // delete when unused
		true,  // exclusive (独占)
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare exclusive queue: %v", err)
	}
	log.Printf("Declared exclusive queue: %s", q.Name)

	// 3. 将队列绑定到 'im.delivery', Routing Key 为本 Gateway 实例的 ID
	err = amqpChannel.QueueBind(
		q.Name,           // queue name
		*gatewayID,       // routing key (e.g., "gw1")
		exchangeDelivery, // exchange
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind queue %s to exchange %s with key %s: %v", q.Name, exchangeDelivery, *gatewayID, err)
	}
	log.Printf("Queue %s bound to exchange %s (Routing Key: %s)", q.Name, exchangeDelivery, *gatewayID)

	// 4. 开始消费
	msgs, err := amqpChannel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	// 循环处理消息
	go func() {
		for d := range msgs {
			log.Printf("Received message from %s for delivery", exchangeDelivery)
			var msg DownstreamMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("Failed to unmarshal downstream message: %v", err)
				continue
			}

			// 调用 Hub 的方法将消息推送到本地 WebSocket 连接
			h.sendMessageToClient(msg.ToUserID, msg.Payload)
		}
	}()

	log.Println("RabbitMQ Consumer running. Waiting for messages.")
}

// sendMessageToClient 保持不变
// sendMessageToClient 是由 Hub 的 RabbitMQ 消费者调用的。
// 它将消息精确地发送给此 Gateway 实例上的特定用户。
func (h *Hub) sendMessageToClient(userID string, message []byte) {
	h.mu.RLock()
	client, ok := h.clients[userID]
	h.mu.RUnlock()

	if !ok {
		log.Printf("Failed to send message: Client %s not found on this gateway", userID)
		return
	}

	select {
	case client.send <- message:
		log.Printf("Sent message to client %s", userID)
	default:
		// 如果发送缓冲区已满，则认为客户端已掉线或处理缓慢
		log.Printf("Failed to send message: Client %s send channel full", userID)
		// 可以在这里采取措施，例如关闭连接
		// close(client.send)
		// delete(h.clients, userID)
	}
}

// serveWs 保持不变 (仍使用临时 user_id)
// serveWs 处理来自 peer 的 websocket 请求。
func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	// TODO (步骤 3): JWT 认证
	// 1. 从 r.URL.Query().Get("token") 获取 token
	// 2. 验证 token
	// 3. 从 token 中解析出 userID

	// 临时使用 "user_id" query param 作为 userID
	tempUserID := r.URL.Query().Get("user_id")
	if tempUserID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	client := &Client{
		hub:    hub,
		conn:   conn,
		send:   make(chan []byte, 256),
		userID: tempUserID, // 临时 userID
	}
	client.hub.register <- client

	// 启动 goroutines
	go client.writePump()
	go client.readPump()
}

// *** (新增) ***
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

// *** (新增) ***
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

	// 声明 'im.messages' (Topic Exchange)，用于消息上报
	err = amqpChannel.ExchangeDeclare(
		exchangeMessages, // name
		"topic",          // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare 'im.messages' exchange: %v", err)
	}
	log.Printf("Exchange %s declared.", exchangeMessages)
}

func main() {
	flag.Parse()

	// *** (修改点 4) ***
	// 初始化外部连接
	initRedis()
	initAMQP()
	defer amqpConn.Close()
	defer amqpChannel.Close()
	defer redisClient.Close()

	hub := newHub()
	go hub.run()

	// 启动 RabbitMQ 消费者
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
