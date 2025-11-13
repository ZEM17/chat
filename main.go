package main

import (
	"flag"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

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

var (
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
)

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

// readPump 将消息从 WebSocket 连接泵送到 hub。
//
// 每个连接一个 goroutine 运行 readPump。
// 当 readPump 退出时，应用程序将关闭连接。
func (c *Client) readPump() {
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

		// 在这里，我们将消息打印到日志
		// TODO (步骤 2): 将消息 (message) 封装并发布到 RabbitMQ 的 'im.messages' (Topic Exchange)
		// 消息应包含 from_user_id (c.userID)
		log.Printf("recv from client %s: %s", c.userID, message)

		// 暂时先做一个 Echo Server, 广播给所有人
		c.hub.broadcast <- message
	}
}

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

// Hub 维护一组活动的客户端，并向客户端广播消息。
type Hub struct {
	// 注册的客户端.
	clients map[string]*Client // key 是 userID

	// Inbound messages from the clients.
	broadcast chan []byte

	// Register requests from the clients.
	register chan *Client

	// Unregister requests from clients.
	unregister chan *Client

	// 确保 clients map 的并发安全
	mu sync.RWMutex
}

func newHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[string]*Client),
	}
}

func (h *Hub) run() {
	// TODO (步骤 3): 在这里启动 RabbitMQ 消费者
	// 消费者将监听 'im.delivery' (Direct Exchange) 中
	// 绑定到此 Gateway 实例 (例如 'gw1') 的独占队列。
	// 收到消息后，调用 h.sendMessageToClient(userID, message)

	for {
		select {
		case client := <-h.register:
			// TODO (步骤 2):
			// 1. 在 Redis 中注册: SET user:A:location gw1
			// 2. 在 Redis 中更新在线状态
			// 3. (可选) 向 Logic 发送 "user_online" 事件
			// 4. (可选) 检查离线消息
			h.mu.Lock()
			h.clients[client.userID] = client
			h.mu.Unlock()
			log.Printf("Client %s connected", client.userID)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.userID]; ok {
				delete(h.clients, client.userID)
				close(client.send)
				log.Printf("Client %s disconnected", client.userID)
				// TODO (步骤 2):
				// 1. 在 Redis 中删除/更新 location
				// 2. 更新 Redis 在线状态
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			// TODO: 这个 'broadcast' 只是临时的 Echo 功能
			// 真正的逻辑 (4.2 节) 是:
			// Logic -> Bus (im.delivery) -> Gateway (gw1/gw2) -> Client
			// Gateway 不应该执行广播
			h.mu.RLock()
			for userID, client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, userID)
				}
			}
			h.mu.RUnlock()
		}
	}
}

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

// serveWs 处理来自 peer 的 websocket 请求。
func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	// TODO (步骤 2): JWT 认证
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

func main() {
	flag.Parse()
	hub := newHub()
	go hub.run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	log.Printf("Gateway server started on %s", *addr)
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
