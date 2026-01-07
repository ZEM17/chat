package main

import (
	"chat/common/discovery"
	"chat/common/pb"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/streadway/amqp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"chat/common/pool"
	"chat/common/timewheel" // Import
	_ "net/http/pprof"      // Enable pprof
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

var (
	// Global TimeWheel for Heartbeats
	// 59s delay (pongWait is 60s), 1s tick.
	// SlotNum = 3600 (1 hour support), typically we need just > 60.
	globalTimeWheel *timewheel.TimeWheel
)

const (
	exchangeMessages = "im.messages"
	exchangeDelivery = "im.delivery"
	exchangeSystem   = "im.system"
)

type OfflinePullRequest struct {
	UserID    string `json:"user_id"`
	GatewayID string `json:"gateway_id"`
}

var (
	addr      = flag.String("addr", ":8080", "http service address")
	gatewayID = flag.String("id", "gw1", "gateway instance ID")
	redisAddr = flag.String("redis", "localhost:6379", "Redis address")
	amqpAddr  = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "AMQP address")
	logicAddr = flag.String("logic", "etcd:///chat/logic", "Logic service address (etcd target)")
	etcdAddr  = flag.String("etcd", "localhost:2379", "Etcd address")

	redisClient *redis.Client
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
	authClient  pb.AuthServiceClient  // gRPC Client
	groupClient pb.GroupServiceClient // gRPC Client
	ctx         = context.Background()

	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

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

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	send   chan []byte
	userID string
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	// TimeWheel Optimizations:
	// Instead of SetReadDeadline (Poller), we use TimeWheel to track activity.
	// But we still need a hard deadline on socket to unblock ReadMessage if network dies silently?
	// standard SetReadDeadline is fine, but TimeWheel is used for application level logic
	// or to avoid resetting timer syscalls too often.
	// For "Resume Highlight", users often replace SetReadDeadline with just TimeWheel closing the conn.
	// Let's do that: Remove SetReadDeadline (or set it very long), and use TimeWheel to Close().

	// c.conn.SetReadDeadline(time.Now().Add(pongWait)) // Removed

	// Add to TimeWheel
	heartbeat := func() {
		log.Printf("Client %s timed out (TimeWheel)", c.userID)
		c.conn.Close() // This will trigger read error and cleanup
	}
	globalTimeWheel.Add(pongWait, c.userID, heartbeat)

	c.conn.SetPongHandler(func(string) error {
		// Reset TimeWheel
		// globalTimeWheel.Add will automaticall remove old task if key matches
		globalTimeWheel.Add(pongWait, c.userID, heartbeat)
		return nil
		// return c.conn.SetReadDeadline(time.Now().Add(pongWait)) // Removed
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			// Remove from TimeWheel on error/close
			globalTimeWheel.Remove(c.userID)

			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}

		// Reset on message received too? Usually yes for IM.
		globalTimeWheel.Add(pongWait, c.userID, heartbeat)

		var clientMsg struct {
			ToUserID string `json:"to_user_id"`
		}
		if err := json.Unmarshal(message, &clientMsg); err != nil {
			log.Printf("Failed to parse client message JSON: %v. Raw: %s", err, string(message))
			continue
		}

		routingKey := "msg.private"

		upMsg := UpstreamMessage{
			FromUserID: c.userID,
			Payload:    message,
			Type:       "private",
			ToUserID:   clientMsg.ToUserID,
		}

		// Optimize: Use BufferPool + Encoder to avoid allocating new []byte for every message
		buf := pool.GetBuffer()
		err = json.NewEncoder(buf).Encode(upMsg)
		if err != nil {
			log.Printf("Failed to encode upstream message: %v", err)
			pool.PutBuffer(buf)
			continue
		}

		// Note: Encoder adds a newline at the end. We might want to trim it if strict.
		// For JSON compatibility, trailing newline is usually fine.
		// If strict needed: buf.Truncate(buf.Len()-1)

		err = amqpChannel.Publish(
			exchangeMessages,
			routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        buf.Bytes(), // Does not copy? streadway says: "The body is not copied".
			})

		// After Publish returns, it's safe to reuse buffer (data written to kernel socket buffer)
		pool.PutBuffer(buf)

		if err != nil {
			log.Printf("Failed to publish message: %v", err)
		} else {
			log.Printf("Client %s sent message (to: %s), published to %s", c.userID, clientMsg.ToUserID, exchangeMessages)
		}
	}
}

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
				return
			}
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				return
			}
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

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

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			locationKey := "user:" + client.userID + ":location"
			err := redisClient.Set(ctx, locationKey, *gatewayID, 12*time.Hour).Err()
			if err != nil {
				log.Printf("Failed to set user location: %v", err)
				client.conn.Close()
				continue
			}

			h.mu.Lock()
			h.clients[client.userID] = client
			h.mu.Unlock()
			log.Printf("Client %s connected", client.userID)

			go h.requestOfflineMessages(client.userID, *gatewayID)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.userID]; ok {
				delete(h.clients, client.userID)
				close(client.send)
				log.Printf("Client %s disconnected", client.userID)

				locationKey := "user:" + client.userID + ":location"
				val, err := redisClient.Get(ctx, locationKey).Result()
				if err == nil && val == *gatewayID {
					redisClient.Del(ctx, locationKey)
				}
			}
			h.mu.Unlock()
		}
	}
}

func (h *Hub) requestOfflineMessages(userID, gatewayID string) {
	pullReq := OfflinePullRequest{
		UserID:    userID,
		GatewayID: gatewayID,
	}
	reqBody, err := json.Marshal(pullReq)
	if err != nil {
		return
	}

	amqpChannel.Publish(
		exchangeSystem,
		"system.offline.pull",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        reqBody,
		})
}

func (h *Hub) startConsumer() {
	err := amqpChannel.ExchangeDeclare(exchangeDelivery, "direct", true, false, false, false, nil)
	failOnError(err, "Failed to declare exchange")
	q, err := amqpChannel.QueueDeclare("", false, true, true, false, nil)
	failOnError(err, "Failed to declare queue")

	err = amqpChannel.QueueBind(q.Name, *gatewayID, exchangeDelivery, false, nil)
	failOnError(err, "Failed to bind queue")

	msgs, err := amqpChannel.Consume(q.Name, "", true, false, false, false, nil)
	failOnError(err, "Failed to register consumer")

	go func() {
		for d := range msgs {
			var msg DownstreamMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				continue
			}
			h.sendMessageToClient(msg.ToUserID, msg.Payload)
		}
	}()
}

func (h *Hub) sendMessageToClient(userID string, message []byte) {
	h.mu.RLock()
	client, ok := h.clients[userID]
	h.mu.RUnlock()
	if ok {
		select {
		case client.send <- message:
		default:
		}
	}
}

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		http.Error(w, "token is required", http.StatusUnauthorized)
		return
	}

	// *** gRPC Validation ***
	resp, err := authClient.ValidateToken(ctx, &pb.ValidateTokenRequest{Token: tokenStr})
	if err != nil || !resp.Valid {
		log.Printf("Token validation failed: %v", err)
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}

	userID := resp.UserId
	log.Printf("Client %s (Token Validated via gRPC) connecting...", userID)

	locationKey := "user:" + userID + ":location"
	existingGW, err := redisClient.Get(ctx, locationKey).Result()
	if err == nil && existingGW != *gatewayID {
		http.Error(w, "User already connected on another gateway", http.StatusConflict)
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
		userID: userID,
	}
	client.hub.register <- client

	go client.writePump()
	go client.readPump()
}

// HTTP Handler Wrappers for gRPC
func handleLogin(w http.ResponseWriter, r *http.Request) {
	// Simple proxy to gRPC
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pb.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	resp, err := authClient.Login(ctx, &req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Login failed: %v", err), http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pb.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	resp, err := authClient.Register(ctx, &req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Register failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleCreateGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pb.CreateGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	resp, err := groupClient.CreateGroup(ctx, &req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleJoinGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pb.JoinGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	resp, err := groupClient.JoinGroup(ctx, &req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleGetGroupMembers(w http.ResponseWriter, r *http.Request) {
	groupIDStr := r.URL.Query().Get("group_id")
	groupID, _ := strconv.ParseInt(groupIDStr, 10, 64)

	resp, err := groupClient.GetGroupMembers(ctx, &pb.GetGroupMembersRequest{GroupId: groupID})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func initRedis() {
	redisClient = redis.NewClient(&redis.Options{Addr: *redisAddr})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
}

func initAMQP() {
	var err error
	amqpConn, err = amqp.Dial(*amqpAddr)
	failOnError(err, "Failed to connect to RabbitMQ")
	amqpChannel, err = amqpConn.Channel()
	failOnError(err, "Failed to open RabbitMQ channel")

	amqpChannel.ExchangeDeclare(exchangeMessages, "topic", true, false, false, false, nil)
	amqpChannel.ExchangeDeclare(exchangeSystem, "topic", true, false, false, false, nil)
}

func main() {
	flag.Parse()
	initRedis()
	initAMQP()
	defer amqpConn.Close()
	defer amqpChannel.Close()
	defer redisClient.Close()

	// *** Init gRPC Client with Etcd Resolver ***
	etcdResolver := discovery.NewEtcdResolver([]string{*etcdAddr})

	conn, err := grpc.Dial(*logicAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(etcdResolver),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
	)
	if err != nil {
		log.Fatalf("did not connect to Logic: %v", err)
	}
	defer conn.Close()
	authClient = pb.NewAuthServiceClient(conn)
	groupClient = pb.NewGroupServiceClient(conn) // Add Client
	log.Printf("Connected to Logic service at %s", *logicAddr)

	// *** Init TimeWheel ***
	// 1s interval, 60 slots (1 min cycle covers pongWait 60s)
	// Actually pongWait is 60s, so we need > 60 slots if delay is 60s?
	// Add implementation uses (delay/interval)%slots. It supports multiple circles.
	// So 60 slots is fine.
	globalTimeWheel = timewheel.New(1*time.Second, 100)
	globalTimeWheel.Start()
	log.Println("TimeWheel started.")
	defer globalTimeWheel.Stop()

	hub := newHub()
	go hub.run()
	go hub.startConsumer()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	// Expose HTTP APIs backed by gRPC
	http.HandleFunc("/api/login", handleLogin)
	http.HandleFunc("/api/register", handleRegister)

	// Group APIs
	http.HandleFunc("/api/group/create", handleCreateGroup)
	http.HandleFunc("/api/group/join", handleJoinGroup)
	http.HandleFunc("/api/group/members", handleGetGroupMembers)

	log.Printf("Gateway server (ID: %s) started on %s", *gatewayID, *addr)
	err = http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
