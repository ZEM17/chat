package main

import (
	"chat/common/pb"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/streadway/amqp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
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
	logicAddr = flag.String("logic", "localhost:50051", "Logic service gRPC address") // New flag

	redisClient *redis.Client
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
	authClient  pb.AuthServiceClient // gRPC Client
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
			log.Printf("Failed to publish message: %v", err)
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

	// *** Init gRPC Client ***
	conn, err := grpc.Dial(*logicAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect to Logic: %v", err)
	}
	defer conn.Close()
	authClient = pb.NewAuthServiceClient(conn)
	log.Printf("Connected to Logic service at %s", *logicAddr)

	hub := newHub()
	go hub.run()
	go hub.startConsumer()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	// Expose HTTP APIs backed by gRPC
	http.HandleFunc("/api/login", handleLogin)
	http.HandleFunc("/api/register", handleRegister)

	log.Printf("Gateway server (ID: %s) started on %s", *gatewayID, *addr)
	err = http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
