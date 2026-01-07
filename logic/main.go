package main

import (
	"chat/common/discovery"
	"chat/common/pb"
	"chat/logic/repo"
	"chat/logic/service"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"
	"google.golang.org/grpc"

	"net/http"
	_ "net/http/pprof" // Enable pprof
)

const (
	exchangeMessages   = "im.messages"
	exchangeDelivery   = "im.delivery"
	queueLogicMessages = "logic.messages.queue"

	exchangeSystem   = "im.system"
	queueLogicSystem = "logic.system.queue"
)

// OfflinePullRequest 离线消息拉取请求结构体
type OfflinePullRequest struct {
	UserID    string `json:"user_id"`
	GatewayID string `json:"gateway_id"`
}

var (
	redisAddr = flag.String("redis", "localhost:6379", "Redis address")
	amqpAddr  = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "AMQP (RabbitMQ) address")
	mysqlDSN  = flag.String("mysql", "root:rootpassword@tcp(localhost:3306)/im_db?parseTime=true", "MySQL DSN")
	grpcAddr  = flag.String("grpc", ":50051", "gRPC service address")
	etcdAddr  = flag.String("etcd", "localhost:2379", "Etcd address")

	redisClient *redis.Client
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
	ctx         = context.Background()
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

	err = amqpChannel.ExchangeDeclare(exchangeSystem, "topic", true, false, false, false, nil)
	failOnError(err, "Failed to declare 'im.system' exchange")
}

func initDB() {
	repo.InitDB(*mysqlDSN)
}

func handleMessage(d amqp.Delivery) {
	var msg UpstreamMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		log.Printf("Failed to unmarshal upstream message (poison message). Discarding: %v", err)
		d.Ack(true)
		return
	}
	log.Printf("Received message from %s, type: %s, to: %s", msg.FromUserID, msg.Type, msg.ToUserID)

	// Optimization: Async Batch Write
	// 1. Validate IDs
	fromID, err := strconv.ParseInt(msg.FromUserID, 10, 64)
	if err != nil {
		log.Printf("Invalid from_user_id: %v", err)
		d.Ack(true)
		return
	}

	var toID sql.NullInt64
	var groupID sql.NullInt64

	if msg.Type == "private" {
		tid, err := strconv.ParseInt(msg.ToUserID, 10, 64)
		if err != nil {
			log.Printf("Invalid to_user_id: %v", err)
			d.Ack(true)
			return
		}
		toID = sql.NullInt64{Int64: tid, Valid: true}
	} else if msg.Type == "group" {
		// Group Message Fan-out (Write Diffusion)
		if msg.ToGroupID == "" {
			log.Printf("Invalid group_id for group message")
			d.Ack(true)
			return
		}

		groupID, err := strconv.ParseInt(msg.ToGroupID, 10, 64)
		if err != nil {
			log.Printf("Invalid group_id: %v", err)
			d.Ack(true)
			return
		}

		members, err := repo.GetGroupMembers(groupID)
		if err != nil {
			log.Printf("Failed to get group members: %v. Requeuing", err)
			d.Nack(false, true)
			return
		}

		// Simple Loop Fan-out
		for _, memberID := range members {
			// Skip sender? usually yes.
			if memberID == fromID {
				continue
			}

			// Push to each member
			// Optimization: Start goroutines for parallel push if group is large
			// For now, serial push is fine for < 500 members

			// We construct a Downstream message for each member
			downMsg := DownstreamMessage{
				ToUserID: fmt.Sprintf("%d", memberID),
				Payload:  msg.Payload,
			}

			// Deliver to Gateway (via Exchange)
			// Need to find which Gateway the user is on.
			// Query Redis
			locationKey := fmt.Sprintf("user:%d:location", memberID)
			gatewayID, err := redisClient.Get(ctx, locationKey).Result()

			if err == redis.Nil {
				// Offline processing
				// Ideally save to offline_messages table (one per user for Read Diffusion optimization? No, usually one global message ref)
				// Current `SaveOfflineMessage` saves `Payload` blob.
				// If we use Write Diffusion, we insert into `offline_messages` table for this user.
				// This matches our `SaveOfflineMessage` logic.
				repo.SaveOfflineMessage(fmt.Sprintf("%d", memberID), msg.Payload)
				continue
			} else if err != nil {
				log.Printf("Redis error for member %d: %v", memberID, err)
				continue
			}

			// Online
			msgBody, _ := json.Marshal(downMsg)
			amqpChannel.Publish(
				exchangeDelivery,
				gatewayID,
				false,
				false,
				amqp.Publishing{
					ContentType: "application/json",
					Body:        msgBody,
				})
		}
	}

	// 2. Async Push with Batch ACK (Zero Data Loss)
	service.AsyncSaveMessage(fromID, toID, groupID, string(msg.Payload), msg.Type,
		func() {
			// On Success: Ack
			// Note: d.Ack is generally thread-safe in streadway/amqp
			d.Ack(false)
		},
		func() {
			// On Failure: Nack and Requeue
			d.Nack(false, true)
		})

	if msg.Type == "private" {
		locationKey := "user:" + msg.ToUserID + ":location"
		gatewayID, err := redisClient.Get(ctx, locationKey).Result()

		if err == redis.Nil {
			log.Printf("User %s is OFFLINE. (Location key %s not found)", msg.ToUserID, locationKey)

			err := repo.SaveOfflineMessage(msg.ToUserID, d.Body)
			if err != nil {
				log.Printf("Failed to save offline message (poison message?). Discarding: %v. Body: %s", err, string(d.Body))
				d.Ack(true)
				return
			}

			log.Printf("Saved offline message for user %s", msg.ToUserID)
			d.Ack(true)
			return
		}
		if err != nil {
			log.Printf("Failed to get user location from Redis: %v. Re-queueing...", err)
			d.Nack(false, true)
			return
		}

		log.Printf("User %s is ONLINE at Gateway %s. Forwarding message...", msg.ToUserID, gatewayID)

		downMsg := DownstreamMessage{
			ToUserID: msg.ToUserID,
			Payload:  msg.Payload,
		}
		msgBody, err := json.Marshal(downMsg)
		if err != nil {
			log.Printf("Failed to marshal downstream message (poison message). Discarding: %v", err)
			d.Ack(true)
			return
		}

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

func startConsumer() {
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

func startSystemConsumer() {
	q, err := amqpChannel.QueueDeclare(queueLogicSystem, true, false, false, false, nil)
	failOnError(err, "Failed to declare 'logic.system.queue'")

	bindingKey := "system.offline.pull"
	err = amqpChannel.QueueBind(q.Name, bindingKey, exchangeSystem, false, nil)
	failOnError(err, fmt.Sprintf("Failed to bind queue %s to %s", q.Name, exchangeSystem))
	log.Printf("Queue %s bound to exchange %s (Binding Key: %s)", q.Name, exchangeSystem, bindingKey)

	err = amqpChannel.Qos(1, 0, false)
	failOnError(err, "Failed to set QoS for system queue")

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
		d.Ack(true)
		return
	}

	var req OfflinePullRequest
	if err := json.Unmarshal(d.Body, &req); err != nil {
		log.Printf("Failed to unmarshal offline pull request (poison message). Discarding: %v", err)
		d.Ack(true)
		return
	}
	log.Printf("Received offline pull request for User: %s (Gateway: %s)", req.UserID, req.GatewayID)

	tx, err := repo.DB.Begin() // 使用 repo.DB
	if err != nil {
		log.Printf("Failed to begin transaction: %v. Acknowledging request (will retry on next login)", err)
		d.Ack(true)
		return
	}
	defer tx.Rollback()

	ids, messages, err := repo.GetOfflineMessages(tx, req.UserID)
	if err != nil {
		log.Printf("Failed to get offline messages from DB: %v. Acknowledging request (will retry on next login)", err)
		d.Ack(true)
		return
	}

	if len(messages) == 0 {
		log.Printf("No offline messages found for user %s", req.UserID)
		d.Ack(true)
		return
	}
	log.Printf("Found %d offline messages for user %s. Pushing to gateway %s...", len(messages), req.UserID, req.GatewayID)

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
			d.Ack(true)
			return
		}
	}

	if err := repo.ClearOfflineMessages(tx, ids); err != nil {
		log.Printf("Failed to clear offline messages: %v. Acknowledging (will retry on next login)", err)
		d.Ack(true)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v. Acknowledging (will retry on next login)", err)
		d.Ack(true)
		return
	}

	log.Printf("Successfully pushed %d offline messages for user %s", len(messages), req.UserID)
	d.Ack(true)
}

func main() {
	flag.Parse()

	initDB()
	initRedis()
	initAMQP()
	defer amqpConn.Close()
	defer amqpChannel.Close()
	defer repo.DB.Close()
	defer redisClient.Close()

	startConsumer()
	go startSystemConsumer()

	// Start Async DB Writer
	service.StartAsyncWriter()

	// *** PPROF Server ***
	go func() {
		log.Println("Pprof server running on :6060")
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	// *** gRPC Server ***
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	// Register AuthService & GroupService
	authSvc := service.NewAuthService()
	pb.RegisterAuthServiceServer(s, authSvc)

	groupSvc := service.NewGroupService()
	pb.RegisterGroupServiceServer(s, groupSvc)

	log.Printf("Logic gRPC server starting on %s...", *grpcAddr)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve gRPC: %v", err)
		}
	}()

	log.Println("Logic service running. Press CTRL+C to exit.")
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
	<-sc

	log.Println("Shutting down Logic service...")

	if service.GlobalWriter != nil {
		service.GlobalWriter.Stop()
		log.Println("AsyncWriter flushed and stopped.")
	}

	// *** Etcd Registration ***
	register, err := discovery.NewServiceRegister([]string{*etcdAddr})
	if err != nil {
		log.Fatalf("Failed to create Etcd register: %v", err)
	}
	// Service Name: chat/logic, Addr: *grpcAddr (should be reachable IP in production)
	// For local docker, we might need an advertised address flag, but for now using *grpcAddr
	if err := register.Register(context.Background(), "chat/logic", *grpcAddr, 5); err != nil {
		log.Fatalf("Failed to register service to Etcd: %v", err)
	}
	defer register.Close()
	log.Printf("Service registered to Etcd: %s @ %s", "chat/logic", *grpcAddr)

	s.GracefulStop()
}
