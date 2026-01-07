package main

import (
	"chat/common/pb"
	"chat/logic/repo"
	"chat/logic/service"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"
	"google.golang.org/grpc"
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

	_, err := repo.SaveMessage(msg.FromUserID, msg.ToUserID, msg.ToGroupID, msg.Payload, msg.Type)
	if err != nil {
		if errors.Is(err, repo.ErrUserNotFound) {
			log.Printf("Failed to save message: %v. Discarding message.", err)
			var errorMsg string
			if strings.Contains(err.Error(), "recipient user") {
				errorMsg = fmt.Sprintf("发送失败: 对方用户 (ID: %s) 不存在。", msg.ToUserID)
			} else if strings.Contains(err.Error(), "sender user") {
				errorMsg = "发送失败: 您的账户似乎无效。"
			} else {
				errorMsg = "发送失败: 目标用户不存在。"
			}

			systemPayload := map[string]string{
				"type":           "system_error",
				"content":        errorMsg,
				"ref_to_user_id": msg.ToUserID,
			}
			systemPayloadBytes, jsonErr := json.Marshal(systemPayload)
			if jsonErr != nil {
				log.Printf("Failed to marshal system error message: %v", jsonErr)
				d.Ack(true)
				return
			}

			locationKey := "user:" + msg.FromUserID + ":location"
			gatewayID, redisErr := redisClient.Get(ctx, locationKey).Result()

			if redisErr == redis.Nil {
				log.Printf("Sender %s is offline, cannot deliver system error.", msg.FromUserID)
			} else if redisErr != nil {
				log.Printf("Failed to get sender %s location from Redis: %v", msg.FromUserID, redisErr)
			} else {
				downMsg := DownstreamMessage{
					ToUserID: msg.FromUserID,
					Payload:  systemPayloadBytes,
				}
				msgBody, err := json.Marshal(downMsg)
				if err != nil {
					log.Printf("Failed to marshal downstream system error: %v", err)
				} else {
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
						log.Printf("Failed to publish system error back to sender %s: %v", msg.FromUserID, err)
					} else {
						log.Printf("Published system error back to sender %s (Gateway: %s)", msg.FromUserID, gatewayID)
					}
				}
			}
			d.Ack(true)
		} else {
			log.Printf("Failed to save message to DB (poison message?). Discarding: %v. Body: %s", err, string(d.Body))
			d.Ack(true)
		}
		return
	}

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

	// *** gRPC Server ***
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	// Register AuthService
	authSvc := service.NewAuthService()
	pb.RegisterAuthServiceServer(s, authSvc)

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
	s.GracefulStop()
}
