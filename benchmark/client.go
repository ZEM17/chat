package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

var (
	addr     = flag.String("addr", "localhost:8080", "http service address")
	clients  = flag.Int("c", 100, "number of concurrent clients")
	duration = flag.Duration("d", 10*time.Second, "test duration")

	sendCount int64
	recvCount int64
	jwtKey    = []byte("my_secret_key")
)

type Claims struct {
	UserID   string `json:"user_id"`
	Nickname string `json:"nickname"`
	jwt.RegisteredClaims
}

func generateToken(userID string) string {
	expirationTime := time.Now().Add(24 * time.Hour)
	claims := &Claims{
		UserID:   userID,
		Nickname: "BenchUser",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			Issuer:    "chat_auth",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString(jwtKey)
	return tokenString
}

func main() {
	flag.Parse()

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
	log.Printf("Connecting to %s with %d clients for %s...", u.String(), *clients, *duration)

	var wg sync.WaitGroup
	start := time.Now()

	clientFunc := func(id int) {
		defer wg.Done()

		q := u.Query()
		token := generateToken(fmt.Sprintf("%d", id))
		q.Set("token", token)
		u.RawQuery = q.Encode()

		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			return
		}
		defer c.Close()

		done := make(chan struct{})

		// Reader
		go func() {
			defer close(done)
			for {
				_, _, err := c.ReadMessage()
				if err != nil {
					return
				}
				atomic.AddInt64(&recvCount, 1)
			}
		}()

		// Writer
		ticker := time.NewTicker(time.Second) // 1 msg/sec per client
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				msg := fmt.Sprintf(`{"to_user_id":"%d", "content":"hello"}`, id+1)
				err := c.WriteMessage(websocket.TextMessage, []byte(msg))
				if err != nil {
					return
				}
				atomic.AddInt64(&sendCount, 1)
			}
		}
	}

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go clientFunc(i + 1)
	}

	time.Sleep(*duration)
	log.Println("Stopping benchmark...")

	log.Printf("Benchmark finished.")
	log.Printf("Clients: %d", *clients)
	log.Printf("Duration: %s", time.Since(start))
	log.Printf("Sent: %d (%.2f/s)", sendCount, float64(sendCount)/time.Since(start).Seconds())
	log.Printf("Recv: %d (%.2f/s)", recvCount, float64(recvCount)/time.Since(start).Seconds())
}
