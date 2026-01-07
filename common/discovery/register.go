package discovery

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ServiceRegister 创建租约注册服务
type ServiceRegister struct {
	cli     *clientv3.Client
	leaseID clientv3.LeaseID
	// channel to keep alive
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key           string
}

// NewServiceRegister 新建注册服务
func NewServiceRegister(endpoints []string) (*ServiceRegister, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &ServiceRegister{
		cli: cli,
	}, nil
}

// Register 注册服务
func (s *ServiceRegister) Register(ctx context.Context, serviceName, addr string, ttl int64) error {
	// 创建租约
	grantResp, err := s.cli.Grant(ctx, ttl)
	if err != nil {
		return err
	}

	s.leaseID = grantResp.ID

	// 注册服务 kv，带租约
	s.key = "/" + serviceName + "/" + addr
	_, err = s.cli.Put(ctx, s.key, addr, clientv3.WithLease(s.leaseID))
	if err != nil {
		return err
	}

	// 自动续约
	keepAliveChan, err := s.cli.KeepAlive(ctx, s.leaseID)
	if err != nil {
		return err
	}
	s.keepAliveChan = keepAliveChan

	log.Printf("Service Registered: %s", s.key)

	// 处理续约应答，如果 channel 关闭则说明租约失效
	go func() {
		for {
			_, ok := <-s.keepAliveChan
			if !ok {
				log.Println("Etcd keep alive channel closed, lease revoked?")
				return
			}
		}
	}()

	return nil
}

// Close 注销服务
func (s *ServiceRegister) Close() error {
	// 撤销租约
	if s.leaseID > 0 {
		_, err := s.cli.Revoke(context.Background(), s.leaseID)
		if err != nil {
			log.Printf("Etcd revoke lease failed: %v", err)
		}
	}
	log.Printf("Service Unregistered: %s", s.key)
	return s.cli.Close()
}
