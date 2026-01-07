package discovery

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"
)

// EtcdResolver 自定义 resolver
type EtcdResolver struct {
	etcdAddr []string
}

func NewEtcdResolver(etcdAddr []string) resolver.Builder {
	return &EtcdResolver{etcdAddr: etcdAddr}
}

func (r *EtcdResolver) Scheme() string {
	return "etcd"
}

func (r *EtcdResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   r.etcdAddr,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	// target.Endpoint() 是没有 scheme 后的部分，例如 etcd:///chat/logic -> /chat/logic
	// Build 时 target.URL.Path 是 key 前缀
	// 注意：新版 grpc resolver.Target 结构有点变化，通常 target.URL.Path 或者 target.Endpoint()
	// 这里假设使用 etcd:///service_name 格式
	prefix := "/" + target.URL.Path
	if prefix == "/" {
		// handle legacy or different parsing
		prefix = "/" + target.Endpoint()
	}

	// 修正: 如果是 etcd:///chat/logic，Endpoint可能是 chat/logic，我们要确保前缀是 /chat/logic
	// 简单处理：我们约定服务名 key 也是 / 开头

	log.Printf("EtcdResolver watching prefix: %s", prefix)

	// 先获取当前所有地址
	resp, err := cli.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err == nil {
		var addrStrs []string
		for _, kv := range resp.Kvs {
			addrStrs = append(addrStrs, string(kv.Value))
		}
		cc.UpdateState(resolver.State{Addresses: convertToAddresses(addrStrs)})
	}

	// 监听变化
	go r.watcher(cli, cc, prefix)

	return &etcdResolver{cli: cli}, nil
}

func (r *EtcdResolver) watcher(cli *clientv3.Client, cc resolver.ClientConn, prefix string) {
	rch := cli.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				log.Printf("Etcd update: %s : %s", string(ev.Kv.Key), string(ev.Kv.Value))
			case clientv3.EventTypeDelete:
				log.Printf("Etcd delete: %s", string(ev.Kv.Key))
			}
		}
		// 简单起见，每次变动都重新全量获取一次 (Real implementations maintain local cache)
		// 这里为了演示方便，生产环境可以用更高效的增量更新
		resp, err := cli.Get(context.Background(), prefix, clientv3.WithPrefix())
		if err == nil {
			var addrStrs []string
			for _, kv := range resp.Kvs {
				addrStrs = append(addrStrs, string(kv.Value))
			}
			cc.UpdateState(resolver.State{Addresses: convertToAddresses(addrStrs)})
		}
	}
}

func convertToAddresses(addrStrs []string) []resolver.Address {
	addrs := make([]resolver.Address, 0, len(addrStrs))
	for _, a := range addrStrs {
		addrs = append(addrs, resolver.Address{Addr: a})
	}
	return addrs
}

type etcdResolver struct {
	cli *clientv3.Client
}

func (r *etcdResolver) ResolveNow(rn resolver.ResolveNowOptions) {
}

func (r *etcdResolver) Close() {
	r.cli.Close()
}
