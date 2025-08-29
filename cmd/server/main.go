package main

import (
	"context"
	"fmt"
	config "github.com/QuchengRep1/chaos-grpc/config"
	pb "github.com/QuchengRep1/chaos-grpc/proto"
	"github.com/QuchengRep1/chaos-grpc/service"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"

	"gopkg.in/yaml.v3"

	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

func main() {

	sc := []constant.ServerConfig{
		{
			IpAddr: "192.168.100.100", // Nacos 服务器地址
			Port:   38848,             // Nacos 端口
		},
	}

	cc := constant.ClientConfig{
		NamespaceId:         "public",
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		Username:            "nacos", // 添加用户名（默认nacos）
		Password:            "nacos", // 添加密码（默认nacos）
	}

	configClient, err := clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		})
	if err != nil {
		panic(err)
	}

	chaosInitConf, err := configClient.GetConfig(vo.ConfigParam{
		DataId: "chaos-init-config.yml",
		Group:  "DEFAULT_GROUP",
	})
	if err != nil || chaosInitConf == "" {
		panic("get config failed: " + err.Error())
	}
	fmt.Printf("get config to Nacos: %s:%d\n", chaosInitConf)

	chaosInit, err := parseChaosInitConfig(chaosInitConf)
	if err != nil {
		panic("parse config failed: " + err.Error())
	}
	config.SetConfig(chaosInit)

	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", chaosInit.Chaos.Redis.Address, chaosInit.Chaos.Redis.Port),
		Password: chaosInit.Chaos.Redis.Password,
		DB:       chaosInit.Chaos.Redis.Database,
	})

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	//s := grpc.NewServer()
	s := grpc.NewServer(
		grpc.StreamInterceptor(debugStreamInterceptor),
	)

	pb.RegisterCommandExecutorServer(s, service.NewCommandExecutorServer(redisClient))

	// 启用反射（关键添加）
	reflection.Register(s)
	registerToNacos(50051)

	log.Println("gRPC server started on port 50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func debugStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	log.Printf("Stream call: %v", info.FullMethod)
	return handler(srv, ss)
}

func logClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	log.Printf("Client call: %s, req: %v", method, req)
	err := invoker(ctx, method, req, reply, cc, opts...)
	log.Printf("Client response: %v, err: %v", reply, err)
	return err
}

func registerToNacos(grpcPort int) {
	sc := []constant.ServerConfig{
		{
			IpAddr: "192.168.100.100", // Nacos 服务器地址
			Port:   38848,             // Nacos 端口
		},
	}

	cc := constant.ClientConfig{
		NamespaceId:         "public",
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		Username:            "nacos", // 添加用户名（默认nacos）
		Password:            "nacos", // 添加密码（默认nacos）
	}

	// 2. 创建 Nacos 客户端
	namingClient, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		},
	)

	if err != nil {
		panic(err)
	}

	// 3. 注册 gRPC 服务
	serviceName := "chaos-grpc"
	ip := getLocalIP() // 获取本机 IP
	success, err := namingClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        uint64(grpcPort),
		ServiceName: serviceName,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true, // 临时实例
		Metadata:    map[string]string{"version": "1.0"},
	})
	if err != nil || !success {
		panic("Register failed: " + err.Error())
	}
	fmt.Printf("Registered to Nacos: %s:%d\n", ip, grpcPort)

	//nsData, err2 := configClient.GetConfig(vo.ConfigParam{
	//	DataId: "application-dev.yml",
	//	Group:  "DEFAULT_GROUP",
	//})
	//if err2 != nil || nsData == "" {
	//	panic("get config failed: " + err.Error())
	//}
	//fmt.Printf("get config to Nacos: %s:%d\n", nsData)
	//
	//// 解析配置
	//config, err := parseConfig(nsData)
	//if err != nil {
	//	log.Printf("Failed to parse config: %v", err)
	//} else {
	//	// 获取特定字段
	//	fmt.Printf("Feign Sentinel enabled: %v\n", config.Feign.Sentinel.Enabled)
	//	fmt.Printf("Connect timeout: %d\n", config.Feign.Client.Config.Default.ConnectTimeout)
	//	fmt.Printf("Management include: %s\n", config.Management.Endpoints.Web.Exposure.Include)
	//}

}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "127.0.0.1"
}

func parseChaosInitConfig(configStr string) (*config.ChaosInitConfig, error) {
	var parseConfig config.ChaosInitConfig
	err := yaml.Unmarshal([]byte(configStr), &parseConfig)
	if err != nil {
		return nil, err
	}
	return &parseConfig, nil
}
