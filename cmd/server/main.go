package main

import (
	pb "github.com/QuchengRep1/chaos-grpc/proto"
	"github.com/QuchengRep1/chaos-grpc/service"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"
	"log"
	"net"
)

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "192.168.100.100:6379",
		Password: "qucheng", // no password
		DB:       0,         // use default DB
	})

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterCommandExecutorServer(s, service.NewCommandExecutorServer(redisClient))

	log.Println("gRPC server started on port 50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
