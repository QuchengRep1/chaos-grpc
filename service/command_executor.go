package service

import (
	"context"
	pb "github.com/QuchengRep1/chaos-grpc/proto"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
)

type CommandExecutorServer struct {
	pb.UnimplementedCommandExecutorServer
	taskManager *TaskManager
}

func NewCommandExecutorServer(redisClient *redis.Client) *CommandExecutorServer {

	taskManager := NewTaskManager(redisClient)
	go taskManager.StartTaskRecoveryScheduler()

	return &CommandExecutorServer{
		taskManager: taskManager,
	}
}

func (s *CommandExecutorServer) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.CreateTaskResponse, error) {
	var taskID string
	//var err error

	switch spec := req.GetTaskSpec().(type) {
	case *pb.CreateTaskRequest_RedisSpec:
		taskID = s.taskManager.CreateRedisTask(req.Name, spec.RedisSpec)
	case *pb.CreateTaskRequest_KafkaProducerSpec:
		taskID = s.taskManager.CreateKafkaProducerTask(req.Name, spec.KafkaProducerSpec)
	case *pb.CreateTaskRequest_KafkaConsumerSpec:
		taskID = s.taskManager.CreateKafkaConsumerTask(req.Name, spec.KafkaConsumerSpec)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown task type")
	}

	if taskID == "" {
		return nil, status.Errorf(codes.Internal, "failed to create task")
	}

	return &pb.CreateTaskResponse{TaskId: taskID}, nil
}

func (s *CommandExecutorServer) StartTask(ctx context.Context, req *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	if err := s.taskManager.StartTask(req.TaskId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to start task: %v", err)
	}
	return &pb.StartTaskResponse{Status: "started"}, nil
}

func (s *CommandExecutorServer) StopTask(ctx context.Context, req *pb.StopTaskRequest) (*pb.StopTaskResponse, error) {
	if err := s.taskManager.StopTask(req.TaskId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to stop task: %v", err)
	}
	return &pb.StopTaskResponse{Status: "stopped"}, nil
}

func (s *CommandExecutorServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.GetTaskResponse, error) {
	task, err := s.taskManager.GetTask(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "task not found: %v", err)
	}
	return &pb.GetTaskResponse{Status: task.Status, Output: strings.Join(task.Output, "\n")}, nil
}

func (s *CommandExecutorServer) GetTaskOutput(ctx context.Context, req *pb.GetTaskOutputRequest) (*pb.GetTaskOutputResponse, error) {
	output, err := s.taskManager.GetTaskOutput(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "task not found: %v", err)
	}
	return &pb.GetTaskOutputResponse{Output: output}, nil
}

func (s *CommandExecutorServer) StartKafkaProducer(reqWithContent *pb.StartTaskRequest, stream pb.CommandExecutor_StartKafkaProducerServer) error {
	return s.taskManager.StreamKafkaProducerTask(reqWithContent.TaskId, stream)
}

func (s *CommandExecutorServer) StartKafkaConsumer(req *pb.StartTaskRequest, stream pb.CommandExecutor_StartKafkaConsumerServer) error {
	return s.taskManager.StreamKafkaConsumerTask(req.TaskId, stream)
}
