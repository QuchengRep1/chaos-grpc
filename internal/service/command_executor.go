package service

import (
	"context"
	proto "github.com/QuchengRep1/chaos-grpc/internal/proto" // 替换为你的 proto 文件生成的包路径
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
)

type CommandExecutorServer struct {
	proto.UnimplementedCommandExecutorServer
	taskManager *TaskManager
}

func NewCommandExecutorServer(redisClient *redis.Client) *CommandExecutorServer {
	return &CommandExecutorServer{
		taskManager: NewTaskManager(redisClient),
	}
}

func (s *CommandExecutorServer) CreateTask(ctx context.Context, req *proto.CreateTaskRequest) (*proto.CreateTaskResponse, error) {
	taskID := s.taskManager.CreateTask(req.Name, req.Spec)
	return &proto.CreateTaskResponse{TaskId: taskID}, nil
}

func (s *CommandExecutorServer) StartTask(ctx context.Context, req *proto.StartTaskRequest) (*proto.StartTaskResponse, error) {
	if err := s.taskManager.StartTask(req.TaskId); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to start task: %v", err)
	}
	return &proto.StartTaskResponse{Status: "started"}, nil
}

func (s *CommandExecutorServer) StopTask(ctx context.Context, req *proto.StopTaskRequest) (*proto.StopTaskResponse, error) {
	if err := s.taskManager.StopTask(req.TaskId); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to stop task: %v", err)
	}
	return &proto.StopTaskResponse{Status: "stopped"}, nil
}

func (s *CommandExecutorServer) GetTask(ctx context.Context, req *proto.GetTaskRequest) (*proto.GetTaskResponse, error) {
	task, err := s.taskManager.GetTask(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Task not found: %v", err)
	}
	return &proto.GetTaskResponse{Status: task.Status, Output: strings.Join(task.Output, "\n")}, nil
}

func (s *CommandExecutorServer) GetTaskOutput(ctx context.Context, req *proto.GetTaskOutputRequest) (*proto.GetTaskOutputResponse, error) {
	output, err := s.taskManager.GetTaskOutput(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Task not found: %v", err)
	}
	return &proto.GetTaskOutputResponse{Output: output}, nil
}
