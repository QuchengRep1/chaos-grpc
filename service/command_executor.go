package service

import (
	"context"
	"github.com/QuchengRep1/chaos-grpc/proto"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
)

type CommandExecutorServer struct {
	grpc.UnimplementedCommandExecutorServer
	taskManager *TaskManager
}

func NewCommandExecutorServer(redisClient *redis.Client) *CommandExecutorServer {
	return &CommandExecutorServer{
		taskManager: NewTaskManager(redisClient),
	}
}

func (s *CommandExecutorServer) CreateTask(ctx context.Context, req *grpc.CreateTaskRequest) (*grpc.CreateTaskResponse, error) {
	taskID := s.taskManager.CreateTask(req.Name, req.Spec)
	return &grpc.CreateTaskResponse{TaskId: taskID}, nil
}

func (s *CommandExecutorServer) StartTask(ctx context.Context, req *grpc.StartTaskRequest) (*grpc.StartTaskResponse, error) {
	if err := s.taskManager.StartTask(req.TaskId); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to start task: %v", err)
	}
	return &grpc.StartTaskResponse{Status: "started"}, nil
}

func (s *CommandExecutorServer) StopTask(ctx context.Context, req *grpc.StopTaskRequest) (*grpc.StopTaskResponse, error) {
	if err := s.taskManager.StopTask(req.TaskId); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to stop task: %v", err)
	}
	return &grpc.StopTaskResponse{Status: "stopped"}, nil
}

func (s *CommandExecutorServer) GetTask(ctx context.Context, req *grpc.GetTaskRequest) (*grpc.GetTaskResponse, error) {
	task, err := s.taskManager.GetTask(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Task not found: %v", err)
	}
	return &grpc.GetTaskResponse{Status: task.Status, Output: strings.Join(task.Output, "\n")}, nil
}

func (s *CommandExecutorServer) GetTaskOutput(ctx context.Context, req *grpc.GetTaskOutputRequest) (*grpc.GetTaskOutputResponse, error) {
	output, err := s.taskManager.GetTaskOutput(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Task not found: %v", err)
	}
	return &grpc.GetTaskOutputResponse{Output: output}, nil
}
