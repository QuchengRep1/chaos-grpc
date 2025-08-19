package service

import (
	"bufio"
	"context"
	"fmt"
	pb "github.com/QuchengRep1/chaos-grpc/proto"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"os/exec"
	"strings"
	"time"
)

type CommandExecutorServer struct {
	pb.UnimplementedCommandExecutorServer
	taskManager *TaskManager
}

func NewCommandExecutorServer(redisClient *redis.Client) *CommandExecutorServer {
	return &CommandExecutorServer{
		taskManager: NewTaskManager(redisClient),
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

func (s *CommandExecutorServer) StartKafkaProducer(req *pb.StartTaskRequest, stream pb.CommandExecutor_StartKafkaProducerServer) error {
	//return s.taskManager.StreamKafkaProducerTask(req.TaskId, stream)

	args := []string{
		"-h", "192.168.100.100",
		"-p", "6379",
		"-a", "qucheng",
		"-n", "100000",
		"-c", "20",
	}

	cmd := exec.Command("redis-benchmark", args...)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}
	// 3. 流式传输
	go func() {
		defer cmd.Wait()

		scanner := bufio.NewScanner(io.MultiReader(stdoutPipe, stderrPipe))
		scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
			if atEOF {
				if len(data) > 0 {
					return len(data), data, nil
				}
				return 0, nil, nil
			}
			for i := 0; i < len(data); i++ {
				if data[i] == '\n' || data[i] == '\r' {
					return i + 1, data[:i+1], nil
				}
			}
			return 0, nil, nil
		})

		for scanner.Scan() {
			line := scanner.Text()
			timestamp := time.Now().Format(time.RFC3339)
			logLine := fmt.Sprintf("[%s] %s", timestamp, line)

			if err := stream.Send(&pb.StreamOutputResponse{
				Line: logLine,
			}); err != nil {
				log.Printf("Stream send failed: %v", err)
				cmd.Process.Kill()
				return
			}
		}
	}()

	return nil

}

func (s *CommandExecutorServer) StartKafkaConsumer(req *pb.StartTaskRequest, stream pb.CommandExecutor_StartKafkaConsumerServer) error {
	return s.taskManager.StreamKafkaConsumerTask(req.TaskId, stream)
}
