package service

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strconv"
	//"strings"
	"sync"
	"time"

	pb "github.com/QuchengRep1/chaos-grpc/internal/proto"
	"github.com/go-redis/redis/v8"
	//"google.golang.org/grpc/codes"
	//"google.golang.org/grpc/status"
)

type Task struct {
	ID      string
	Command string
	Args    []string
	Status  string
	Output  []string
	Process *exec.Cmd
	mu      sync.Mutex
}

type TaskManager struct {
	tasks sync.Map
	redis *redis.Client
}

func NewTaskManager(redisClient *redis.Client) *TaskManager {
	return &TaskManager{
		redis: redisClient,
	}
}

func (tm *TaskManager) CreateTask(name string, spec *pb.RedisBenchmarkSpec) string {
	taskID := "task-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	args := []string{
		"-h", spec.Hostname,
		"-p", spec.Port,
	}
	if spec.Password != "" {
		args = append(args, "-a", spec.Password)
	}
	args = append(args,
		"-n", spec.Requests,
		"-c", spec.Clients,
	)
	if spec.Size != "" {
		args = append(args, "-s", spec.Size)
	}
	if spec.Loop != "" {
		args = append(args, "-l", spec.Loop)
	}
	if spec.LoopDuration != "" {
		args = append(args, "-d", spec.LoopDuration)
	}

	task := &Task{
		ID:      taskID,
		Command: "redis-benchmark",
		Args:    args,
		Status:  "created",
	}
	tm.tasks.Store(taskID, task)
	return taskID
}

func (tm *TaskManager) StartTask(taskID string) error {
	val, ok := tm.tasks.Load(taskID)
	if !ok {
		return fmt.Errorf("task not found")
	}
	task := val.(*Task)

	task.mu.Lock()
	defer task.mu.Unlock()

	if task.Status != "created" {
		return fmt.Errorf("task already started or stopped")
	}

	cmd := exec.Command(task.Command, task.Args...)
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

	task.Process = cmd
	task.Status = "running"

	scanner := bufio.NewScanner(io.MultiReader(stdoutPipe, stderrPipe))
	for scanner.Scan() {
		line := scanner.Text()
		task.Output = append(task.Output, line)
		_, err := tm.redis.RPush(context.Background(), "task-output:"+taskID, line).Result()
		if err != nil {
			log.Printf("Failed to push output to Redis: %v", err)
		}
	}

	if err := cmd.Wait(); err != nil {
		task.Status = "failed"
	} else {
		task.Status = "finished"
	}

	return nil
}

func (tm *TaskManager) StopTask(taskID string) error {
	val, ok := tm.tasks.Load(taskID)
	if !ok {
		return fmt.Errorf("task not found")
	}
	task := val.(*Task)

	task.mu.Lock()
	defer task.mu.Unlock()

	if task.Process != nil && task.Process.Process != nil {
		if err := task.Process.Process.Kill(); err != nil {
			return err
		}
		task.Status = "stopped"
	}

	return nil
}

func (tm *TaskManager) GetTask(taskID string) (*Task, error) {
	val, ok := tm.tasks.Load(taskID)
	if !ok {
		return nil, fmt.Errorf("task not found")
	}
	return val.(*Task), nil
}

func (tm *TaskManager) GetTaskOutput(taskID string) ([]string, error) {
	val, ok := tm.tasks.Load(taskID)
	if !ok {
		return nil, fmt.Errorf("task not found")
	}
	task := val.(*Task)
	return task.Output, nil
}
