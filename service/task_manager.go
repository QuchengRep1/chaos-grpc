package service

import (
	"bufio"
	"context"
	"encoding/json"

	//"encoding/json"
	"fmt"
	pb "github.com/QuchengRep1/chaos-grpc/proto"
	"github.com/go-redis/redis/v8"
	"io"
	"log"
	"os/exec"
	"sync"
	"time"
)

const (
	taskInstanceKeyPrefix = "chaos-task:instance:"
	taskOutputKeyPrefix   = "chaos-task:output:"
	taskStatusKey         = "chaos-task:status"
)

type Task struct {
	ID      string
	Type    string // "redis", "kafka-producer", "kafka-consumer"
	Command string
	Args    []string
	Status  string
	Output  []string
	Process *exec.Cmd
	mu      sync.Mutex
}

type KubeObjectMetadata struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type RedisBenchmarkSpec struct {
	Action       string `json:"action"`
	Hostname     string `json:"hostname"`
	Port         string `json:"port"`
	Password     string `json:"password,omitempty"`
	Requests     string `json:"requests"`
	Clients      string `json:"clients"`
	Size         string `json:"size,omitempty"`
	Loop         string `json:"loop,omitempty"`
	LoopDuration string `json:"loopduration,omitempty"`
}

type KubeObject struct {
	Kind       string             `json:"kind"`
	APIVersion string             `json:"apiVersion"`
	Metadata   KubeObjectMetadata `json:"metadata"`
	Spec       RedisBenchmarkSpec `json:"spec"`
}

type TaskInstance struct {
	Namespace  string     `json:"namespace"`
	Name       string     `json:"name"`
	Kind       string     `json:"kind"`
	CreatedAt  time.Time  `json:"created_at"`
	Status     string     `json:"status"` // paused, running, finished, failed
	TaskID     string     `json:"task_id"`
	LiveTime   int64      `json:"live_time"` // 累计运行时间(秒)
	KubeObject KubeObject `json:"kube_object"`
	StartTime  time.Time  `json:"start_time,omitempty"`
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

func (tm *TaskManager) CreateRedisTask(name string, spec *pb.RedisBenchmarkSpec) string {
	taskID := generateTaskID()
	args := buildRedisBenchmarkArgs(spec)

	task := &Task{
		ID:      taskID,
		Type:    "redis",
		Command: "redis-benchmark",
		Args:    args,
		Status:  "created",
	}

	instance := &TaskInstance{
		Namespace: "default",
		Name:      name,
		Kind:      "InvokeChaos",
		CreatedAt: time.Now(),
		Status:    "paused",
		TaskID:    taskID,
		LiveTime:  0,
		KubeObject: KubeObject{
			Kind:       "RedisBenchmark",
			APIVersion: "chaos-mesh.org/v1alpha1",
			Metadata: KubeObjectMetadata{
				Namespace: "default",
				Name:      name,
			},
			Spec: RedisBenchmarkSpec{
				Action:       "",
				Hostname:     spec.Hostname,
				Port:         spec.Port,
				Password:     spec.Password,
				Requests:     spec.Requests,
				Clients:      spec.Clients,
				Size:         spec.Size,
				Loop:         spec.Loop,
				LoopDuration: spec.LoopDuration,
			},
		},
	}

	if err := tm.saveInstanceToRedis(instance); err != nil {
		return "CreateTask Error"
	}

	tm.tasks.Store(taskID, task)
	return taskID

}

func (tm *TaskManager) CreateKafkaProducerTask(name string, spec *pb.KafkaProducerSpec) string {
	taskID := generateTaskID()
	args := buildKafkaProducerArgs(spec)

	task := &Task{
		ID:      taskID,
		Type:    "kafka-producer",
		Command: "/opt/kafkains/k1/bin/kafka-producer-perf-test.sh",
		Args:    args,
		Status:  "created",
	}

	tm.tasks.Store(taskID, task)
	return taskID
}

func (tm *TaskManager) CreateKafkaConsumerTask(name string, spec *pb.KafkaConsumerSpec) string {
	taskID := generateTaskID()
	args := buildKafkaConsumerArgs(spec)

	task := &Task{
		ID:      taskID,
		Type:    "kafka-consumer",
		Command: "/opt/kafkains/k1/bin/kafka-consumer-perf-test.sh",
		Args:    args,
		Status:  "created",
	}

	tm.tasks.Store(taskID, task)
	return taskID
}

func buildRedisBenchmarkArgs(spec *pb.RedisBenchmarkSpec) []string {
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
	return args
}

func buildKafkaProducerArgs(spec *pb.KafkaProducerSpec) []string {
	args := []string{
		"--topic", spec.Topic,
		"--num-records", spec.NumRecords,
		"--record-size", spec.RecordSize,
		"--producer-props",
		fmt.Sprintf("bootstrap.servers=%s acks=%s compression.type=%s",
			spec.BootstrapServers, spec.Acks, spec.CompressionType),
	}
	return args
}

func buildKafkaConsumerArgs(spec *pb.KafkaConsumerSpec) []string {
	args := []string{
		"--topic", spec.Topic,
		"--messages", spec.Messages,
		"--bootstrap-server", spec.BootstrapServers,
	}
	return args
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

	go func() {
		scanner := bufio.NewScanner(io.MultiReader(stdoutPipe, stderrPipe))
		for scanner.Scan() {
			line := scanner.Text()
			task.Output = append(task.Output, line)
			_, err := tm.redis.RPush(context.Background(), "task-output:"+taskID, line).Result()
			if err != nil {
				log.Printf("failed to push output to Redis: %v", err)
			}
		}

		if err := cmd.Wait(); err != nil {
			task.Status = "failed"
		} else {
			task.Status = "finished"
		}
	}()

	return nil
}

func (tm *TaskManager) StreamKafkaTask(taskID string, stream interface{}) error {
	val, _ := tm.tasks.Load(taskID)
	//if !ok {
	//	return fmt.Errorf("task not found")
	//}
	task := val.(*Task)

	task.mu.Lock()
	defer task.mu.Unlock()

	if task.Status != "created" {
		return fmt.Errorf("task already started or stopped")
	}

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

	task.Process = cmd
	task.Status = "running"

	go func() {
		scanner := bufio.NewScanner(io.MultiReader(stdoutPipe, stderrPipe))
		for scanner.Scan() {
			line := scanner.Text()
			task.Output = append(task.Output, line)

			// 根据流类型发送响应
			switch s := stream.(type) {
			case pb.CommandExecutor_StartKafkaProducerServer:
				s.Send(&pb.StreamOutputResponse{Line: line})
			case pb.CommandExecutor_StartKafkaConsumerServer:
				s.Send(&pb.StreamOutputResponse{Line: line})
			}
		}

		if err := cmd.Wait(); err != nil {
			task.Status = "failed"
		} else {
			task.Status = "finished"
		}
	}()

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

func generateTaskID() string {
	return fmt.Sprintf("chaos-task-%d", time.Now().UnixNano())
}

func (tm *TaskManager) saveInstanceToRedis(instance *TaskInstance) error {
	ctx := context.Background()
	instanceKey := taskInstanceKeyPrefix + instance.TaskID

	data, err := json.Marshal(instance)
	if err != nil {
		return err
	}

	_, err = tm.redis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, instanceKey, "json", string(data))
		pipe.SAdd(ctx, taskStatusKey, instance.TaskID)
		return nil
	})
	return err
}
