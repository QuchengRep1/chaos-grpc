package service

import (
	"bufio"
	"context"
	"encoding/json"
	config "github.com/QuchengRep1/chaos-grpc/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	taskStatusKey         = "chaos-task:grpc-kafka:status"
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

type KafkaProducerInstance struct {
	Namespace  string                  `json:"namespace"`
	Name       string                  `json:"name"`
	Kind       string                  `json:"kind"`
	CreatedAt  time.Time               `json:"created_at"`
	Status     string                  `json:"status"` // paused, running, finished, failed
	TaskID     string                  `json:"task_id"`
	LiveTime   int64                   `json:"live_time"` // 累计运行时间(秒)
	KubeObject KafkaProducerKubeObject `json:"kube_object"`
	StartTime  time.Time               `json:"start_time,omitempty"`
}

type KafkaConsumerInstance struct {
	Namespace  string                  `json:"namespace"`
	Name       string                  `json:"name"`
	Kind       string                  `json:"kind"`
	CreatedAt  time.Time               `json:"created_at"`
	Status     string                  `json:"status"` // paused, running, finished, failed
	TaskID     string                  `json:"task_id"`
	LiveTime   int64                   `json:"live_time"` // 累计运行时间(秒)
	KubeObject KafkaConsumerKubeObject `json:"kube_object"`
	StartTime  time.Time               `json:"start_time,omitempty"`
}

type KafkaConsumerKubeObject struct {
	Kind       string                `json:"kind"`
	APIVersion string                `json:"apiVersion"`
	Metadata   KubeObjectMetadata    `json:"metadata"`
	Spec       KafkaConsumerPerfSpec `json:"spec"`
}

type KafkaConsumerPerfSpec struct {
	Action       string `json:"action"`
	Hostname     string `json:"hostname"`
	Port         string `json:"port"`
	Username     string `json:"username,omitempty"`
	Password     string `json:"password,omitempty"`
	Topic        string `json:"topic"`
	Counts       string `json:"counts"`
	Size         string `json:"size,omitempty"`
	ConsumerGP   string `json:"group,omitempty"`
	Loop         string `json:"loop,omitempty"`
	LoopDuration string `json:"loopduration,omitempty"`
}

type KafkaProducerKubeObject struct {
	Kind       string                `json:"kind"`
	APIVersion string                `json:"apiVersion"`
	Metadata   KubeObjectMetadata    `json:"metadata"`
	Spec       KafkaProducerPerfSpec `json:"spec"`
}

type KafkaProducerPerfSpec struct {
	Action       string `json:"action"`
	Hostname     string `json:"hostname"`
	Port         string `json:"port"`
	Username     string `json:"username,omitempty"`
	Password     string `json:"password,omitempty"`
	Topic        string `json:"topic,omitempty"`
	Records      string `json:"records"`
	Size         string `json:"size,omitempty"`
	Throughput   string `json:"throughput,omitempty"`
	Acks         string `json:"acks,omitempty"`
	Compression  string `json:"compression,omitempty"`
	Loop         string `json:"loop,omitempty"`
	LoopDuration string `json:"loopduration,omitempty"`
	Recycle      string `json:"recycle,omitempty"`
}

type CommonInstance struct {
	Namespace  string           `json:"namespace"`
	Name       string           `json:"name"`
	Kind       string           `json:"kind"`
	CreatedAt  time.Time        `json:"created_at"`
	Status     string           `json:"status"` // paused, running, finished, failed
	TaskID     string           `json:"task_id"`
	LiveTime   int64            `json:"live_time"` // 累计运行时间(秒)
	KubeObject CommonKubeObject `json:"kube_object"`
	StartTime  time.Time        `json:"start_time,omitempty"`
}

type CommonKubeObject struct {
	Kind       string             `json:"kind"`
	APIVersion string             `json:"apiVersion"`
	Metadata   KubeObjectMetadata `json:"metadata"`
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
	tasks            sync.Map
	redis            *redis.Client
	recoveryInterval time.Duration
}

func NewTaskManager(redisClient *redis.Client) *TaskManager {

	cfg := config.GetConfig()
	interval := cfg.Chaos.Kafka.KafkaTaskScheduler.RecoveryInterval

	return &TaskManager{
		redis:            redisClient,
		recoveryInterval: interval, // 默认1分钟
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
	//先从redis中查找，是否有，再从本地缓存的tasks中查找是否存在

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

// StreamKafkaProducerTask 原版本流式代码
// StreamKafkaProducerTask 原版本流式代码
// StreamKafkaProducerTask 原版本流式代码
//func (tm *TaskManager) StreamKafkaProducerTask(taskID string, stream pb.CommandExecutor_StartKafkaProducerServer) error {
//	kafkaArgs := []string{
//		"--topic", "perf",
//		"--num-records", "10000",
//		"--record-size", "1024",
//		"--throughput", "-1",
//		"--producer-props", "bootstrap.servers=192.168.100.100:9092",
//		"acks=0", "compression.type=snappy",
//	}
//
//	cmd := exec.Command("/opt/kafkains/k1/bin/kafka-producer-perf-test.sh", kafkaArgs...)
//
//	stdoutPipe, err := cmd.StdoutPipe()
//	if err != nil {
//		return err
//	}
//	stderrPipe, err := cmd.StderrPipe()
//	if err != nil {
//		return err
//	}
//
//	if err := cmd.Start(); err != nil {
//		return err
//	}
//	done := make(chan error) // 用于通知主 goroutine 子 goroutine 已完成
//
//	go func() {
//
//		defer func() {
//			if err := cmd.Wait(); err != nil {
//				log.Printf("Command exited: %v", err)
//			}
//			close(done) // 发送完成信号
//		}()
//
//		scanner := bufio.NewScanner(io.MultiReader(stdoutPipe, stderrPipe))
//		scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
//			if atEOF {
//				if len(data) > 0 {
//					return len(data), data, nil
//				}
//				return 0, nil, nil
//			}
//			for i := 0; i < len(data); i++ {
//				if data[i] == '\n' || data[i] == '\r' {
//					return i + 1, data[:i+1], nil
//				}
//			}
//			return 0, nil, nil
//		})
//
//		for scanner.Scan() {
//			line := scanner.Text()
//			timestamp := time.Now().Format(time.RFC3339)
//			logLine := fmt.Sprintf("[%s] %s", timestamp, line)
//			if err := stream.Send(&pb.StreamOutputResponse{
//				Line: logLine,
//			}); err != nil {
//				log.Printf("Stream send failed: %v", err)
//				cmd.Process.Kill()
//				return
//			}
//		}
//	}()
//	// 主 goroutine 阻塞，直到子 goroutine 完成或客户端断开
//	select {
//	case <-stream.Context().Done(): // 客户端断开
//		cmd.Process.Kill() // 确保命令终止
//		return stream.Context().Err()
//	case <-done: // 子 goroutine 正常结束
//		return nil
//	}
//}

// StreamKafkaProducerTask 新版本流式代码
func (tm *TaskManager) StreamKafkaProducerTask(taskIDWithContent string, stream pb.CommandExecutor_StartKafkaProducerServer) error {
	var instanceKP KafkaProducerInstance
	err := json.Unmarshal([]byte(taskIDWithContent), &instanceKP)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Failed to parse task data: %v", err)
	}

	var task *Task
	val, ok := tm.tasks.Load(instanceKP.TaskID)
	if !ok {
		task = &Task{
			ID:      instanceKP.TaskID,
			Type:    "kafka",
			Command: "kafka-producer-perf",
			Status:  "created",
		}
		tm.tasks.Store(instanceKP.TaskID, task)
	} else {
		task = val.(*Task)
		task.mu.Lock()
		// 检查任务是否已经在运行中
		if task.Process != nil && task.Process.Process != nil {
			// 检查进程是否真的在运行
			if task.Process.ProcessState == nil || !task.Process.ProcessState.Exited() {
				task.mu.Unlock()
				// 任务已经在运行，直接返回成功
				return stream.Send(&pb.StreamOutputResponse{
					Line: "Task is already running",
				})
			}
		}
		task.mu.Unlock()
	}

	task.mu.Lock()
	defer task.mu.Unlock()

	kafkaArgs := []string{}
	// 必需参数
	if instanceKP.KubeObject.Spec.Topic != "" {
		kafkaArgs = append(kafkaArgs, "--topic", instanceKP.KubeObject.Spec.Topic)
	}
	if instanceKP.KubeObject.Spec.Records != "" {
		kafkaArgs = append(kafkaArgs, "--num-records", instanceKP.KubeObject.Spec.Records)
		//kafkaArgs = append(kafkaArgs, "--num-records", "500000")
	}

	// 可选参数
	if instanceKP.KubeObject.Spec.Size != "" {
		kafkaArgs = append(kafkaArgs, "--record-size", instanceKP.KubeObject.Spec.Size)
	}
	if instanceKP.KubeObject.Spec.Throughput != "" {
		kafkaArgs = append(kafkaArgs, "--throughput", instanceKP.KubeObject.Spec.Throughput)
	}

	if instanceKP.KubeObject.Spec.Hostname != "" && instanceKP.KubeObject.Spec.Port != "" {
		kafkaArgs = append(kafkaArgs, "--producer-props", fmt.Sprintf("bootstrap.servers=%s:%s", instanceKP.KubeObject.Spec.Hostname, instanceKP.KubeObject.Spec.Port))
	}

	if instanceKP.KubeObject.Spec.Username != "" && instanceKP.KubeObject.Spec.Password != "" {
		kafkaArgs = append(kafkaArgs, "sasl.mechanism=PLAIN", "security.protocol=SASL_PLAINTEXT", fmt.Sprintf("sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", instanceKP.KubeObject.Spec.Username, instanceKP.KubeObject.Spec.Password))
	}

	if instanceKP.KubeObject.Spec.Acks != "" {
		kafkaArgs = append(kafkaArgs, fmt.Sprintf(" acks=%s", instanceKP.KubeObject.Spec.Acks))
	}

	if instanceKP.KubeObject.Spec.Compression != "" {
		kafkaArgs = append(kafkaArgs, fmt.Sprintf(" compression.type=%s", instanceKP.KubeObject.Spec.Compression))
	}

	cfg := config.GetConfig()
	kafkaScriptPath := cfg.Chaos.Kafka.KafkaProducerPerf.Dir

	cmd := exec.Command(kafkaScriptPath, kafkaArgs...)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		task.Status = "paused"
		tm.updateKPRedisStatus(instanceKP, "paused")
		return status.Errorf(codes.Internal, "Failed to get stdout pipe: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		task.Status = "paused"
		tm.updateKPRedisStatus(instanceKP, "paused")
		return status.Errorf(codes.Internal, "Failed to get stderr pipe: %v", err)
	}

	// 启动命令
	if err := cmd.Start(); err != nil {
		errorMsg := fmt.Sprintf("Failed to start command: %v", err)
		tm.handleTaskError(instanceKP.TaskID, errorMsg, err)
		return status.Errorf(codes.Internal, errorMsg)
	}

	// 启动成功，更新状态
	task.Process = cmd
	task.Status = "running"
	tm.updateKPRedisStatus(instanceKP, "running")

	// 立即返回成功响应
	if err := stream.Send(&pb.StreamOutputResponse{
		Line: "Task started successfully",
	}); err != nil {
		log.Printf("Failed to send initial response: %v", err)
	}

	//if  instanceKP.KubeObject.Spec.Recycle == "true" {
	//
	//}
	// 异步处理输出（分开处理 stdout 和 stderr）
	go tm.monitorCommandWithSeparatePipesKP(cmd, stdoutPipe, stderrPipe, instanceKP.TaskID, instanceKP)

	return nil

}

func (tm *TaskManager) StreamKafkaConsumerTask(taskIDWithContent string, stream pb.CommandExecutor_StartKafkaConsumerServer) error {
	var instanceCP KafkaConsumerInstance

	err := json.Unmarshal([]byte(taskIDWithContent), &instanceCP)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Failed to parse task data: %v", err)
	}

	var task *Task
	val, ok := tm.tasks.Load(instanceCP.TaskID)
	if !ok {
		task = &Task{
			ID:      instanceCP.TaskID,
			Type:    "kafka",
			Command: "kafka-consumer-perf",
			Status:  "created",
		}
		tm.tasks.Store(instanceCP.TaskID, task)
	} else {
		task = val.(*Task)
		task.mu.Lock()
		// 检查任务是否已经在运行中
		if task.Process != nil && task.Process.Process != nil {
			// 检查进程是否真的在运行
			if task.Process.ProcessState == nil || !task.Process.ProcessState.Exited() {
				task.mu.Unlock()
				// 任务已经在运行，直接返回成功
				return stream.Send(&pb.StreamOutputResponse{
					Line: "Task is already running",
				})
			}
		}
		task.mu.Unlock()
	}

	task.mu.Lock()
	defer task.mu.Unlock()

	kafkaArgs := []string{}
	// 必需参数
	if instanceCP.KubeObject.Spec.Topic != "" {
		kafkaArgs = append(kafkaArgs, "--topic", instanceCP.KubeObject.Spec.Topic)
	}
	if instanceCP.KubeObject.Spec.Counts != "" {
		kafkaArgs = append(kafkaArgs, "--messages", instanceCP.KubeObject.Spec.Counts)
	}

	// 可选参数
	if instanceCP.KubeObject.Spec.Size != "" {
		kafkaArgs = append(kafkaArgs, "--fetch-size", instanceCP.KubeObject.Spec.Size)
	}
	if instanceCP.KubeObject.Spec.ConsumerGP != "" {
		kafkaArgs = append(kafkaArgs, "--group", instanceCP.KubeObject.Spec.ConsumerGP)
	}

	if instanceCP.KubeObject.Spec.Hostname != "" && instanceCP.KubeObject.Spec.Port != "" {
		kafkaArgs = append(kafkaArgs, "--bootstrap-server", fmt.Sprintf("%s:%s", instanceCP.KubeObject.Spec.Hostname, instanceCP.KubeObject.Spec.Port))
	}

	if instanceCP.KubeObject.Spec.Username != "" && instanceCP.KubeObject.Spec.Password != "" {
		kafkaArgs = append(kafkaArgs, "sasl.mechanism=PLAIN", "security.protocol=SASL_PLAINTEXT", fmt.Sprintf("sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", instanceCP.KubeObject.Spec.Username, instanceCP.KubeObject.Spec.Password))
	}

	cfg := config.GetConfig()
	kafkaScriptPath := cfg.Chaos.Kafka.KafkaConsumerPerf.Dir

	cmd := exec.Command(kafkaScriptPath, kafkaArgs...)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		task.Status = "paused"
		tm.updateKCRedisStatus(instanceCP, "paused")
		return status.Errorf(codes.Internal, "Failed to get stdout pipe: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		task.Status = "paused"
		tm.updateKCRedisStatus(instanceCP, "paused")
		return status.Errorf(codes.Internal, "Failed to get stderr pipe: %v", err)
	}

	// 启动命令
	if err := cmd.Start(); err != nil {
		errorMsg := fmt.Sprintf("Failed to start command: %v", err)
		tm.handleTaskError(instanceCP.TaskID, errorMsg, err)
		return status.Errorf(codes.Internal, errorMsg)
	}

	// 启动成功，更新状态
	task.Process = cmd
	task.Status = "running"
	tm.updateKCRedisStatus(instanceCP, "running")

	// 立即返回成功响应
	if err := stream.Send(&pb.StreamOutputResponse{
		Line: "Task started successfully",
	}); err != nil {
		log.Printf("Failed to send initial response: %v", err)
	}

	go tm.monitorCommandWithSeparatePipesCP(cmd, stdoutPipe, stderrPipe, instanceCP.TaskID, instanceCP)

	return nil

}

func (tm *TaskManager) StopTask(taskIDWithContent string) error {

	var instanceKP KafkaProducerInstance
	err := json.Unmarshal([]byte(taskIDWithContent), &instanceKP)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Failed to parse task data: %v", err)
	}

	val, ok := tm.tasks.Load(instanceKP.TaskID)
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
		task.Process = nil // 清理旧的进程引用
		task.Status = "stopped"
	}

	if instanceKP.KubeObject.Spec.Recycle == "true" {
		cfg := config.GetConfig()
		kafkaRecyclePath := cfg.Chaos.Kafka.KafkaTopics.Dir

		kafkaRecycleArgs := []string{
			"--topic", instanceKP.KubeObject.Spec.Topic,
			"--bootstrap-server", fmt.Sprintf("%s:%s", instanceKP.KubeObject.Spec.Hostname, instanceKP.KubeObject.Spec.Port),
			"--delete",
		}

		recycleCmd := exec.Command(kafkaRecyclePath, kafkaRecycleArgs...)

		if err := recycleCmd.Start(); err != nil {
			log.Printf("%s kafka-producer-perf recycle start failed: %v", instanceKP.TaskID, err)
		}

		if err := recycleCmd.Wait(); err != nil {
			log.Printf("%s kafka-producer-perf recycle wait failed: %v", instanceKP.TaskID, err)
		}

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

// 异步监控任务执行
//func (tm *TaskManager) monitorKafkaTask(cmd *exec.Cmd, stdoutPipe io.ReadCloser, stderrPipe io.ReadCloser, taskID string, spec KafkaProducerPerfSpec) {
//	// 捕获命令输出并写入 Redis
//	//stdoutPipe, _ := cmd.StdoutPipe()
//	//stderrPipe, _ := cmd.StderrPipe()
//
//	// 创建组合的 reader
//	combinedReader := io.MultiReader(stdoutPipe, stderrPipe)
//	scanner := bufio.NewScanner(combinedReader)
//
//	// 实时读取输出并写入 Redis
//	for scanner.Scan() {
//		line := scanner.Text()
//		timestamp := time.Now().Format(time.RFC3339)
//		logLine := fmt.Sprintf("[%s] %s", timestamp, line)
//
//		// 写入 Redis
//		tm.writeOutputToRedis(taskID, logLine)
//	}
//
//	// 等待命令完成
//	err := cmd.Wait()
//
//	// 根据执行结果更新状态
//	if err != nil {
//		tm.handleTaskError(taskID, "Command execution failed", err)
//	} else {
//		tm.handleTaskSuccess(taskID)
//	}
//}

// 更新 Redis 状态
func (tm *TaskManager) updateKPRedisStatus(instanceKP KafkaProducerInstance, status string) {
	ctx := context.Background()
	//key := fmt.Sprintf("task:%s:status", instanceKP.TaskID)
	instanceKey := fmt.Sprintf("chaos-task:instance:%s", instanceKP.TaskID)

	instanceKP.Status = status
	instanceKPStr, err := json.Marshal(instanceKP)
	if err != nil {
		return
	}

	_, err = tm.redis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, instanceKey, "json", string(instanceKPStr))
		pipe.Set(ctx, taskStatusKey, status, 0)
		return nil
	})
	return
	//tm.redis.HSet(ctx, instanceKey, "json", string(instanceKPStr))

}

func (tm *TaskManager) updateKCRedisStatus(instanceKC KafkaConsumerInstance, status string) {
	ctx := context.Background()
	//key := fmt.Sprintf("task:%s:status", instanceKP.TaskID)
	instanceKey := fmt.Sprintf("chaos-task:instance:%s", instanceKC.TaskID)

	instanceKC.Status = status
	instanceKCStr, err := json.Marshal(instanceKC)
	if err != nil {
		return
	}

	_, err = tm.redis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, instanceKey, "json", string(instanceKCStr))
		pipe.Set(ctx, taskStatusKey, status, 0)
		return nil
	})
	return
}

func (tm *TaskManager) updateRedisStatus(taskID, status string) {
	ctx := context.Background()
	key := fmt.Sprintf("task:%s:status", taskID)

	err := tm.redis.Set(ctx, key, status, 0).Err()
	if err != nil {
		log.Printf("Failed to update Redis status for task %s: %v", taskID, err)
	}
}

// 写入输出到 Redis
func (tm *TaskManager) writeOutputToRedis(taskID, line string) {
	ctx := context.Background()
	key := fmt.Sprintf("chaos-task:output:%s", taskID)

	// 使用列表存储输出，保留历史记录
	err := tm.redis.RPush(ctx, key, line).Err()
	if err != nil {
		log.Printf("Failed to write output to Redis for task %s: %v", taskID, err)
	}

	// 可选：限制输出列表长度，避免内存占用过大
	tm.redis.LTrim(ctx, key, -1000, -1)
}

// 处理任务错误
func (tm *TaskManager) handleTaskError(taskID, message string, err error) {
	log.Printf("%s: %v", message, err)

	// 更新内存状态
	if val, ok := tm.tasks.Load(taskID); ok {
		task := val.(*Task)
		task.mu.Lock()
		task.Status = "paused"
		task.mu.Unlock()
	}

	// 更新 Redis 状态
	tm.updateRedisStatus(taskID, "paused")

	// 记录错误信息到 Redis
	//errorMsg := fmt.Sprintf("[ERROR] %s: %v", message, err)

	// 获取中国时区的时间
	chinaTimezone, _ := time.LoadLocation("Asia/Shanghai")
	timestamp := time.Now().In(chinaTimezone).Format("2006-01-02T15:04:05-07:00")
	logLine := fmt.Sprintf("[%s] [ERROR] %s: %v", timestamp, message, err)

	tm.writeOutputToRedis(taskID, logLine)
}

// 处理任务成功
func (tm *TaskManager) handleTaskSuccess(taskID string) {
	log.Printf("Task %s completed successfully", taskID)

	// 更新内存状态
	if val, ok := tm.tasks.Load(taskID); ok {
		task := val.(*Task)
		task.mu.Lock()
		task.Status = "finished"
		task.mu.Unlock()
	}

	// 更新 Redis 状态
	tm.updateRedisStatus(taskID, "finished")

	// 记录完成信息到 Redis
	chinaTimezone, _ := time.LoadLocation("Asia/Shanghai")
	timestamp := time.Now().In(chinaTimezone).Format("2006-01-02T15:04:05-07:00")
	logLine := fmt.Sprintf("[%s] [INFO] Task completed successfully", timestamp)

	tm.writeOutputToRedis(taskID, logLine)
}

// 分开处理 stdout 和 stderr 的监控函数
func (tm *TaskManager) monitorCommandWithSeparatePipesKP(cmd *exec.Cmd, stdoutPipe, stderrPipe io.ReadCloser, taskID string, instanceKP KafkaProducerInstance) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in task %s: %v", taskID, r)
			tm.handleTaskError(taskID, "Panic occurred", fmt.Errorf("%v", r))
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// 处理 stdout
	go func() {
		defer wg.Done()
		defer stdoutPipe.Close()

		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {

				// 获取中国时区的时间
				chinaTimezone, _ := time.LoadLocation("Asia/Shanghai")
				timestamp := time.Now().In(chinaTimezone).Format("2006-01-02T15:04:05-07:00")
				logLine := fmt.Sprintf("[%s] [OUT] %s", timestamp, line)
				tm.writeOutputToRedis(taskID, logLine)
				//timestamp := time.Now().Format(time.RFC3339)
				//logLine := fmt.Sprintf("[%s] [OUT] %s", timestamp, line)
				//tm.writeOutputToRedis(taskID, logLine)
			}
		}

		if instanceKP.KubeObject.Spec.Recycle == "true" {
			cfg := config.GetConfig()
			kafkaRecyclePath := cfg.Chaos.Kafka.KafkaTopics.Dir

			kafkaRecycleArgs := []string{
				"--topic", instanceKP.KubeObject.Spec.Topic,
				"--bootstrap-server", fmt.Sprintf("%s:%s", instanceKP.KubeObject.Spec.Hostname, instanceKP.KubeObject.Spec.Port),
				"--delete",
			}

			recycleCmd := exec.Command(kafkaRecyclePath, kafkaRecycleArgs...)

			if err := recycleCmd.Start(); err != nil {
				log.Printf("%s kafka-producer-perf recycle start failed: %v", taskID, err)
			}

			if err := recycleCmd.Wait(); err != nil {
				log.Printf("%s kafka-producer-perf recycle wait failed: %v", taskID, err)
			}

		}

		if err := scanner.Err(); err != nil {
			log.Printf("Stdout scanner error for task %s: %v", taskID, err)
		}
	}()

	// 处理 stderr（重要：用于捕获错误信息）
	go func() {
		defer wg.Done()
		defer stderrPipe.Close()

		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				// 获取中国时区的时间
				chinaTimezone, _ := time.LoadLocation("Asia/Shanghai")
				timestamp := time.Now().In(chinaTimezone).Format("2006-01-02T15:04:05-07:00")
				logLine := fmt.Sprintf("[%s] [OUT] %s", timestamp, line)
				//timestamp := time.Now().Format(time.RFC3339)
				//logLine := fmt.Sprintf("[%s] [ERR] %s", timestamp, line)
				tm.writeOutputToRedis(taskID, logLine)
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Stderr scanner error for task %s: %v", taskID, err)
		}
	}()

	// 等待所有输出处理完成
	wg.Wait()

	// 等待命令完成
	err := cmd.Wait()

	// 获取退出状态和错误输出
	exitCode := 0
	var stderrContent string

	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode = exitErr.ExitCode()
		stderrContent = string(exitErr.Stderr)
	}

	if err != nil {
		// 命令执行失败
		tm.updateKPRedisStatus(instanceKP, "paused")
		errorMsg := fmt.Sprintf("Command failed with exit code %d: %v", exitCode, err)
		if stderrContent != "" {
			errorMsg += "\nStderr: " + stderrContent
		}

		log.Printf("Task %s failed: %s", taskID, errorMsg)

		chinaTimezone, _ := time.LoadLocation("Asia/Shanghai")
		timestamp := time.Now().In(chinaTimezone).Format("2006-01-02T15:04:05-07:00")
		logLine := fmt.Sprintf("[%s] [FATAL] Command failed with exit code %d: %v", timestamp, exitCode, err)

		tm.writeOutputToRedis(taskID, logLine)
		tm.handleTaskError(taskID, "Command execution failed", err)
	} else {
		// 命令执行成功
		log.Printf("Task %s completed successfully with exit code %d", taskID, exitCode)
		chinaTimezone, _ := time.LoadLocation("Asia/Shanghai")
		timestamp := time.Now().In(chinaTimezone).Format("2006-01-02T15:04:05-07:00")
		logLine := fmt.Sprintf("[%s] [INFO] Command completed successfully with exit code %d", timestamp, exitCode)

		tm.writeOutputToRedis(taskID, logLine)
		tm.handleTaskSuccess(taskID)
		tm.updateKPRedisStatus(instanceKP, "finished")
	}
}

func (tm *TaskManager) monitorCommandWithSeparatePipesCP(cmd *exec.Cmd, stdoutPipe, stderrPipe io.ReadCloser, taskID string, instanceKC KafkaConsumerInstance) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in task %s: %v", taskID, r)
			tm.handleTaskError(taskID, "Panic occurred", fmt.Errorf("%v", r))
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// 处理 stdout
	go func() {
		defer wg.Done()
		defer stdoutPipe.Close()

		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {

				// 获取中国时区的时间
				chinaTimezone, _ := time.LoadLocation("Asia/Shanghai")
				timestamp := time.Now().In(chinaTimezone).Format("2006-01-02T15:04:05-07:00")
				logLine := fmt.Sprintf("[%s] [OUT] %s", timestamp, line)
				tm.writeOutputToRedis(taskID, logLine)
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Stdout scanner error for task %s: %v", taskID, err)
		}
	}()

	go func() {
		defer wg.Done()
		defer stderrPipe.Close()

		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				chinaTimezone, _ := time.LoadLocation("Asia/Shanghai")
				timestamp := time.Now().In(chinaTimezone).Format("2006-01-02T15:04:05-07:00")
				logLine := fmt.Sprintf("[%s] [OUT] %s", timestamp, line)

				tm.writeOutputToRedis(taskID, logLine)
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Stderr scanner error for task %s: %v", taskID, err)
		}
	}()

	// 等待所有输出处理完成
	wg.Wait()
	// 等待命令完成
	err := cmd.Wait()

	// 获取退出状态和错误输出
	exitCode := 0
	var stderrContent string

	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode = exitErr.ExitCode()
		stderrContent = string(exitErr.Stderr)
	}

	if err != nil {

		tm.updateKCRedisStatus(instanceKC, "paused")
		errorMsg := fmt.Sprintf("Command failed with exit code %d: %v", exitCode, err)
		if stderrContent != "" {
			errorMsg += "\nStderr: " + stderrContent
		}
		log.Printf("Task %s failed: %s", taskID, errorMsg)

		chinaTimezone, _ := time.LoadLocation("Asia/Shanghai")
		timestamp := time.Now().In(chinaTimezone).Format("2006-01-02T15:04:05-07:00")
		logLine := fmt.Sprintf("[%s] [FATAL] Command failed with exit code %d: %v", timestamp, exitCode, err)

		tm.writeOutputToRedis(taskID, logLine)
		tm.handleTaskError(taskID, "Command execution failed", err)
	} else {
		// 命令执行成功
		log.Printf("Task %s completed successfully with exit code %d", taskID, exitCode)
		chinaTimezone, _ := time.LoadLocation("Asia/Shanghai")
		timestamp := time.Now().In(chinaTimezone).Format("2006-01-02T15:04:05-07:00")
		logLine := fmt.Sprintf("[%s] [INFO] Command completed successfully with exit code %d", timestamp, exitCode)

		tm.writeOutputToRedis(taskID, logLine)
		tm.handleTaskSuccess(taskID)
		tm.updateKCRedisStatus(instanceKC, "finished")
	}
}

// StartTaskRecoveryScheduler 启动任务恢复调度器
func (tm *TaskManager) StartTaskRecoveryScheduler() {
	// 每分钟检查一次
	ticker := time.NewTicker(tm.recoveryInterval)
	defer ticker.Stop()

	for range ticker.C {
		tm.recoverOrphanedTasks()
	}
}

// recoverOrphanedTasks 恢复孤儿任务（状态为running但实际不在内存中的任务）
func (tm *TaskManager) recoverOrphanedTasks() {
	ctx := context.Background()

	// 1. 获取所有匹配的 task instance keys
	instanceKeys, err := tm.getTaskInstanceKeys(ctx)
	if err != nil {
		log.Printf("Failed to get task instance keys: %v", err)
		return
	}

	// 2. 遍历所有实例
	for _, key := range instanceKeys {
		tm.processTaskInstance(ctx, key)
	}
}

// getTaskInstanceKeys 获取所有任务实例的key
func (tm *TaskManager) getTaskInstanceKeys(ctx context.Context) ([]string, error) {
	// 使用 SCAN 命令避免阻塞，特别是当key很多时
	var keys []string
	var cursor uint64
	//var err error

	for {
		// 扫描匹配 chaos-task:instance:* 的key
		result, nextCursor, err := tm.redis.Scan(ctx, cursor, taskInstanceKeyPrefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}

		keys = append(keys, result...)
		cursor = nextCursor

		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

// processTaskInstance 处理单个任务实例
func (tm *TaskManager) processTaskInstance(ctx context.Context, key string) {
	// 1. 获取任务实例数据
	data, err := tm.redis.HGetAll(ctx, key).Result()
	if err != nil {
		log.Printf("Failed to get task data for key %s: %v", key, err)
		return
	}

	var instance CommonInstance

	if err := json.Unmarshal([]byte(data["json"]), &instance); err != nil {
		return
	}

	_, exists := tm.tasks.Load(instance.TaskID)

	if instance.KubeObject.Kind == "KafkaProducerPerf" && !exists {
		var instanceKP KafkaProducerInstance
		if err := json.Unmarshal([]byte(data["json"]), &instanceKP); err != nil {
			return
		}
		// 5. 检查状态是否为 running
		if instanceKP.Status != "running" {
			return
		}
		tm.updateKPRedisStatus(instanceKP, "paused")

	} else if instance.KubeObject.Kind == "KafkaConsumerPerf" && !exists {
		var instanceKC KafkaConsumerInstance
		if err := json.Unmarshal([]byte(data["json"]), &instanceKC); err != nil {
			return
		}
		// 5. 检查状态是否为 running
		if instanceKC.Status != "running" {
			return
		}
		tm.updateKCRedisStatus(instanceKC, "paused")
	} else {
		return
	}

	//if _, exists := tm.tasks.Load(instance.TaskID); exists {
	//	return // 内存中存在对应task，不做额外处理，只处理内存中没有但是redis中状态为running的task
	//}
}

// isTargetTaskType 检查是否为目标任务类型
func (tm *TaskManager) isTargetTaskType(instance map[string]interface{}) bool {
	// 检查 kind 是否为 InvokeChaos
	kind, ok := instance["kind"].(string)
	if !ok || kind != "InvokeChaos" {
		return false
	}

	// 检查 kube_object.kind
	kubeObject, ok := instance["kube_object"].(map[string]interface{})
	if !ok {
		return false
	}

	kubeObjectKind, ok := kubeObject["kind"].(string)
	if !ok {
		return false
	}

	// 检查是否为 KafkaProducerPerf 或 KafkaConsumerPerf
	return kubeObjectKind == "KafkaProducerPerf" || kubeObjectKind == "KafkaConsumerPerf"
}

// 可选的：批量处理版本（性能更好）
func (tm *TaskManager) recoverOrphanedTasksBatch() {
	ctx := context.Background()

	// 使用管道批量处理
	pipe := tm.redis.Pipeline()
	defer pipe.Close()

	instanceKeys, err := tm.getTaskInstanceKeys(ctx)
	if err != nil {
		log.Printf("Failed to get task instance keys: %v", err)
		return
	}

	recoveryCount := 0
	for _, key := range instanceKeys {
		data, err := tm.redis.HGet(ctx, key, "json").Bytes()
		if err != nil {
			continue
		}

		var instance map[string]interface{}
		if err := json.Unmarshal(data, &instance); err != nil {
			continue
		}

		if !tm.isTargetTaskType(instance) {
			continue
		}

		taskID, ok := instance["task_id"].(string)
		if !ok || taskID == "" {
			continue
		}

		status, ok := instance["status"].(string)
		if !ok || status != "running" {
			continue
		}

		if _, exists := tm.tasks.Load(taskID); exists {
			continue
		}

		// 批量更新
		var instanceCopy map[string]interface{}
		json.Unmarshal(data, &instanceCopy)
		instanceCopy["status"] = "paused"
		updatedData, _ := json.Marshal(instanceCopy)

		pipe.HSet(ctx, key, "json", string(updatedData))
		recoveryCount++
	}

	// 执行批量更新
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("Failed to execute batch recovery: %v", err)
	} else if recoveryCount > 0 {
		log.Printf("Batch recovered %d orphaned tasks", recoveryCount)
	}
}
