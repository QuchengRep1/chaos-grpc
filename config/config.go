package config

import (
	"sync"
	"time"
)

var (
	globalConfig *ChaosInitConfig
	configMutex  sync.RWMutex
)

type ChaosInitConfig struct {
	Chaos struct {
		Redis struct {
			Address  string `yaml:"address"`
			Port     string `yaml:"port"`
			Database int    `yaml:"database"`
			Password string `yaml:"password"`
		} `yaml:"redis"`

		Kafka struct {
			KafkaTopics struct {
				Dir string `yaml:"dir"`
			} `yaml:"kafka-topics"`

			KafkaProducerPerf struct {
				Dir string `yaml:"dir"`
			} `yaml:"kafka-producer-perf"`

			KafkaConsumerPerf struct {
				Dir string `yaml:"dir"`
			} `yaml:"kafka-consumer-perf"`

			KafkaTaskScheduler struct {
				RecoveryInterval time.Duration `yaml:"recovery-interval"`
			} `yaml:"kafka-task-scheduler"`
		} `yaml:"kafka"`
	} `yaml:"chaos"`
}

func SetConfig(config *ChaosInitConfig) {
	configMutex.Lock()
	defer configMutex.Unlock()
	globalConfig = config
}

func GetConfig() *ChaosInitConfig {
	configMutex.RLock()
	defer configMutex.RUnlock()
	return globalConfig
}
