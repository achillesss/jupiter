package admin

import (
	"github.com/achillesss/jupiter/pkg/client/kafka/config"
	"github.com/achillesss/jupiter/pkg/conf"
	"github.com/achillesss/jupiter/pkg/xlog"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gqcn/structs"
)

type kafkaConfig struct {
	config.ConfigHighLevel `json:",flatten"`
}

type Config struct {
	KafkaConfig kafkaConfig `json:"kafka_config"`
	logger      *xlog.Logger
}

func DefaultKafkaConfig() Config {
	return Config{
		KafkaConfig: kafkaConfig{
			ConfigHighLevel: config.DefaultConfigHigh(),
		},

		logger: xlog.JupiterLogger,
	}
}

// RawKafkaConfig ...
func RawKafkaConfig(key string) Config {
	var config = DefaultKafkaConfig()

	if err := conf.UnmarshalKey(key, &config); err != nil {
		xlog.Panic("unmarshal kafkaConfig",
			xlog.String("key", key),
			xlog.Any("kafkaConfig", config),
			xlog.String("error", err.Error()))
	}
	return config
}

// StdKafkaConfig ...
func StdKafkaConfig(name string) Config {
	return RawKafkaConfig("jupiter.kafka.admin." + name)
}

// Build ...
func (config *Config) Build() *Admin {
	if config == nil {
		return nil
	}

	var admin Admin

	structs.DefaultTagName = "json"
	var m = structs.Map(config.KafkaConfig)

	var kafkaConf = make(kafka.ConfigMap)
	for k, v := range m {
		kafkaConf.SetKey(k, v)
	}

	var a, err = kafka.NewAdminClient(&kafkaConf)
	if err != nil {
		config.logger.Panic("new kafka producer failed", xlog.String("error", err.Error()))
		return nil
	}

	admin.AdminClient = a
	return &admin
}
