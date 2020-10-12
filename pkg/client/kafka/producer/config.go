package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/douyu/jupiter/pkg/client/kafka/config"
	"github.com/douyu/jupiter/pkg/conf"
	"github.com/douyu/jupiter/pkg/xlog"
	"github.com/fatih/structs"
)

type ConfigHighLevel struct {
	config.ConfigHighLevel `json:",flatten"`

	// Enables the transactional producer. The transactional.id is used to identify the same transactional producer instance across process restarts. It allows the producer to guarantee that transactions corresponding to earlier instances of the same producer have been finalized prior to starting any new transactions, and that any zombie instances are fenced off. If no transactional.id is provided, then the producer is limited to idempotent delivery (if enable.idempotence is set). Requires broker version >= 0.11.0.
	// Type: string
	TransactionalID string `json:"transactional.id"`

	// When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: max.in.flight.requests.per.connection=5 (must be less than or equal to 5), retries=INT32_MAX (must be greater than 0), acks=all, queuing.strategy=fifo. Producer instantation will fail if user-supplied configuration is incompatible.
	// Type: boolean
	EnableIdempotence bool `json:"enable.idempotence"`

	// Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions.
	// range: 1 ~ 1e7
	// Type: integer
	QueueBufferingMaxMessages int `json:"queue.buffering.max.messages"`

	// Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions. This property has higher priority than queue.buffering.max.messages.
	// range 1 ~ 2147483647
	// Type: integer
	QueueBufferingMaxKbytes int `json:"queue.buffering.max.kbytes"`

	// Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
	// range: 0 ~ 9e5
	// Type: float; 实际上应该是 integer
	// alias: linger.ms
	QueueBufferingMaxMs int `json:"queue.buffering.max.ms"`

	// How many times to retry sending a failing Message. Note: retrying may cause reordering unless enable.idempotence is set to true.
	// range: 0 ~ 10000000
	// Type: integer
	// alias: retries
	MessageSendMaxRetries int `json:"message.send.max.retries"`
}

func DefaultConfigKafkaHigh() ConfigHighLevel {
	return ConfigHighLevel{
		ConfigHighLevel: config.DefaultKafkaConfigHigh(),

		TransactionalID: "",

		EnableIdempotence: false,

		QueueBufferingMaxMessages: 1e5,

		QueueBufferingMaxKbytes: 1048576,

		QueueBufferingMaxMs: 5,

		MessageSendMaxRetries: 10000000,
	}
}

type ConfigMediumLevel struct {
	config.ConfigMediumLevel `json:",flatten"`

	// The maximum amount of time in milliseconds that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction. If this value is larger than the transaction.max.timeout.ms setting in the broker, the init_transactions() call will fail with ERR_INVALID_TRANSACTION_TIMEOUT. The transaction timeout automatically adjusts message.timeout.ms and socket.timeout.ms, unless explicitly configured in which case they must not exceed the transaction timeout (socket.timeout.ms must be at least 100ms lower than transaction.timeout.ms). This is also the default timeout value if no timeout (-1) is supplied to the transactional API methods.
	// range: 1e3 ~ 2147483647
	// Type: integer
	TransactionTimeoutMs int `json:"transaction.timeout.ms"`

	// The backoff time in milliseconds before retrying a protocol request.
	// range: 1 ~ 3e5
	// Type: integer
	RetryBackoffMs int `json:"retry.backoff.ms"`

	// compression codec to use for compressing message sets. This is the default value for all topics, may be overridden by the topic configuration property compression.codec.
	// range: none, gzip, snappy, lz4, zstd
	// Type: enum value
	// alias: compression.type
	CompressionCodec string `json:"compression.codec"`

	// Maximum number of messages batched in one MessageSet. The total MessageSet size is also limited by batch.size and message.max.bytes.
	// range: 1 ~ 1e6
	// Type: integer
	BatchNumMessages int `json:"batch.num.messages"`

	// Maximum size (in bytes) of all messages batched in one MessageSet, including protocol framing overhead. This limit is applied after the first message has been added to the batch, regardless of the first message's size, this is to ensure that messages that exceed batch.size are produced. The total MessageSet size is also limited by batch.num.messages and message.max.bytes.
	// range: 1 ~ 2147483647
	// Type: integer
	// NOTE: 此项配置在 https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	// 中有介绍可以使用，但在实际使用中，被被提示 'No such configuration property'
	// BatchSize int `json:"batch.size"`
}

func DefaultConfigKafkaMedium() ConfigMediumLevel {
	return ConfigMediumLevel{
		ConfigMediumLevel: config.DefaultKafkaConfigMedium(),

		TransactionTimeoutMs: 6e4,

		RetryBackoffMs: 100,

		CompressionCodec: "none",

		BatchNumMessages: 1e4,

		// BatchSize: 1e6,
	}
}

type ConfigLowLevel struct {
	config.ConfigLowLevel `json:",flatten"`

	// EXPERIMENTAL: subject to change or removal. When set to true, any error that could result in a gap in the produced message series when a batch of messages fails, will raise a fatal error (ERR__GAPLESS_GUARANTEE) and stop the producer. Messages failing due to message.timeout.ms are not covered by this guarantee. Requires enable.idempotence=true.
	// Type: boolean
	EnableGaplessGuarantee bool `json:"enable.gapless.guarantee"`

	// The threshold of outstanding not yet transmitted broker requests needed to backpressure the producer's message accumulator. If the number of not yet transmitted requests equals or exceeds this number, produce request creation that would have otherwise been triggered (for example, in accordance with linger.ms) will be delayed. A lower number yields larger and more effective batches. A higher value can improve latency when using compression on slow machines.
	// range: 1 ~ 1e6
	// Type: integer
	QueueBufferingBackpressureThreshold int `json:"queue.buffering.backpressure.threshold"`

	// 	Only provide delivery reports for failed messages.
	// Type: boolean
	DeliveryReportOnlyError bool `json:"delivery.report.only.error"`

	// Delivery report callback (set with rd_kafka_conf_set_dr_cb())
	// Type: see dedicated API
	DrCb interface{} `json:"-"`

	// Delivery report callback (set with rd_kafka_conf_set_dr_msg_cb())
	// Type: see dedicated API
	DrMsgCb interface{} `json:"-"`
}

func DefaultConfigKafkaLow() ConfigLowLevel {
	return ConfigLowLevel{
		ConfigLowLevel: config.DefaultKafkaConfigLow(),

		EnableGaplessGuarantee: false,

		QueueBufferingBackpressureThreshold: 1,

		DeliveryReportOnlyError: false,
	}
}

type Config struct {
	ConfigHighLevel   `json:",flatten"`
	ConfigMediumLevel `json:",flatten"`
	ConfigLowLevel    `json:",flatten"`

	logger *xlog.Logger `json:"-"`
}

func DefaultKafkaConfig() Config {
	return Config{
		ConfigHighLevel:   DefaultConfigKafkaHigh(),
		ConfigMediumLevel: DefaultConfigKafkaMedium(),
		ConfigLowLevel:    DefaultConfigKafkaLow(),
		logger:            xlog.JupiterLogger,
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
	return RawKafkaConfig("jupiter.kafka.producer." + name)
}

// Build ...
func (config *Config) Build() *Producer {
	if config == nil {
		return nil
	}

	var producer Producer
	producer.Config = config

	structs.DefaultTagName = "json"
	var m = structs.Map(config)

	var kafkaConf = make(kafka.ConfigMap)
	for k, v := range m {
		kafkaConf.SetKey(k, v)
	}

	for k, v := range kafkaConf {
		config.logger.Info("kv", xlog.Any(k, fmt.Sprintf("%v, %T", v, v)))
	}

	var p, err = kafka.NewProducer(&kafkaConf)
	if err != nil {
		config.logger.Panic("new kafka producer failed", xlog.String("error", err.Error()))
		return nil
	}

	producer.Producer = p
	return &producer
}
