package gomolkafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/aphistic/gomol"
)

type KafkaLoggerConfig struct {

	// The prefix to add before every field name in the JSON data. Defaults to a blank string
	FieldPrefix string
	// A list of field names excluded from having the FieldPrefix added
	UnprefixedFields []string

	// The delimiter to use at the end of every message sent. Defaults to '\n'
	MessageDelimiter []byte

	// The name of the JSON field to put the log level into. Defaults to "level"
	LogLevelField string
	// The name of the JSON field to put the message into. Defaults to "message"
	MessageField string
	// The name of the JSON field to put the timestamp into. Defaults to "timestamp"
	TimestampField string

	// A map to customize the values of each gomol.LogLevel in the JSON message.
	// Defaults to the string value of each gomol.LogLevel
	LogLevelMap map[gomol.LogLevel]interface{}

	// A map of additional attributes to be added to each JSON message sent. This is useful
	// if there fields to send only to a JSON receiver.  These will override any existing
	// attributes already set on a message.
	JSONAttrs map[string]interface{}

	// Kafka Brokers in host:port format
	kafkaBrokers []string

	// Kafka Topics to log to
	kafkaTopics []string
}

type KafkaLogger struct {
	base          *gomol.Base
	isInitialized bool
	unprefixedMap map[string]bool
	config        *KafkaLoggerConfig
	// Configuration for Kafka Producer
	kafkaConfig   *sarama.Config
	kafkaProducer sarama.AsyncProducer
}

func NewKafkaLoggerConfig(brokers []string, topics []string) *KafkaLoggerConfig {

	return &KafkaLoggerConfig{
		kafkaBrokers:     brokers,
		kafkaTopics:      topics,
		FieldPrefix:      "",
		UnprefixedFields: make([]string, 0),
		MessageDelimiter: []byte("\n"),

		LogLevelField:  "level",
		MessageField:   "message",
		TimestampField: "timestamp",

		LogLevelMap: map[gomol.LogLevel]interface{}{
			gomol.LevelDebug:   gomol.LevelDebug.String(),
			gomol.LevelInfo:    gomol.LevelInfo.String(),
			gomol.LevelWarning: gomol.LevelWarning.String(),
			gomol.LevelError:   gomol.LevelError.String(),
			gomol.LevelFatal:   gomol.LevelFatal.String(),
			gomol.LevelNone:    gomol.LevelNone.String(),
		},

		JSONAttrs: make(map[string]interface{}),
	}

}

func NewKafkaLogger(config *KafkaLoggerConfig) (*KafkaLogger, error) {

	fmt.Println("NewKafkaLogger Entered")

	kafkaConfig := sarama.NewConfig()

	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	kafkaConfig.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	kafkaConfig.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(config.kafkaBrokers, kafkaConfig)
	if err != nil {
		fmt.Println("Failed to create AsyncProducer:", err)
		return nil, err
	}

	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			fmt.Println("Failed to write access log entry:", err)
		}
	}()

	l := &KafkaLogger{
		config:        config,
		kafkaConfig:   kafkaConfig,
		kafkaProducer: producer,
	}

	return l, nil
}

func (l *KafkaLogger) IsInitialized() bool {
	fmt.Println("KafkaLogger IsInitialized Entered")

	return l.isInitialized
}

// SetBase will set the gomol.Base this logger is associated with
func (l *KafkaLogger) SetBase(base *gomol.Base) {
	fmt.Println("KafkaLogger SetBase Entered")

	l.base = base
}

// ShutdownLogger shuts down the logger and frees any resources that may be used
func (l *KafkaLogger) ShutdownLogger() error {
	fmt.Println("KafkaLogger ShutdownLogger Entered")

	l.isInitialized = false
	return nil
}

func (l *KafkaLogger) InitLogger() error {
	fmt.Println("KafkaLogger InitLogger Entered")

	l.isInitialized = true
	return nil
}

// Logm sends a JSON log message to the appropriate  kafka broker
func (l *KafkaLogger) Logm(timestamp time.Time, level gomol.LogLevel, attrs map[string]interface{}, msg string) error {

	fmt.Println("KafkaLogger Logm Entered")

	if !l.isInitialized {
		fmt.Println("KafkaLogger Logm Logger Not initialized")
		return errors.New("Kafka logger has not been initialized")
	}

	// Check time for partition key
	var partitionKey sarama.ByteEncoder
	b, err := timestamp.MarshalBinary()
	if err != nil {
		return err
	}

	partitionKey = sarama.ByteEncoder(b)

	msgBytes, err := l.marshalJSON(timestamp, level, attrs, msg)
	if err != nil {
		return err
	}
	msgBytes = append(msgBytes, l.config.MessageDelimiter...)

	err = l.write(partitionKey, msgBytes)

	if err != nil {
		fmt.Printf("KafkaLogger Write  returned error %s", err)
		return err
	}

	return nil
}

func (l *KafkaLogger) write(pk sarama.ByteEncoder, msgBytes []byte) error {

	value := sarama.ByteEncoder(msgBytes)

	for _, topic := range l.config.kafkaTopics {

		l.kafkaProducer.Input() <- &sarama.ProducerMessage{
			Key:   pk,
			Topic: topic,
			Value: value,
		}
	}

	return nil
}

func (l *KafkaLogger) marshalJSON(timestamp time.Time, level gomol.LogLevel, attrs map[string]interface{}, msg string) ([]byte, error) {
	msgMap := make(map[string]interface{})
	if attrs != nil {
		for key := range attrs {
			val := attrs[key]
			if _, ok := l.unprefixedMap[key]; ok {
				msgMap[key] = val
			} else {
				msgMap[l.config.FieldPrefix+key] = val
			}
		}
	}

	// Add level
	var levelVal interface{}
	if lval, ok := l.config.LogLevelMap[level]; ok {
		levelVal = lval
	} else {
		return nil, fmt.Errorf("Log level %v does not have a mapping.", level)
	}
	if _, ok := l.unprefixedMap[l.config.LogLevelField]; ok {
		msgMap[l.config.LogLevelField] = levelVal
	} else {
		msgMap[l.config.FieldPrefix+l.config.LogLevelField] = levelVal
	}

	// Add message
	if _, ok := l.unprefixedMap[l.config.MessageField]; ok {
		msgMap[l.config.MessageField] = msg
	} else {
		msgMap[l.config.FieldPrefix+l.config.MessageField] = msg
	}

	// Add timestamp
	if _, ok := l.unprefixedMap[l.config.TimestampField]; ok {
		msgMap[l.config.TimestampField] = timestamp
	} else {
		msgMap[l.config.FieldPrefix+l.config.TimestampField] = timestamp
	}

	// Add any json attrs that are set
	for jsonKey := range l.config.JSONAttrs {
		if jsonVal, ok := l.config.JSONAttrs[jsonKey]; ok {
			if _, ok := l.unprefixedMap[jsonKey]; ok {
				msgMap[jsonKey] = jsonVal
			} else {
				msgMap[l.config.FieldPrefix+jsonKey] = jsonVal
			}
		}
	}

	jsonBytes, err := json.Marshal(msgMap)
	if err != nil {
		return nil, err
	}

	return jsonBytes, nil
}
