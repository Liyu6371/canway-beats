package kafka

import (
	"canway-beats/logger"
	"canway-beats/source"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/mitchellh/mapstructure"
)

const (
	Name = "kafka"
)

var (
	rebalance         = sarama.BalanceStrategyRange
	consumerGroup     = "kafka_default_consumer_group"
	kafkaRebalanceMap = map[string]sarama.BalanceStrategy{
		"sticky":     sarama.BalanceStrategySticky,
		"roundrobin": sarama.BalanceStrategyRoundRobin,
		"range":      sarama.BalanceStrategyRange,
	}
)

type kafkaConf struct {
	Enabled       bool     `mapstructure:"enabled"`
	Host          string   `mapstructure:"host"`
	Port          int      `mapstructure:"port"`
	Username      string   `mapstructure:"username"`
	Password      string   `mapstructure:"password"`
	Version       string   `mapstructure:"version"`
	ConsumerGroup string   `mapstructure:"kafka_consumer_group"`
	ConsumeOldest bool     `mapstructure:"kafka_oldest"`
	Assignor      string   `mapstructure:"kafka_assignor"`
	Topics        []string `mapstructure:"topics"`
}

type kafkaSource struct {
	c kafkaConf

	ctx    context.Context
	cancel context.CancelFunc

	client  sarama.ConsumerGroup
	handler sarama.ConsumerGroupHandler
	wg      sync.WaitGroup
	mu      sync.RWMutex
	state   bool
}

func init() {
	source.RegistSource(Name, New)
}

func New(conf map[string]interface{}) source.Source {
	v, ok := conf[Name]
	if !ok {
		logger.Errorln("kafka source instance: config not found")
		return nil
	}
	c := kafkaConf{}
	if err := mapstructure.Decode(v, &c); err != nil {
		logger.Errorf("kafka source instance: unable to decode config, error: %s", err)
		return nil
	}
	if c.Host == "" || c.Port == 0 || len(c.Topics) == 0 {
		logger.Errorln("kafka source instance: plz check configuration, some important parama is empty")
		return nil
	}

	saramaConf := sarama.NewConfig()
	if c.Version != "" {
		if v, err := sarama.ParseKafkaVersion(c.Version); err == nil {
			saramaConf.Version = v
		}
	}

	if c.Username != "" && c.Password != "" {
		saramaConf.Net.SASL.Enable = true
		saramaConf.Net.SASL.User = c.Username
		saramaConf.Net.SASL.Password = c.Password
	}

	if c.ConsumeOldest {
		saramaConf.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		saramaConf.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	if v, ok := kafkaRebalanceMap[c.Assignor]; ok {
		saramaConf.Consumer.Group.Rebalance.Strategy = v
	} else {
		saramaConf.Consumer.Group.Rebalance.Strategy = rebalance
	}

	if c.ConsumerGroup != "" {
		consumerGroup = c.ConsumerGroup
	}

	addr := fmt.Sprintf("%s:%d", c.Host, c.Port)
	client, err := sarama.NewConsumerGroup([]string{addr}, consumerGroup, saramaConf)
	if err != nil {
		logger.Errorf("kafka source instance: create consumer group error: %s", err)
		return nil
	}
	return &kafkaSource{
		c:      c,
		client: client,
		wg:     sync.WaitGroup{},
		mu:     sync.RWMutex{},
		state:  false,
	}
}

func (k *kafkaSource) GetName() string {
	return Name
}

func (k *kafkaSource) StartSource(parent context.Context, output chan<- []byte) error {
	k.ctx, k.cancel = context.WithCancel(parent)
	// 传递 channel 给到具体的 kafka Handler 用于将消费的数据写入到上层 sourceCtrl
	k.handler = &Handler{
		ch: output,
	}
	// 标志运行状态为 true
	k.turnOn()
	k.wg.Add(1)

	go func() {
		defer func() {
			k.turnOff()
			k.wg.Done()
			k.client.Close()
		}()
		for {
			if err := k.client.Consume(k.ctx, k.c.Topics, k.handler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					logger.Errorf("kafka source instance: error is : %s", err)
					return
				}
				logger.Errorf("kafka source instance: Error from consumer: %s", err)
				return
			}
			if k.ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

func (k *kafkaSource) StopSource() {
	logger.Infoln("kafka source instance: catch exit signal")
	k.cancel()
	k.wg.Wait()
	if err := k.client.Close(); err != nil {
		logger.Errorf("kafka source instance: an error happend during close kafka source instance, error: %s", err)
	}
	logger.Infoln("kafka source instance: exit")
}

func (k *kafkaSource) turnOn() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.state {
		k.state = true
	}
}

func (k *kafkaSource) turnOff() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.state {
		k.state = false
	}

}

func (k *kafkaSource) IsRunning() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.state
}

type Handler struct {
	ch chan<- []byte
}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				logger.Infof("Source->%s message chan claim is closed.", Name)
				return nil
			}
			h.ch <- msg.Value
		case <-session.Context().Done():
			logger.Info("session exit")
			return nil
		}
	}
}
