package kafka

import (
	"canway-beats/logger"
	"canway-beats/sender"
	"context"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/mitchellh/mapstructure"
)

const (
	Name = "kafka"
)

var (
	defaultWorker = 1
	defaultBuffer = 500
	defaultAck    = sarama.WaitForAll
	defaultRetry  = 3
	ackMap        = map[string]sarama.RequiredAcks{
		"all":   sarama.WaitForAll,
		"local": sarama.WaitForLocal,
		"no":    sarama.NoResponse,
	}
)

type kafkaConf struct {
	Enabled bool     `mapstructure:"enabled"`
	Worker  int      `mapstructure:"worker"`
	Buffer  int      `mapstructure:"buffer"`
	Ack     string   `mapstructure:"required_acks"`
	Brokers []string `mapstructure:"brokers"`
}

type KafkaSender struct {
	c kafkaConf

	ctx    context.Context
	cancel context.CancelFunc

	wg     sync.WaitGroup
	mu     sync.RWMutex
	ch     chan sender.SenderMsg
	client sarama.SyncProducer
	state  bool
}

func init() {
	sender.RegistSender(Name, New)
}

func New(conf map[string]interface{}) sender.Sender {
	v, ok := conf[Name]
	if !ok {
		logger.Errorln("kafka sender instance: config not found")
		return nil
	}
	c := kafkaConf{}
	if err := mapstructure.Decode(v, &c); err != nil {
		logger.Errorf("kafka sender instance: decode config error: %s", err)
		return nil
	}
	if !c.Enabled {
		logger.Warnln("kafka sender instance: sender not enabled")
		return nil
	}
	if len(c.Brokers) == 0 {
		logger.Errorln("kafka sender instance: kafka brokers is not define")
		return nil
	}
	var ch chan sender.SenderMsg
	if c.Buffer != 0 {
		ch = make(chan sender.SenderMsg, c.Buffer)
	} else {
		ch = make(chan sender.SenderMsg, defaultBuffer)
	}

	ack := defaultAck
	if c.Ack != "" {
		if v, ok := ackMap[c.Ack]; ok {
			ack = v
		}
	}

	kConf := sarama.NewConfig()
	kConf.Producer.Retry.Max = defaultRetry
	kConf.Producer.RequiredAcks = ack
	kConf.Producer.Return.Successes = true
	client, err := sarama.NewSyncProducer(c.Brokers, kConf)
	if err != nil {
		logger.Errorf("kafka sender instance: unable to create kafka producer, error: %s", err)
		return nil
	}

	return &KafkaSender{
		c:      c,
		wg:     sync.WaitGroup{},
		mu:     sync.RWMutex{},
		ch:     ch,
		client: client,
		state:  false,
	}
}

func (k *KafkaSender) GetName() string {
	return Name
}

func (k *KafkaSender) RunSender(parent context.Context) error {
	k.turnOn()
	k.ctx, k.cancel = context.WithCancel(parent)
	var worker int
	if k.c.Worker != 0 {
		worker = k.c.Worker
	} else {
		worker = defaultWorker
	}

	for i := 0; i < worker; i++ {
		k.wg.Add(1)
		go k.consume(i)
	}

	return nil
}

func (k *KafkaSender) consume(idx int) {
	defer func() {
		k.wg.Done()
		k.turnOff()
	}()
	for {
		select {
		case msg, ok := <-k.ch:
			if !ok {
				logger.Warnf("kafka sender instance: channel has been closed, consume goroutine %d exit", idx)
				return
			}
			topic, ok := msg.Where().(string)
			if !ok {
				logger.Errorf("kafka sender instance: unable to transform topic, origin is %+v", msg.Where())
				logger.Debugf("kafka sender instance: transform topic failed, drop data %s", msg.GetData())
				continue
			}
			value := sarama.ByteEncoder(msg.GetData())
			data := &sarama.ProducerMessage{
				Topic: topic,
				Value: value,
			}
			partition, offset, err := k.client.SendMessage(data)
			if err != nil {
				logger.Errorf("kafka sender instance: send msg failed, error: %s", err)
				continue
			} else {
				logger.Debugf("kafka sender instance: send msg success, topic %s, partition %d, offset: %d, data: %s", topic, partition, offset, msg.GetData())
			}
		case <-k.ctx.Done():
			logger.Infof("kafka sender instance: consume goroutine %d exit", idx)
			return
		}
	}

}

func (k *KafkaSender) StopSender() {
	logger.Infoln("kafka sender instance: start to stop sender...")
	k.cancel()
	k.wg.Wait()
	k.client.Close()
	close(k.ch)
	logger.Infoln("kafka sender instance: stop success")
}

func (k *KafkaSender) Push(msg sender.SenderMsg) {
	k.ch <- msg
}

func (k *KafkaSender) turnOn() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.state {
		k.state = true
	}
}

func (k *KafkaSender) turnOff() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.state {
		k.state = false
	}
}

func (k *KafkaSender) IsRunning() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.state
}
