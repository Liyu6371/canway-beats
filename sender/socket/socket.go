package socket

import (
	"canway-beats/logger"
	"canway-beats/sender"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/libgse/gse"
	"github.com/mitchellh/mapstructure"
)

const (
	Name = "socket"
)

var (
	defaultWorker  = 1
	defaultBuffer  = 500
	defaultGseConf = gse.Config{
		MsgQueueSize:   1,
		WriteTimeout:   5 * time.Second,
		ReadTimeout:    60 * time.Second,
		Nonblock:       false,
		RetryTimes:     3,
		RetryInterval:  3 * time.Second,
		ReconnectTimes: 3,
	}
)

type socketConf struct {
	Enabled  bool   `mapstructure:"enabled"`
	Worker   int    `mapstructure:"worker"`
	Buffer   int    `mapstructure:"buffer"`
	EndPoint string `mapstructure:"end_point"`
}

type GSESocket struct {
	c      socketConf
	client *gse.GseClient

	ctx    context.Context
	cancel context.CancelFunc

	wg    sync.WaitGroup
	mu    sync.RWMutex
	ch    chan sender.SenderMsg
	state bool
}

func init() {
	sender.RegistSender(Name, New)
}

func New(conf map[string]interface{}) sender.Sender {
	v, ok := conf[Name]
	if !ok {
		logger.Errorln("socket sender instance: config not found")
		return nil
	}
	c := socketConf{}
	if err := mapstructure.Decode(v, &c); err != nil {
		logger.Errorf("socket sender instance: decode conf error: %s", err)
		return nil
	}
	if !c.Enabled {
		logger.Warnln("socket sender instance: sender is not enabled")
		return nil
	}
	if c.EndPoint == "" {
		logger.Errorln("socket sender instance: endPoint is not define")
		return nil
	}
	defaultGseConf.Endpoint = c.EndPoint
	client, err := gse.NewGseClientFromConfig(defaultGseConf)
	if err != nil {
		logger.Errorf("socket sender instance: New GSE client error: %s", err)
		return nil
	}
	var ch chan sender.SenderMsg
	if c.Buffer != 0 {
		ch = make(chan sender.SenderMsg, c.Buffer)
	} else {
		ch = make(chan sender.SenderMsg, defaultBuffer)
	}

	return &GSESocket{
		c:      c,
		client: client,
		wg:     sync.WaitGroup{},
		mu:     sync.RWMutex{},
		ch:     ch,
		state:  false,
	}
}

func (g *GSESocket) GetName() string {
	return Name
}

func (g *GSESocket) RunSender(parent context.Context) error {
	// 运行 sender 前需要启动 GseClient, 若失败则要释放 channel
	if err := g.client.Start(); err != nil {
		close(g.ch)
		return fmt.Errorf("socket sender instance: unable to run gse client, error: %s", err)
	}
	// 接收上层的 context
	g.ctx, g.cancel = context.WithCancel(parent)
	// 改变 socket sender instance 运行状态，并且在退出前关闭
	g.turnOn()
	// 开始拉起一定数量的 worker 进行消费
	var worker int
	if g.c.Worker != 0 {
		worker = g.c.Worker
	} else {
		worker = defaultWorker
	}
	for i := 0; i < worker; i++ {
		g.wg.Add(1)
		go g.consume(i)
	}
	return nil
}

// consume 消费 channel 内部的 msg
// 正常条件下 consume 方法的退出只会是因为  channel 关闭 以及 ctx 结束
// 因此在此处进行对  GSESocket 的状态（state）的修正操作
func (g *GSESocket) consume(idx int) {
	defer func() {
		g.turnOff()
		g.wg.Done()
	}()
	for {
		select {
		case msg, ok := <-g.ch:
			if !ok {
				logger.Warnf("socket sender instance: channel has been closed. consume goroutine %d exit", idx)
				return
			}
			dataid, ok := msg.Where().(int32)
			if !ok {
				logger.Errorf("socket sender instance: unable to transform dataid, origin: %+v", msg.Where())
				logger.Debugf("socket sender instance: drop data %s", msg.GetData())
				continue
			}
			err := g.client.Send(gse.NewGseCommonMsg(msg.GetData(), dataid, 0, 0, 0))
			if err != nil {
				logger.Errorf("socket sender instance: send msg error: %s", err)
			} else {
				logger.Debugf("socket sender instance: success to send msg %s to dataid: %d", msg.GetData(), dataid)
			}
		case <-g.ctx.Done():
			logger.Infof("socket sender instance: consume goroutine %d exit", idx)
			return
		}
	}
}

func (g *GSESocket) StopSender() {
	logger.Infoln("socket sender instance: catch stop signal")
	g.cancel()
	g.wg.Wait()
	close(g.ch)
	g.client.Close()
	logger.Infoln("socket sender instance: stop success")
}

func (g *GSESocket) Push(msg sender.SenderMsg) {
	g.ch <- msg
}

// turnOn 可能会存在多次调用的情况
func (g *GSESocket) turnOn() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.state {
		g.state = true
	}
}

// turnOff 会存在多 goroutine 多次调用的情况
// 这里我们只修改一次即可
func (g *GSESocket) turnOff() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state {
		g.state = false
	}
}

func (g *GSESocket) IsRunning() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state
}
