package sender

import (
	"canway-beats/config"
	"canway-beats/logger"
	"context"
	"sync"
	"time"
)

var (
	ctrlWorker    = 3
	checkInterval = time.Minute * 5
	senderFactory map[string]func(map[string]interface{}) Sender
)

// RegistSender 注册发送者
func RegistSender(name string, fc func(map[string]interface{}) Sender) {
	senderFactory[name] = fc
}

// Sender 抽象
type Sender interface {
	// 推送数据到 sender 自有的 pipeline
	Push(SenderMsg)
	// 启动 Sender
	RunSender(context.Context) error
	// 停止 Sender
	StopSender()
	// 判断是否正在运行
	IsRunning() bool
	// 返回 sender 的名字
	GetName() string
}

// SenderMsg 抽象
type SenderMsg interface {
	// 获取 Sender 名
	GetSender() string
	// 获取需要发送的数据
	GetData() []byte
	// 补充信息 可以是 string 也可以是数字类型
	Where() interface{}
}

// SenderCtrl 一个控制器可以拉起并管理多个 Sender 实例
type SenderCtrl struct {
	senders []string
	wg      sync.WaitGroup
	mu      sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	ch       chan SenderMsg
	factory  map[string]func(map[string]interface{}) Sender
	instance map[string]Sender
}

// NewSenderCtrl 控制器，负责根据传入的配置拉起并且管理 sender 实例
// 对于写入 ch 的 senderMsg 进行分发操作
func NewSenderCtrl(senders []string) *SenderCtrl {
	if len(senders) == 0 {
		logger.Warn("senderCtrl: sender list is empty, nothing to do")
		return nil
	}

	if len(senderFactory) == 0 {
		logger.Warn("senderCtrl: sender factory is empty, nothing to do")
		return nil
	}

	return &SenderCtrl{
		senders:  senders,
		wg:       sync.WaitGroup{},
		mu:       sync.RWMutex{},
		ch:       make(chan SenderMsg, 2000),
		factory:  senderFactory,
		instance: make(map[string]Sender),
	}
}

func (s *SenderCtrl) RunSenderCtrl(parent context.Context) {
	s.ctx, s.cancel = context.WithCancel(parent)
	// 确保在拉起 sender 实例失败后也能正常释放资源
	defer func() {
		close(s.ch)
		s.cancel()
	}()
	// 尝试拉起 sender 实例，对于找不到的 sender 实例则直接返回错误信息
	for _, name := range s.senders {
		// 确保所需的 sender 功能可以在 sender 工厂中找到
		fn, ok := s.factory[name]
		if !ok {
			logger.Errorf("senderCtrl: sender: %s not found, maybe something wrong?", name)
			return
		}
		// 确保能够正确的 new 一个具体的 sender 实例
		sender := fn(config.GetConf().Sender)
		if sender == nil {
			logger.Errorf("senderCtrl: new sender: %s failed", name)
			return
		}
		// 确保将 sender 实例直接拉起
		if err := sender.RunSender(s.ctx); err != nil {
			logger.Errorf("senderCtrl: run sender: %s, error: %s", name, err)
			return
		}
		// 记录正在运行的 sender 实例
		s.mu.Lock()
		s.instance[name] = sender
		s.mu.Unlock()
	}
	s.wg.Add(1)
	go s.check()

	// 拉起分发消息的 goroutine
	for i := 0; i < ctrlWorker; i++ {
		s.wg.Add(1)
		go s.dispatch(i)
	}

	// 监听 ctx 取消的信号
	// ctx 信号传递是由上到下，因此不用显示的通知所有 sender instance 结束
	<-parent.Done()
	s.wg.Wait()
	logger.Infoln("senderCtrl: controller exit")
}

// dispatch 将 channel 内部的消息进行分发
func (s *SenderCtrl) dispatch(idx int) {
	defer s.wg.Done()
	for {
		select {
		case msg, ok := <-s.ch:
			if !ok {
				logger.Infof("senderCtrl: channel is closed, dispatch goroutine %d exit", idx)
				return
			}
			sender, ok := s.instance[msg.GetSender()]
			if !ok {
				logger.Errorf("senderCtrl: sender instance: %s not found, dropped msg %s", msg.GetSender(), msg.GetData())
			} else {
				sender.Push(msg)
				logger.Debugf("senderCtrl: pushed msg: %s to sender instance: %s", msg.GetData(), msg.GetSender())
			}
		case <-s.ctx.Done():
			logger.Infof("senderCtrl: dispatch goroutine %d exit", idx)
			return
		}
	}
}

func (s *SenderCtrl) check() {
	defer s.wg.Done()
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// TODO 暂时只做了日志的输出功能
			// 后续可能要做 s.instance 这块内容的变更，到时候需要更换锁的状态
			s.mu.RLock()
			for name, sender := range s.instance {
				if !sender.IsRunning() {
					logger.Errorf("senderCtrl: sender instance %s is not running", name)
				}
			}
			s.mu.RUnlock()
		case <-s.ctx.Done():
			logger.Infoln("senderCtrl: check goroutine exit")
			return
		}
	}
}

// Push pipeline 调用此方法将处理好的数据传入
func (s *SenderCtrl) Push(msg SenderMsg) {
	s.ch <- msg
}
