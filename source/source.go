package source

import (
	"canway-beats/config"
	"canway-beats/logger"
	"context"
	"sync"
	"time"
)

type Source interface {
	// 获取 source instance 的名字
	GetName() string
	// 启动 source instance 并且在启动的时候传递 channel 给到 source instance
	StartSource(context.Context, chan<- []byte) error
	// 判断是否正在运行
	IsRunning() bool
	// 停止 source instance
	StopSource()
}

var (
	checkInterval = time.Minute * 5
	sourceFactory = map[string]func(map[string]interface{}) Source{}
)

func RegistSource(name string, fn func(map[string]interface{}) Source) {
	sourceFactory[name] = fn
}

type SourceCtrl struct {
	sourcies []string

	ctx    context.Context
	cancel context.CancelFunc

	wg       sync.WaitGroup
	mu       sync.RWMutex
	ch       chan []byte
	factory  map[string]func(map[string]interface{}) Source
	instance map[string]Source
}

func NewSourceCtrl(sourcies []string) *SourceCtrl {
	if len(sourcies) == 0 {
		logger.Warn("sourceCtrl: source list is empty, nothing to do")
		return nil
	}
	if len(sourceFactory) == 0 {
		logger.Warn("sourceCtrl: source factory is empty, nothing to do")
		return nil
	}
	return &SourceCtrl{
		sourcies: sourcies,
		wg:       sync.WaitGroup{},
		mu:       sync.RWMutex{},
		ch:       make(chan []byte, 2000),
		factory:  sourceFactory,
		instance: make(map[string]Source),
	}
}

func (s *SourceCtrl) RunSourceCtrl(parent context.Context) {
	s.ctx, s.cancel = context.WithCancel(parent)
	defer func() {
		close(s.ch)
		s.cancel()
	}()

	// 开始拉起所有定义的 source 实例
	for _, name := range s.sourcies {
		fn, ok := s.factory[name]
		if !ok {
			logger.Errorf("sourceCtrl: source %s not found", name)
			return
		}
		source := fn(config.GetConf().Source)
		if source == nil {
			logger.Errorf("sourceCtrl: new source: %s failed", name)
			return
		}
		if err := source.StartSource(s.ctx, s.ch); err != nil {
			logger.Errorf("sourceCtrl: start source %s error: %s", name, err)
			return
		}
		// 记录成功拉起的 source 实例
		s.mu.Lock()
		s.instance[name] = source
		s.mu.Unlock()
	}

	s.wg.Add(1)
	go s.check()
	<-parent.Done()
	logger.Infoln("sourceCtrl: catch exit signal")
	s.wg.Wait()
	logger.Infoln("sourceCtrl: source controller exit")
}

func (s *SourceCtrl) check() {
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
					logger.Errorf("sourceCtrl: source instance %s is not running", name)
				}
			}
			s.mu.RUnlock()
		case <-s.ctx.Done():
			logger.Infoln("sourceCtrl: check goroutine exit")
			return
		}
	}
}

// // GetChannel 对外暴露 channel 用于给 pipeline 消费
// func (s *SourceCtrl) GetChannel() <-chan []byte {
// 	return s.ch
// }

// // Push source instance 通过此方法将数据传入 channel
// func (s *SourceCtrl) Push(msg []byte) {
// 	s.ch <- msg
// }
