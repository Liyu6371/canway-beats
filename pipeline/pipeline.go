package pipeline

import (
	"canway-beats/logger"
	"canway-beats/sender"
	"context"
	"sync"

	"github.com/mitchellh/mapstructure"
)

var (
	Name             = "pipeline"
	processorFactory = map[string]func() Processor{}
	shaperFactory    = map[string]func() Shaper{}
)

func RegistProcessor(name string, fn func() Processor) {
	processorFactory[name] = fn
}

func RegistShaper(name string, fn func() Shaper) {
	shaperFactory[name] = fn
}

type Processor interface {
	GetName() string
	Process(interface{}) (interface{}, error)
}

type Shaper interface {
	GetName() string
	Shape(interface{}) (sender.SenderMsg, error)
}

type pipeConf struct {
	Processors []string `mapstructure:"processors"`
	Shaper     string   `mapstructure:"shaper"`
}

type Pipeline struct {
	input  <-chan []byte
	output chan<- sender.SenderMsg

	ctx    context.Context
	cancel context.CancelFunc

	wg         sync.WaitGroup
	processors []Processor
	shaper     Shaper
}

func New(conf map[string]interface{}) *Pipeline {
	v, ok := conf[Name]
	if !ok {
		logger.Errorln("pipeline: config not found")
		return nil
	}
	c := pipeConf{}
	if err := mapstructure.Decode(v, &c); err != nil {
		logger.Errorf("pipeline: decode config error: %s", err)
		return nil
	}

	processors := []Processor{}
	for _, p := range c.Processors {
		fn, ok := processorFactory[p]
		if !ok {
			logger.Errorf("pipeline: processor %s not found, something error", p)
			return nil
		}
		processors = append(processors, fn())
	}

	if c.Shaper == "" {
		logger.Errorln("pipeline: shaper is not define")
		return nil
	}

	var shaper Shaper
	if fn, ok := shaperFactory[c.Shaper]; ok {
		shaper = fn()
	} else {
		logger.Errorf("pipeline: shaper %s not found", c.Shaper)
		return nil
	}

	return &Pipeline{
		wg:         sync.WaitGroup{},
		processors: processors,
		shaper:     shaper,
	}
}

func (p *Pipeline) RunPipe(ctx context.Context, input <-chan []byte, output chan<- sender.SenderMsg) {
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.input, p.output = input, output

	// 拉起一定数量的 worker 对 input 的数据进行消费
	for i := 0; i < 3; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case msg, ok := <-p.input:
					if !ok {
						return
					}
					p.process(msg)
				case <-p.ctx.Done():
					return
				}
			}
		}()
	}

}

func (p *Pipeline) process(msg interface{}) {
	var (
		err  error
		data interface{} = msg
	)
	for _, instance := range p.processors {
		data, err = instance.Process(data)
		if err != nil {
			logger.Errorf("pipeline: processor: %s process data error: %s", instance.GetName(), err)
			return
		}
	}
	senderMsg, err := p.shaper.Shape(data)
	if err != nil {
		logger.Errorf("pipeline: shaper: %s shape data error: %s", p.shaper.GetName(), err)
		return
	}
	p.output <- senderMsg
}
