package output

import "context"

type OutputServer interface {
	GetName() string
	IsRunning() bool
	Run(context.Context) error
	Stop()
	GetOutputChannel() chan<- interface{}
}
