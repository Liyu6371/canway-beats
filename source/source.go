package source

import "context"

type SourceServer interface {
	GetName() string
	IsRunning() bool
	Run(context.Context) error
	Stop()
	GetSourceChannel() <-chan []byte
}
