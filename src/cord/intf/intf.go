package intf

type IEvalResult interface {
	GetClientID() int64
	GetRequestID() int64
	GetError() error
	GetData() map[string]string
	AwaitWatches() map[string]string
}
