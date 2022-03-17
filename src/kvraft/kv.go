package kvraft

import (
	"errors"
	"sync"

	"github.com/sirupsen/logrus"
)

var ErrKeyNotFound = errors.New("kvstore: errkeynotfound")

type KVInterface interface {
	Put(string, string) error
	Get(string) (string, error)
	Append(string, string) error
	HandleError(error, ReplyInterface, string)
}

type SimpleKVStore struct {
	data   map[string]string
	mu     sync.Mutex
	logger *logrus.Entry
}

func NewKVStore(logger *logrus.Entry) KVInterface {
	sks := &SimpleKVStore{
		data:   make(map[string]string),
		logger: logger,
	}
	logger.Debug("kvstore: started kvstore")
	return sks
}

func (sk *SimpleKVStore) HandleError(err error, replier ReplyInterface, errorSuffix string) {
	if replier == nil {
		return
	}
	replier.SetReplyErr(OK)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			replier.SetReplyErr(ErrNoKey)
		} else {
			sk.logger.WithError(err).Error(errorSuffix)
			replier.SetReplyErr(ErrUnexpected)
		}
	}
}

func (sk *SimpleKVStore) Get(key string) (string, error) {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	data, ok := sk.data[key]
	if ok {
		return data, nil
	}
	return "", ErrKeyNotFound
}

func (sk *SimpleKVStore) Put(key string, value string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	sk.data[key] = value
	sk.logger.WithField("key", key).WithField("val", value).Debug("kvstore: put")
	return nil
}

func (sk *SimpleKVStore) Append(key string, value string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	// todo: 这里如果不存在key，应该报错还是依旧append？
	data, ok := sk.data[key]
	if ok {
		sk.data[key] = data + value
		sk.logger.WithFields(logrus.Fields{
			"old": data,
			"new": sk.data[key],
		}).Debug("kvstore: append: diff")
		return nil
	} else {
		return ErrKeyNotFound
	}
}
