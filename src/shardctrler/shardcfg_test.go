package shardctrler

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func TestConfig_rebalance(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
	}{
		{
			name: "normal",
			cfg: &Config{
				Num:    0,
				Shards: [NShards]int{1, 1, 1, 1, 1, 1, 2, 2, 3, 4},
				Groups: map[int][]string{
					1: {},
					2: {},
					3: {},
					4: {},
				},
			},
		},
		{
			name: "skew",
			cfg: &Config{
				Num: 1,
				Shards: [NShards]int{
					1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
				},
				Groups: map[int][]string{
					1: {"x", "y"},
					2: {"a", "b"},
				},
			},
		},
		{
			name: "join",
			cfg: &Config{
				Num: 1,
				Shards: [NShards]int{
					0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				},
				Groups: map[int][]string{
					1: {"x", "y"},
				},
			},
		},
		{
			name: "leave",
			cfg: &Config{
				Num: 1,
				Shards: [NShards]int{
					1, 1, 1, 1, 1, 2, 2, 2, 2,
				},
				Groups: map[int][]string{
					1: {"x", "y"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.rebalance()
			tt.cfg.dump(logrus.WithField("", "")).Warn("out")
		})
	}
}
