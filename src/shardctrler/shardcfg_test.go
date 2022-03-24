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
		// TODO: Add test cases.
		{
			name: "1",
			cfg: &Config{
				Num: 1,
				Shards: [NShards]int{
					1, 1, 1, 1, 1, 1, 1, 2, 3, 4,
				},
				Groups: map[int][]string{
					1: {"x", "y"},
					2: {"a", "b"},
					3: {"c", "d"},
					4: {"f", "e"},
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
