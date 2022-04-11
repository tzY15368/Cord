package flowctl

// 目标是限制一秒内的调用数
// start依旧得在写log后返回

// 思路：对请求进行采样，合并多个proto.ServiceArgs，一次提交？
// 后面返回的kv.evalResult需要变
// 不能保证真的有性能提升

type FlowCTL struct {
	maxConcurrency int64
}

func (fc *FlowCTL) SetMaxConcurrency(state int64) {
	fc.maxConcurrency = state
}
