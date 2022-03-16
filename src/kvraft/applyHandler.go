package kvraft

type ApplyHandler struct {
}

func NewApplyHandler() *ApplyHandler {
	return &ApplyHandler{}
}

// 之前的queue估计也要提到外面来

// waitForMajorityOnIndex thread safe
// todo: handle timeouts
func (ah *ApplyHandler) waitForMajorityOnIndex(index int) {

}
