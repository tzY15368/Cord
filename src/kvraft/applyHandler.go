package kvraft

type ApplyHandler struct {
}

func NewApplyHandler() *ApplyHandler {
	return &ApplyHandler{}
}

// waitForMajorityOnIndex thread safe\
// todo: handle timeouts
func (ah *ApplyHandler) waitForMajorityOnIndex(index int) {

}
