package mr

import (
	"crypto/md5"
	"encoding/hex"

	"github.com/google/uuid"
)

func md5V(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}
func genID() string {
	return md5V(uuid.New().String())
}
