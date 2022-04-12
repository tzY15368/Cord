package benches

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"testing"

	"6.824/proto"
)

func BenchmarkGOB(b *testing.B) {

	data := proto.RequestVoteArgsT{
		Term:   33333333,
		Lindex: 6666,
		Lterm:  999999,
		Can:    4,
	}
	gob.Register(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		err := encoder.Encode(&data)
		if err != nil {
			panic(err)
		}
	}

}

func BenchmarkGOGOPB(b *testing.B) {

	data := proto.RequestVoteArgsT{
		Term:   33333333,
		Lindex: 33333322,
		Lterm:  999999,
		Can:    4,
	}
	for i := 0; i < b.N; i++ {
		_, err := data.Marshal()
		if err != nil {
			panic(err)
		}
	}
}

var encodedData = proto.ServiceArgs{
	Cmds: []*proto.CmdArgs{
		{
			OpType: proto.CmdArgs_APPEND,
			OpKey:  "ahelad",
			OpVal:  "dfjskdlf",
			Ttl:    99999,
		},
		{
			OpType: proto.CmdArgs_APPEND,
			OpKey:  "ahelddad",
			OpVal:  "dfjsddkdlf",
			Ttl:    9997799,
		},
	},
	Info: &proto.RequestInfo{
		ClientID:  123,
		RequestID: 456,
	},
	Linearizable: false,
}

func BenchmarkIGOB(b *testing.B) {
	gob.Register(encodedData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		err := encoder.Encode(&encodedData)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkIGOGOPB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := encodedData.Marshal()
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkIJSON(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(encodedData)
		if err != nil {
			panic(err)
		}
	}
}
