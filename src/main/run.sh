rm -f map-out-* ; rm -f mr-out-*;rm -f log.txt;go build -race -buildmode=plugin ../mrapps/wc.go &&  go run -race mrcoordinator.go pg-*.txt
go run -race mrworker.go wc.so