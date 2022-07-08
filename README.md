# Cord
A homebaked etcd as a graduation project.

The Original codebase was from mit/6.824, but RPCs and persisters are replaced with custom ones (gRPC + mmap) to enable actual deployements and reliably persistent logs

## Features
- High throughput Key-Value Store with go-memdb
- Lease with high consistency guarantees (with reference to actual etcd design, broadcasting lock expire event in raft logs)
- Separate Watch module with CDC (change data capture)
