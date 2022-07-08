# Cord
A homebaked etcd as a graduation project

## Features
- High throughput Key-Value Store with go-memdb
- Lease with high consistency guarantees (with reference to actual etcd design, broadcasting lock expire event in raft logs)
- Separate Watch module with CDC (change data capture)
