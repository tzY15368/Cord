package main

import (
	"6.824/clerk"
	"6.824/repl"
)

func main() {
	servers := []string{
		"127.0.0.1:7500",
		"127.0.0.1:7501",
		"127.0.0.1:7502",
	}
	cs := clerk.NewClerk(servers)
	repl.RunREPL(cs)
}
