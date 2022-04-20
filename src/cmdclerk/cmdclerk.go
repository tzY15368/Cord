package main

import (
	"6.824/clerk"
	"6.824/repl"
)

func main() {
	servers := []string{
		"10.9.116.177:7500",
		"10.9.116.177:7501",
		"10.9.116.177:7502",
	}
	cs := clerk.NewClerk(servers)
	repl.RunREPL(cs)
}
