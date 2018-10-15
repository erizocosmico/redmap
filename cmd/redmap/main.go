package main

import cli "gopkg.in/src-d/go-cli.v0"

var (
	version string
	build   string
)

var app = cli.New("redmap", version, build, "redmap command line interface")

func main() {
	app.RunMain()
}
