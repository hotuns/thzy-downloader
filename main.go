package main

import (
	"thzy/downloader/cmd"
)

func main() {
	cmd.Execute()
}

// GOOS=windows GOARCH=amd64 go build -o downloader.exe main.go
