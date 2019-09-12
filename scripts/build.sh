export GOPATH=$GOPATH:$(pwd)

go get github.com/fatih/color

go build src/client/client.go
go build src/server/server.go
