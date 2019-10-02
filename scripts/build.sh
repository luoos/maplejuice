export GOPATH=$GOPATH:$(pwd)

if [ $1 == 'mp1' ]
then
    go get github.com/fatih/color
    go build src/client/client.go
    go build src/server/server.go
elif [ $1 == 'mp2' ]
then
    go build src/node/node.go
fi
