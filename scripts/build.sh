export GOPATH=$GOPATH:$(pwd)

if [ ! -f "sample_logs/sample.log" ]; then
    sh ./scripts/download_sample_logs.sh
fi

go build src/client/client.go
go build src/server/server.go
