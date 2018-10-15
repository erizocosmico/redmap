#!/usr/bin/env bash

set -e
echo "" > coverage.txt

CGO_ENABLED=0 go build -buildmode=plugin -o ./_testdata/job ./_testdata/job.go
CGO_ENABLED=0 go build -buildmode=plugin -o ./_testdata/reduce_error ./_testdata/reduce_error.go
for d in $(go list ./... | grep -v vendor); do
    go test -race -coverprofile=profile.out -covermode=atomic $d
    if [ -f profile.out ]; then
        cat profile.out >> coverage.txt
        rm profile.out
    fi
done
rm -f ./_testdata/job ./_testdata/reduce_error
