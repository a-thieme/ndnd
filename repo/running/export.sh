#!/bin/bash
go build -o running/bin/repo cmd/main.go
go build -o running/bin/producer test/test_producer.go
