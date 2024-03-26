SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client
.PHONY: build

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build --remove-orphans
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs


build-netcat-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./netcat-sv-tester/Dockerfile -t "netcat-sv-tester:latest" .

netcat-sv-test-up: build-netcat-image
	docker compose -f docker-compose-sv-test.yaml up -d --build --remove-orphans

netcat-sv-test-down:
	docker compose -f docker-compose-sv-test.yaml stop -t 1
	docker compose -f docker-compose-sv-test.yaml down

netcat-sv-test-logs:
	docker compose -f docker-compose-sv-test.yaml logs -f
