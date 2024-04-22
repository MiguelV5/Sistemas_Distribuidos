#!/bin/bash

add_compose_header() {
    echo "name: tp1
services:
" > docker-compose.yaml
}

add_rabbitmq() {
    echo "
  rabbitmq:
    container_name: rabbitmq
    build:
      context: ./rabbitmq
      dockerfile: Dockerfile
    networks:
      - testing_net
    ports:
      - "15672:15672"
    healthcheck:
      test: rabbitmq-diagnostics check_port_connectivity
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 40s
" >> docker-compose.yaml
}

add_server() {
    echo "  
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
      - REVIEWS_OUTPUT_QUEUE=scraped_reviews_q
      - BOOKS_OUTPUT_QUEUE=scraped_books_q
      - QUERY1_INPUT_QUEUE=query1_result_q
      - QUERY2_INPUT_QUEUE=query2_result_q
      - QUERY3_INPUT_QUEUE=query3_result_q
      - QUERY4_INPUT_QUEUE=query4_result_q
      - QUERY5_INPUT_QUEUE=query5_result_q
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> docker-compose.yaml
}

add_client() {
    # TODO
}

add_network() {
    echo "
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
" >> docker-compose.yaml
}

check_params() {
    # Check if env vars are set and non-empty
    # C_DEC_PA_WORKERS: Number of [Counters of Decades Per Author] Processes
    # C_REV_PB_WORKERS: Number of [Counters of Reviews Per Book] Processes
    # MERGER_WORKERS: Number of [Merger] Processes
    if [ -z "$C_DEC_PA_WORKERS" ]; then
        echo "Using default values for C_DEC_PA_WORKERS=1"
        export C_DEC_PA_WORKERS=1
    fi

    if [ -z "$C_REV_PB_WORKERS" ]; then
        echo "Using default values for C_REV_PB_WORKERS=1"
        export C_REV_PB_WORKERS=1
    fi

    if [ -z "$MERGER_WORKERS" ]; then
        echo "Using default values for MERGER_WORKERS=1"
        export MERGER_WORKERS=1
    fi
}



# =========================================================================


check_params
add_compose_header
add_rabbitmq
add_server
add_client

# TODO

add_network

echo
echo "Successfully generated docker-compose.yaml"