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
      context: ./src/rabbitmq
      dockerfile: Dockerfile
    networks:
      - testing_net
    ports:
      - \"15672:15672\"
    healthcheck:
      test: rabbitmq-diagnostics check_port_connectivity
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 20s
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
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - SERVER_PORT=8080
      - INPUT_EXCHANGE=query_results_ex
      - INPUT_QUEUE_OF_QUERY_RESULTS=query_results_q
      - OUTPUT_EXCHANGE_OF_DATA=scraped_data_ex
      - OUTPUT_QUEUE_OF_REVIEWS=scraped_reviews_q
      - OUTPUT_QUEUE_OF_BOOKS=scraped_books_q
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> docker-compose.yaml
}

add_client() {
    echo "
  client:
    container_name: client
    image: client:latest
    entrypoint: python3 /main.py
    volumes:
      - ./data:/data
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - SERVER_IP=server
      - SERVER_PORT=8080
      - LOGGING_LEVEL=INFO
      - REVIEWS_FILE_PATH=/data/books_rating.csv
      - BOOKS_FILE_PATH=/data/books_data.csv
      - BATCH_SIZE=200
    networks:
      - testing_net
    depends_on:
      server:
        condition: service_started

  # ================================================================
" >> docker-compose.yaml
}


add_preprocessors() {
    echo "book_sanitizer" > src/controllers/health_checker/monitorable_controllers.txt
    echo "year_preprocessor" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "decade_preprocessor" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "review_sanitizer" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "
  book_sanitizer:
    container_name: book_sanitizer
    image: book_sanitizer:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=scraped_data_ex
      - OUTPUT_EXCHANGE=sanitized_books_ex
      - INPUT_QUEUE_OF_BOOKS=scraped_books_q
      - OUTPUT_QUEUE_OF_BOOKS=sanitized_books_q
      - WORKER_NAME=book_sanitizer
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  year_preprocessor:
    container_name: year_preprocessor
    image: year_preprocessor:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=sanitized_books_ex
      - OUTPUT_EXCHANGE=preprocessed_books_with_year_ex
      - INPUT_QUEUE_OF_BOOKS=sanitized_books_q
      - OUTPUT_QUEUE_OF_BOOKS_TOWARDS_PREPROC=towards_preprocessor__preprocessed_books_with_year_q
      - OUTPUT_QUEUE_OF_BOOKS_TOWARDS_FILTER=towards_filter__preprocessed_books_with_year_q
      - WORKER_NAME=year_preprocessor
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> docker-compose.yaml


    echo "
  decade_preprocessor:
    container_name: decade_preprocessor
    image: decade_preprocessor:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=preprocessed_books_with_year_ex
      - OUTPUT_EXCHANGE=preprocessed_books_with_decade_ex
      - INPUT_QUEUE_OF_BOOKS=towards_preprocessor__preprocessed_books_with_year_q
      - OUTPUT_QUEUE_OF_BOOKS_TOWARDS_EXPANDER=towards_expander__preprocessed_books_with_decade_q" >> docker-compose.yaml
    for ((i=1; i<=$WORKERS; i++)); do
        echo "      - OUTPUT_QUEUE_OF_BOOKS_$i=preprocessed_books_with_decade_q_$i" >> docker-compose.yaml
    done
    echo "      - NUM_OF_DYN_OUTPUT_QUEUES=$WORKERS
      - WORKER_NAME=decade_preprocessor
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy


  review_sanitizer:
    container_name: review_sanitizer
    image: review_sanitizer:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=scraped_data_ex
      - OUTPUT_EXCHANGE=sanitized_reviews_ex
      - INPUT_QUEUE_OF_REVIEWS=scraped_reviews_q" >> docker-compose.yaml
      for ((i=1; i<=$WORKERS; i++)); do
          echo "      - OUTPUT_QUEUE_OF_REVIEWS_$i=sanitized_reviews_q_$i" >> docker-compose.yaml
      done
      echo "      - NUM_OF_DYN_OUTPUT_QUEUES=$WORKERS
      - WORKER_NAME=review_sanitizer
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> docker-compose.yaml
}

add_mergers() {
    for ((i=1; i<=$WORKERS; i++)); do
        echo "merger_$i" >> src/controllers/health_checker/monitorable_controllers.txt
        echo "
  merger_$i:
    container_name: merger_$i
    image: merger:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE_OF_REVIEWS=sanitized_reviews_ex
      - INPUT_EXCHANGE_OF_BOOKS=preprocessed_books_with_decade_ex
      - OUTPUT_EXCHANGE=merged_reviews_ex
      - INPUT_QUEUE_OF_REVIEWS=sanitized_reviews_q_$i
      - INPUT_QUEUE_OF_BOOKS=preprocessed_books_with_decade_q_$i
      - OUTPUT_QUEUE_OF_COMPACT_REVIEWS=merged_compact_reviews_q
      - OUTPUT_QUEUE_OF_FULL_REVIEWS=merged_full_reviews_q
      - WORKER_NAME=merger_$i
      - WORKER_ID=$i
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy" >> docker-compose.yaml
    done
    echo "
  # ================================================================
" >> docker-compose.yaml
}


add_query1_processes() {
    echo "filter_of_books_by_year_and_genre" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "filter_of_books_by_title" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "query1_result_generator" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "
  filter_of_books_by_year_and_genre:
    container_name: filter_of_books_by_year_and_genre
    image: filter_of_books_by_year_and_genre:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=preprocessed_books_with_year_ex
      - OUTPUT_EXCHANGE=books_filtered_by_year_and_genre_ex
      - INPUT_QUEUE_OF_BOOKS=towards_filter__preprocessed_books_with_year_q
      - OUTPUT_QUEUE_OF_BOOKS=books_filtered_by_year_and_genre_q
      - MIN_YEAR=2000
      - MAX_YEAR=2023
      - GENRE=Computers
      - WORKER_NAME=filter_of_books_by_year_and_genre
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
      
  filter_of_books_by_title:
    container_name: filter_of_books_by_title
    image: filter_of_books_by_title:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=books_filtered_by_year_and_genre_ex
      - OUTPUT_EXCHANGE=books_filtered_by_title_ex
      - INPUT_QUEUE_OF_BOOKS=books_filtered_by_year_and_genre_q
      - OUTPUT_QUEUE_OF_BOOKS=books_filtered_by_title_q
      - TITLE_KEYWORD=distributed
      - WORKER_NAME=filter_of_books_by_title
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  query1_result_generator:
    container_name: query1_result_generator
    image: query1_result_generator:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=books_filtered_by_title_ex
      - OUTPUT_EXCHANGE=query_results_ex
      - INPUT_QUEUE_OF_BOOKS=books_filtered_by_title_q
      - OUTPUT_QUEUE_OF_QUERY=query_results_q
      - WORKER_NAME=query1_result_generator
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  # ================================================================
" >> docker-compose.yaml
}


add_query2_processes() {
    echo "author_expander" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "
  author_expander:
    container_name: author_expander
    image: author_expander:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=preprocessed_books_with_decade_ex
      - OUTPUT_EXCHANGE=expanded_authors_ex
      - INPUT_QUEUE_OF_BOOKS=towards_expander__preprocessed_books_with_decade_q" >> docker-compose.yaml
      for ((i=1; i<=$WORKERS; i++)); do
          echo "      - OUTPUT_QUEUE_OF_AUTHORS_$i=expanded_authors_q_$i" >> docker-compose.yaml
      done
      echo "      - NUM_OF_DYN_OUTPUT_QUEUES=$WORKERS
      - WORKER_NAME=author_expander
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> docker-compose.yaml

    for ((i=1; i<=$WORKERS; i++)); do
        echo "counter_of_decades_per_author_$i" >> src/controllers/health_checker/monitorable_controllers.txt
        echo "
  counter_of_decades_per_author_$i:
    container_name: counter_of_decades_per_author_$i
    image: counter_of_decades_per_author:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=expanded_authors_ex
      - OUTPUT_EXCHANGE=authors_decades_count_ex
      - INPUT_QUEUE_OF_AUTHORS=expanded_authors_q_$i
      - OUTPUT_QUEUE_OF_AUTHORS=authors_decades_count_q_$i
      - WORKER_NAME=counter_of_decades_per_author_$i
      - WORKER_ID=$i
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy" >> docker-compose.yaml
    done

    for ((i=1; i<=$WORKERS; i++)); do
        echo "filter_of_authors_by_decade_$i" >> src/controllers/health_checker/monitorable_controllers.txt
        echo "
  filter_of_authors_by_decade_$i:
    container_name: filter_of_authors_by_decade_$i
    image: filter_of_authors_by_decade:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=authors_decades_count_ex
      - OUTPUT_EXCHANGE=authors_filtered_by_decade_ex
      - INPUT_QUEUE_OF_AUTHORS=authors_decades_count_q_$i
      - OUTPUT_QUEUE_OF_AUTHORS=authors_filtered_by_decade_q
      - MIN_DECADES_TO_FILTER=10
      - WORKER_NAME=filter_of_authors_by_decade_$i
      - WORKER_ID=$i
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy" >> docker-compose.yaml
    done
    
    echo "query2_result_generator" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "
  query2_result_generator:
    container_name: query2_result_generator
    image: query2_result_generator:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=authors_filtered_by_decade_ex
      - OUTPUT_EXCHANGE=query_results_ex
      - INPUT_QUEUE_OF_AUTHORS=authors_filtered_by_decade_q
      - OUTPUT_QUEUE_OF_QUERY=query_results_q
      - FILTERS_QUANTITY=$WORKERS
      - WORKER_NAME=query2_result_generator
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  # ================================================================
" >> docker-compose.yaml
}


add_query3_processes() {
    echo "filter_of_compact_reviews_by_decade" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "
  filter_of_compact_reviews_by_decade:
    container_name: filter_of_compact_reviews_by_decade
    image: filter_of_compact_reviews_by_decade:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=merged_reviews_ex
      - OUTPUT_EXCHANGE=compact_reviews_filtered_by_decade_ex
      - INPUT_QUEUE_OF_REVIEWS=merged_compact_reviews_q
      - NUM_OF_INPUT_WORKERS=$WORKERS
      - DECADE_TO_FILTER=1990" >> docker-compose.yaml
      for ((i=1; i<=$WORKERS; i++)); do
          echo "      - OUTPUT_QUEUE_OF_REVIEWS_$i=compact_reviews_filtered_by_decade_q_$i" >> docker-compose.yaml
      done
      echo "      - NUM_OF_DYN_OUTPUT_QUEUES=$WORKERS
      - WORKER_NAME=filter_of_compact_reviews_by_decade
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> docker-compose.yaml

    for ((i=1; i<=$WORKERS; i++)); do
        echo "counter_of_reviews_per_book_$i" >> src/controllers/health_checker/monitorable_controllers.txt
        echo "
  counter_of_reviews_per_book_$i:
    container_name: counter_of_reviews_per_book_$i
    image: counter_of_reviews_per_book:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=compact_reviews_filtered_by_decade_ex
      - OUTPUT_EXCHANGE=review_count_per_book_ex
      - INPUT_QUEUE_OF_REVIEWS=compact_reviews_filtered_by_decade_q_$i
      - OUTPUT_QUEUE_OF_REVIEWS=review_count_per_book_q
      - WORKER_NAME=counter_of_reviews_per_book_$i
      - WORKER_ID=$i
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy" >> docker-compose.yaml
    done

    echo "filter_of_books_by_review_count" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "query3_result_generator" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "
  filter_of_books_by_review_count:
    container_name: filter_of_books_by_review_count
    image: filter_of_books_by_review_count:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=review_count_per_book_ex
      - OUTPUT_EXCHANGE=books_filtered_by_review_count_ex
      - INPUT_QUEUE_OF_BOOKS=review_count_per_book_q
      - OUTPUT_QUEUE_OF_BOOKS_TOWARDS_QUERY3=towards_query3__books_filtered_by_review_count_q
      - OUTPUT_QUEUE_OF_BOOKS_TOWARDS_sorter=towards_sorter__books_filtered_by_review_count_q
      - NUM_OF_COUNTERS=$WORKERS
      - MIN_REVIEWS=500
      - WORKER_NAME=filter_of_books_by_review_count
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  query3_result_generator:
    container_name: query3_result_generator
    image: query3_result_generator:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=books_filtered_by_review_count_ex
      - OUTPUT_EXCHANGE=query_results_ex
      - INPUT_QUEUE_OF_BOOKS=towards_query3__books_filtered_by_review_count_q
      - OUTPUT_QUEUE_OF_QUERY=query_results_q
      - WORKER_NAME=query3_result_generator
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  # ================================================================
" >> docker-compose.yaml
}


add_query4_processes() {
    echo "sorter_of_books_by_review_count" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "query4_result_generator" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "
  sorter_of_books_by_review_count:
    container_name: sorter_of_books_by_review_count
    image: sorter_of_books_by_review_count:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=books_filtered_by_review_count_ex
      - OUTPUT_EXCHANGE=top_books_by_review_count_ex
      - INPUT_QUEUE_OF_BOOKS=towards_sorter__books_filtered_by_review_count_q
      - OUTPUT_QUEUE_OF_BOOKS=top_books_by_review_count_q
      - TOP_OF_BOOKS=10
      - WORKER_NAME=sorter_of_books_by_review_count
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  query4_result_generator:
    container_name: query4_result_generator
    image: query4_result_generator:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=top_books_by_review_count_ex
      - OUTPUT_EXCHANGE=query_results_ex
      - INPUT_QUEUE_OF_BOOKS=top_books_by_review_count_q
      - OUTPUT_QUEUE_OF_QUERY=query_results_q
      - WORKER_NAME=query4_result_generator
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  # ================================================================
" >> docker-compose.yaml
}


add_query5_processes() {
    echo "filter_of_merged_reviews_by_book_genre" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "sentiment_analyzer" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "filter_of_books_by_sentiment_quantile" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "query5_result_generator" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "
  filter_of_merged_reviews_by_book_genre:
    container_name: filter_of_merged_reviews_by_book_genre
    image: filter_of_merged_reviews_by_book_genre:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=merged_reviews_ex
      - OUTPUT_EXCHANGE=reviews_filtered_by_book_genre_ex
      - INPUT_QUEUE_OF_REVIEWS=merged_full_reviews_q
      - OUTPUT_QUEUE_OF_REVIEWS=reviews_filtered_by_book_genre_q
      - GENRE=fiction
      - NUM_OF_INPUT_WORKERS=$WORKERS
      - WORKER_NAME=filter_of_merged_reviews_by_book_genre
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  sentiment_analyzer:
    container_name: sentiment_analyzer
    image: sentiment_analyzer:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=reviews_filtered_by_book_genre_ex
      - OUTPUT_EXCHANGE=sentiment_per_book_ex
      - INPUT_QUEUE_OF_REVIEWS=reviews_filtered_by_book_genre_q
      - OUTPUT_QUEUE_OF_REVIEWS=sentiment_per_book_q
      - WORKER_NAME=sentiment_analyzer
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  filter_of_books_by_sentiment_quantile:
    container_name: filter_of_books_by_sentiment_quantile
    image: filter_of_books_by_sentiment_quantile:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=sentiment_per_book_ex
      - OUTPUT_EXCHANGE=books_filtered_by_highest_sentiment_ex
      - INPUT_QUEUE_OF_BOOKS=sentiment_per_book_q
      - OUTPUT_QUEUE_OF_BOOKS=books_filtered_by_highest_sentiment_q
      - QUANTILE=0.9
      - WORKER_NAME=filter_of_books_by_sentiment_quantile
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy

  query5_result_generator:
    container_name: query5_result_generator
    image: query5_result_generator:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - INPUT_EXCHANGE=books_filtered_by_highest_sentiment_ex
      - OUTPUT_EXCHANGE=query_results_ex
      - INPUT_QUEUE_OF_BOOKS=books_filtered_by_highest_sentiment_q
      - OUTPUT_QUEUE_OF_QUERY=query_results_q
      - WORKER_NAME=query5_result_generator
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
        
  # ================================================================
" >> docker-compose.yaml
}

add_health_checkers(){
  for ((i=1; i<=$HEALTH_CHECKERS; i++)); do
    echo "health_checker_$i" >> src/controllers/health_checker/monitorable_controllers.txt
    echo "
  health_checker_$i:
    container_name: health_checker_$i
    image: health_checker:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONHASHSEED=1
      - LOGGING_LEVEL=INFO
      - HEALTH_CHECK_INTERVAL=5
      - HEALTH_CHECK_TIMEOUT=1
      - WORKER_NAME=health_checker_$i
      - WORKER_ID=$i
    networks:
      - testing_net
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
      server:
        condition: service_started" >> docker-compose.yaml
  done
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
    # WORKERS: Number of workers or replicas per process.
    # HEALTH_CHECKERS: Number of [Health Checker] processes.

    if [ -z "$WORKERS" ]; then
        echo "Using default values for WORKERS=1"
        export WORKERS=1
    fi

    if [ -z "$HEALTH_CHECKERS" ]; then
        echo "Using default values for HEALTH_CHECKERS=1"
        export HEALTH_CHECKERS=1
    fi
    
    local MAX_WORKERS=5
    if [ $WORKERS -gt $MAX_WORKERS ] || [ $HEALTH_CHECKERS -gt $MAX_WORKERS ] ; then
        echo "The maximum number of workers is $MAX_WORKERS"
        exit 1
    fi

}



# =========================================================================


check_params
add_compose_header
add_rabbitmq
add_server
add_client
add_preprocessors
add_mergers
add_query1_processes
add_query2_processes
add_query3_processes
add_query4_processes
add_query5_processes
add_health_checkers
add_network

echo ">>>   Successfully generated docker-compose.yaml   <<<"