SHELL := /bin/bash
PWD := $(shell pwd)

docker-image-build:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	
	docker build -f ./book_sanitizer/Dockerfile -t "book_sanitizer:latest" .
	docker build -f ./year_preprocessor/Dockerfile -t "year_preprocessor:latest" .
	docker build -f ./decade_preprocessor/Dockerfile -t "decade_preprocessor:latest" .
	docker build -f ./review_sanitizer/Dockerfile -t "review_sanitizer:latest" .
	docker build -f ./merger/Dockerfile -t "merger:latest" .

	docker build -f ./filter_of_books_by_year_and_genre/Dockerfile -t "filter_of_books_by_year_and_genre:latest" .
	docker build -f ./filter_of_books_by_title/Dockerfile -t "filter_of_books_by_title:latest" .
	docker build -f ./query1_result_generator/Dockerfile -t "query1_result_generator:latest" .
	
	docker build -f ./author_expander/Dockerfile -t "author_expander:latest" .
	docker build -f ./counter_of_decades_per_author/Dockerfile -t "counter_of_decades_per_author:latest" .
	docker build -f ./filter_of_authors_by_decade/Dockerfile -t "filter_of_authors_by_decade:latest" .
	docker build -f ./query2_result_generator/Dockerfile -t "query2_result_generator:latest" .

	docker build -f ./filter_of_compact_reviews_by_decade/Dockerfile -t "filter_of_compact_reviews_by_decade:latest" .
	docker build -f ./counter_of_reviews_per_book/Dockerfile -t "counter_of_reviews_per_book:latest" .
	docker build -f ./filter_of_books_by_review_count/Dockerfile -t "filter_of_books_by_review_count:latest" .
	docker build -f ./query3_result_generator/Dockerfile -t "query3_result_generator:latest" .

	docker build -f ./sorter_of_books_by_review_count/Dockerfile -t "sorter_of_books_by_review_count:latest" .
	docker build -f ./query4_result_generator/Dockerfile -t "query4_result_generator:latest" .

	docker build -f ./filter_of_merged_reviews_by_book_genre/Dockerfile -t "filter_of_merged_reviews_by_book_genre:latest" .
	docker build -f ./sentiment_analyzer/Dockerfile -t "sentiment_analyzer:latest" .
	docker build -f ./filter_of_books_by_sentiment_quantile/Dockerfile -t "filter_of_books_by_sentiment_quantile:latest" .
	docker build -f ./query5_result_generator/Dockerfile -t "query5_result_generator:latest" .

.PHONY: docker-image-build

docker-compose-up: docker-image-build
	docker compose -f docker-compose.yaml up -d --build --remove-orphans
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 3
	docker compose -f docker-compose.yaml down
	docker image prune -f
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

# ==============================================================================

## Temporary targets for testing purposes

test-docker-image-build:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	
	docker build -f ./book_sanitizer/Dockerfile -t "book_sanitizer:latest" .
	docker build -f ./year_preprocessor/Dockerfile -t "year_preprocessor:latest" .
	docker build -f ./decade_preprocessor/Dockerfile -t "decade_preprocessor:latest" .
	docker build -f ./review_sanitizer/Dockerfile -t "review_sanitizer:latest" .
	docker build -f ./merger/Dockerfile -t "merger:latest" .

	docker build -f ./filter_of_books_by_year_and_genre/Dockerfile -t "filter_of_books_by_year_and_genre:latest" .
	docker build -f ./filter_of_books_by_title/Dockerfile -t "filter_of_books_by_title:latest" .
	docker build -f ./query1_result_generator/Dockerfile -t "query1_result_generator:latest" .
	
	docker build -f ./author_expander/Dockerfile -t "author_expander:latest" .
	docker build -f ./counter_of_decades_per_author/Dockerfile -t "counter_of_decades_per_author:latest" .
	docker build -f ./filter_of_authors_by_decade/Dockerfile -t "filter_of_authors_by_decade:latest" .
	docker build -f ./query2_result_generator/Dockerfile -t "query2_result_generator:latest" .

	docker build -f ./filter_of_compact_reviews_by_decade/Dockerfile -t "filter_of_compact_reviews_by_decade:latest" .
	docker build -f ./counter_of_reviews_per_book/Dockerfile -t "counter_of_reviews_per_book:latest" .





test-compose-up: test-docker-image-build
	docker compose -f testing-compose.yaml up -d --build --remove-orphans
.PHONY: test-compose-up

test-compose-down:
	docker compose -f testing-compose.yaml stop -t 3
	docker compose -f testing-compose.yaml down
.PHONY: test-compose-down

test-compose-logs:
	docker compose -f testing-compose.yaml logs -f

# ==============================================================================