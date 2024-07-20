SHELL := /bin/bash
PWD := $(shell pwd)

docker-image-build:
	docker build -f ./src/server/Dockerfile -t "server:latest" .
	docker build -f ./src/client/Dockerfile -t "client:latest" .
	
	docker build -f ./src/controllers/preprocessors/book_sanitizer/Dockerfile -t "book_sanitizer:latest" .
	docker build -f ./src/controllers/preprocessors/year_preprocessor/Dockerfile -t "year_preprocessor:latest" .
	docker build -f ./src/controllers/preprocessors/decade_preprocessor/Dockerfile -t "decade_preprocessor:latest" .
	docker build -f ./src/controllers/preprocessors/review_sanitizer/Dockerfile -t "review_sanitizer:latest" .
	
	docker build -f ./src/controllers/merger/Dockerfile -t "merger:latest" .

	docker build -f ./src/controllers/filters/filter_of_books_by_year_and_genre/Dockerfile -t "filter_of_books_by_year_and_genre:latest" .
	docker build -f ./src/controllers/filters/filter_of_books_by_title/Dockerfile -t "filter_of_books_by_title:latest" .
	docker build -f ./src/controllers/sinks/query1_result_generator/Dockerfile -t "query1_result_generator:latest" .
	
	docker build -f ./src/controllers/preprocessors/author_expander/Dockerfile -t "author_expander:latest" .
	docker build -f ./src/controllers/accumulators/counter_of_decades_per_author/Dockerfile -t "counter_of_decades_per_author:latest" .
	docker build -f ./src/controllers/filters/filter_of_authors_by_decade_count/Dockerfile -t "filter_of_authors_by_decade_count:latest" .
	docker build -f ./src/controllers/sinks/query2_result_generator/Dockerfile -t "query2_result_generator:latest" .

	docker build -f ./src/controllers/filters/filter_of_compact_reviews_by_decade/Dockerfile -t "filter_of_compact_reviews_by_decade:latest" .
	docker build -f ./src/controllers/accumulators/counter_of_reviews_per_book/Dockerfile -t "counter_of_reviews_per_book:latest" .
	docker build -f ./src/controllers/filters/filter_of_books_by_review_count/Dockerfile -t "filter_of_books_by_review_count:latest" .
	docker build -f ./src/controllers/sinks/query3_result_generator/Dockerfile -t "query3_result_generator:latest" .

	docker build -f ./src/controllers/filters/sorter_of_books_by_score_average/Dockerfile -t "sorter_of_books_by_score_average:latest" .
	docker build -f ./src/controllers/sinks/query4_result_generator/Dockerfile -t "query4_result_generator:latest" .

	docker build -f ./src/controllers/filters/filter_of_merged_reviews_by_book_genre/Dockerfile -t "filter_of_merged_reviews_by_book_genre:latest" .
	docker build -f ./src/controllers/accumulators/sentiment_analyzer/Dockerfile -t "sentiment_analyzer:latest" .
	docker build -f ./src/controllers/filters/filter_of_books_by_sentiment_quantile/Dockerfile -t "filter_of_books_by_sentiment_quantile:latest" .
	docker build -f ./src/controllers/sinks/query5_result_generator/Dockerfile -t "query5_result_generator:latest" .

	docker build -f ./src/controllers/health_checker/Dockerfile -t "health_checker:latest" .
	docker build -f ./src/killer/Dockerfile -t "killer:latest" .

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

killer-run:
	@test -n "$(INTERVAL)" || (echo 'An INTERVAL is required' && exit 1)
	@test -n "$(KILL_PERCENTAGE)" || (echo 'A KILL_PERCENTAGE is required' && exit 1)
	@test -n "$(NUM_OF_HEALTH_CHECKERS)" || (echo 'NUM_OF_HEALTH_CHECKERS is required' && exit 1)
	docker build -f ./src/killer/Dockerfile -t "killer:latest" .
	docker run -it --rm --name=killer \
	--network=host \
	-v ./src/monitorable_controllers.txt:/monitorable_controllers.txt \
	-v /var/run/docker.sock:/var/run/docker.sock \
	killer:latest \
	--interval $(INTERVAL) --kill_percentage $(KILL_PERCENTAGE) --num_of_health_checkers $(NUM_OF_HEALTH_CHECKERS)
	docker attach killer
	docker logs killer
.PHONY: killer-run

killer-stop:
	docker stop killer

