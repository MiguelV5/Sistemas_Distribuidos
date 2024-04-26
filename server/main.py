import logging
from shared.initializers import init_log, init_configs
from server import Server


def main():
    config_params = init_configs(["LOGGING_LEVEL", "SERVER_PORT", "INPUT_EXCHANGE", "INPUT_QUEUE_OF_QUERY_RESULTS","OUTPUT_EXCHANGE_OF_DATA", "OUTPUT_QUEUE_OF_REVIEWS", "OUTPUT_QUEUE_OF_BOOKS"])
    init_log(config_params["LOGGING_LEVEL"])

    logging.info("Starting server")
    server = Server(server_port=int(config_params["SERVER_PORT"]),
                    input_exchange=config_params["INPUT_EXCHANGE"],
                    input_queue_of_query_results=config_params["INPUT_QUEUE_OF_QUERY_RESULTS"],
                    output_exchange_of_data=config_params["OUTPUT_EXCHANGE_OF_DATA"],
                    output_queue_of_reviews=config_params["OUTPUT_QUEUE_OF_REVIEWS"],
                    output_queue_of_books=config_params["OUTPUT_QUEUE_OF_BOOKS"])
    server.run()
    

if __name__ == "__main__":
    main()
