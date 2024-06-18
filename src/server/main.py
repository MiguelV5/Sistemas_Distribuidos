import logging
from shared.initializers import init_log, init_configs
from server import Server


def main():
    config_params = init_configs(["LOGGING_LEVEL", "SERVER_PORT", "INPUT_EXCHANGE_OF_QUERY_RESULTS", "INPUT_EXCHANGE_OF_MERGERS_CONFIRMS","INPUT_QUEUE_OF_QUERY_RESULTS", "INPUT_QUEUE_OF_MERGERS_CONFIRMS", "OUTPUT_EXCHANGE_OF_DATA", "OUTPUT_QUEUE_OF_REVIEWS", "OUTPUT_QUEUE_OF_BOOKS", "MERGERS_QUANTITY"])
    init_log(config_params["LOGGING_LEVEL"])

    logging.info("Starting server")
    server = Server(server_port=int(config_params["SERVER_PORT"]),
                    input_exchange_of_query_results=config_params["INPUT_EXCHANGE_OF_QUERY_RESULTS"],
                    input_exchange_of_mergers_confirms=config_params["INPUT_EXCHANGE_OF_MERGERS_CONFIRMS"],
                    input_queue_of_query_results=config_params["INPUT_QUEUE_OF_QUERY_RESULTS"],
                    input_queue_of_mergers_confirms=config_params["INPUT_QUEUE_OF_MERGERS_CONFIRMS"],
                    output_exchange_of_data=config_params["OUTPUT_EXCHANGE_OF_DATA"],
                    output_queue_of_reviews=config_params["OUTPUT_QUEUE_OF_REVIEWS"],
                    output_queue_of_books=config_params["OUTPUT_QUEUE_OF_BOOKS"],
                    mergers_quantity=int(config_params["MERGERS_QUANTITY"]))
    server.run()
    

if __name__ == "__main__":
    main()
