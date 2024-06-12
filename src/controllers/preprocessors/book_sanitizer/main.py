import logging
from book_sanitizer import BookSanitizer
from shared.initializers import init_log, init_configs

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_BOOKS", "OUTPUT_QUEUE_OF_BOOKS", "CONTROLLER_NAME"])
    init_log(config_params["LOGGING_LEVEL"])

    logging.info("Starting book_sanitizer")
    book_sanitizer = BookSanitizer(input_exchange=config_params["INPUT_EXCHANGE"], 
                                   input_queue=config_params["INPUT_QUEUE_OF_BOOKS"],
                                   output_exchange=config_params["OUTPUT_EXCHANGE"],
                                   output_queue=config_params["OUTPUT_QUEUE_OF_BOOKS"],
                                   controller_name=config_params["CONTROLLER_NAME"])
    book_sanitizer.start()


if __name__ == "__main__":
    main()