import logging
from year_preprocessor import YearPreprocessor
from shared.initializers import init_log, init_configs

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_BOOKS", "OUTPUT_QUEUE_OF_BOOKS_TOWARDS_PREPROC", "OUTPUT_QUEUE_OF_BOOKS_TOWARDS_FILTER", "CONTROLLER_NAME"])
    init_log(config_params["LOGGING_LEVEL"])

    decade_preprocessor = YearPreprocessor(input_exchange=config_params["INPUT_EXCHANGE"], 
                                           input_queue=config_params["INPUT_QUEUE_OF_BOOKS"],
                                           output_exchange=config_params["OUTPUT_EXCHANGE"],
                                           output_queue_towards_preproc=config_params["OUTPUT_QUEUE_OF_BOOKS_TOWARDS_PREPROC"],
                                           output_queue_towards_filter=config_params["OUTPUT_QUEUE_OF_BOOKS_TOWARDS_FILTER"],
                                           controller_name=config_params["CONTROLLER_NAME"])
    decade_preprocessor.start()


if __name__ == "__main__":
    main()