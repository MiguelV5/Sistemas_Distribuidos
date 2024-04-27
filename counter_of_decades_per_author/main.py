from shared.initializers import init_log, init_configs
from counter import Counter
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_AUTHORS", "OUTPUT_QUEUE_OF_AUTHORS"])
    init_log(config_params["LOGGING_LEVEL"])
    counter = Counter(input_exchange=config_params["INPUT_EXCHANGE"],
                        output_exchange=config_params["OUTPUT_EXCHANGE"],
                        input_queue_of_authors=config_params["INPUT_QUEUE_OF_AUTHORS"],
                        output_queue_of_authors=config_params["OUTPUT_QUEUE_OF_AUTHORS"])
    logging.info("Starting counter")
    counter.start()
    
if __name__ == "__main__":
    main()
    