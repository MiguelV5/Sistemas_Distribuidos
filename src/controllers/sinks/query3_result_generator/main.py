from shared.initializers import init_configs, init_log
import logging
from generator import Generator

def main():
    config_params = init_configs(["LOGGING_LEVEL","INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_BOOKS", "OUTPUT_QUEUE_OF_QUERY", "CONTROLLER_NAME"])
    init_log(config_params["LOGGING_LEVEL"])
    generator = Generator(input_exchange=config_params["INPUT_EXCHANGE"], 
                          output_exchange=config_params["OUTPUT_EXCHANGE"], 
                          input_queue=config_params["INPUT_QUEUE_OF_BOOKS"], 
                          output_queue=config_params["OUTPUT_QUEUE_OF_QUERY"],
                          controller_name=config_params["CONTROLLER_NAME"])
    generator.start()
    logging.info("Query3 result generator started")
    
if __name__ == "__main__":
    main()