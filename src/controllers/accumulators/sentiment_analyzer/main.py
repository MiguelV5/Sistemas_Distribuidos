from shared.initializers import init_configs, init_log
from sentiment_analyzer import SentimentAnalyzer
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_REVIEWS", "OUTPUT_QUEUE_OF_REVIEWS", "WORKER_NAME"])
    init_log(config_params["LOGGING_LEVEL"])
    logging.info("Sentiment Analyzer started.")
    sentiment_analyzer = SentimentAnalyzer(config_params["INPUT_EXCHANGE"], 
                                           config_params["OUTPUT_EXCHANGE"], 
                                           config_params["INPUT_QUEUE_OF_REVIEWS"], 
                                           config_params["OUTPUT_QUEUE_OF_REVIEWS"],
                                           config_params["WORKER_NAME"])
    sentiment_analyzer.start()
    
if __name__ == "__main__":
    main()