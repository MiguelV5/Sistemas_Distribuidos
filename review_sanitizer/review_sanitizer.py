from shared.mq_connection_handler import MQConnectionHandler
import signal
import logging

class ReviewSanitizer:
    def __init__(self, input_exchange: str, input_queue: str, output_exchange: str, output_queues: list[str]):
        self.output_queues = output_queues
        self.mq_connection_handler = MQConnectionHandler(output_exchange, 
                                                         {output_queue: [output_queue] for output_queue in output_queues},
                                                         input_exchange,
                                                         [input_queue])
        
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue, self.__sanitize_batch_of_reviews)
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down book_sanitizer")
        self.mq_connection_handler.close_connection()


    def __sanitize_batch_of_reviews(self, ch, method, properties, body):
        # TODO
        pass



    def start(self):
        self.mq_connection_handler.start_consuming()
