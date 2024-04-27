from shared.mq_connection_handler import MQConnectionHandler
import signal
import logging

class DecadePreprocessor:
    def __init__(self, input_exchange: str, input_queue: str, output_exchange: str, output_queue_towards_preproc: str, output_queue_towards_filter: str):
        self.output_queue = output_queue_towards_preproc
        self.mq_connection_handler = MQConnectionHandler(output_exchange, 
                                                         {output_queue_towards_preproc: [output_queue_towards_preproc],
                                                          output_queue_towards_filter: [output_queue_towards_filter]},
                                                         input_exchange,
                                                         [input_queue])
        
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue, self.__preprocess_batch)
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down book_sanitizer")
        self.mq_connection_handler.close_connection()


    def __preprocess_batch(self, ch, method, properties, body):
        # TODO
        pass




    def start(self):
        self.mq_connection_handler.start_consuming()
