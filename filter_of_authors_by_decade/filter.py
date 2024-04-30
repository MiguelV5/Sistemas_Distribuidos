from shared.mq_connection_handler import MQConnectionHandler
import logging
import signal
from shared import constants


class FilterOfAuthorsByDecade:
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str, 
                 min_decades_to_filter: int
                 ):
        self.input_exchange_name = input_exchange_name
        self.output_exchange_name = output_exchange_name
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name
        self.min_decades_to_filter = min_decades_to_filter
        self.mq_connection_handler = None
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down FilterOfAuthorsByDecade")
        self.mq_connection_handler.stop_consuming()
        
    def start(self):
        self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange_name, 
                                                         output_queues_to_bind={self.output_queue_name: [self.output_queue_name]}, 
                                                         input_exchange_name=self.input_exchange_name, 
                                                         input_queues_to_recv_from=[self.input_queue_name])
        self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue_name, self.__filter_authors_by_decades_quantity)
        self.mq_connection_handler.channel.start_consuming()
        
            
    def __filter_authors_by_decades_quantity(self, ch, method, properties, body):
        msg = body.decode()
        logging.debug(f"Received message: {msg}")
        if msg == constants.FINISH_MSG:
            logging.info("EOF RECEIVED")
            self.mq_connection_handler.send_message(self.output_queue_name, constants.FINISH_MSG)
            logging.info("Sent EOF message to output queue")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.mq_connection_handler.close_connection()
        else:
            author, decades = msg.split(",")
            if int(decades) >= int(self.min_decades_to_filter):
                self.mq_connection_handler.send_message(self.output_queue_name, msg)
                logging.debug(f"Sent message to output queue: {msg}")
            else:
                logging.debug(f"Author {author} was filtered out. Decades: {decades} < {self.min_decades_to_filter}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
       