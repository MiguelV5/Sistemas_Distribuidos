from shared.mq_connection_handler import MQConnectionHandler
import logging
from shared import constants
import numpy as np
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType

TITLE_IDX = 0
AVG_POLARITY_IDX = 1

class FilterBySentimentQuantile(MonitorableProcess):
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str,
                 quantile: float,
                 batch_size: int,
                 controller_name: str):
        super().__init__(controller_name)
        self.batch_size = batch_size
        
        self.output_queue = output_queue_name
        self.quantile = quantile
        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue_name, self.state_handler_callback, self.__filter_by_quantile) 

            
    def __filter_by_quantile(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_R:
            logging.info(f"Received EOF_R from [ client_{body.client_id} ]")
            self.__handle_eof_reviews()
        else:
            book = msg.split(",")
            title = book[TITLE_IDX]
            avg_polarity = float(book[AVG_POLARITY_IDX])
            self.__insert_in_sorted_books(title, avg_polarity)
            
        
    def __handle_eof_reviews(self):
        polarity_at_quantile = self.__get_polarity_at_required_quantile()
        logging.info(f"The required quantile: {self.quantile} has a value with average polarity of {polarity_at_quantile}")

        for idx, book in enumerate(self.sorted_books_by_polarity):
            if book[AVG_POLARITY_IDX] >= polarity_at_quantile:
                msg = f"{book[TITLE_IDX]},{book[AVG_POLARITY_IDX]}"
                self.mq_connection_handler.send_message(self.output_queue, msg)
            else:
                break
        self.mq_connection_handler.send_message(self.output_queue, constants.FINISH_MSG)
        logging.info("Sent EOF message to output queue")
        self.sorted_books_by_polarity = []
        
        

    def __insert_in_sorted_books(self, title, avg_polarity):
        if len(self.sorted_books_by_polarity) == 0:
            self.sorted_books_by_polarity.append((title, avg_polarity))
        else:
            for idx, book in enumerate(self.sorted_books_by_polarity):
                if avg_polarity > book[AVG_POLARITY_IDX]:
                    self.sorted_books_by_polarity.insert(idx, (title, avg_polarity))
                    break
            else:
                self.sorted_books_by_polarity.append((title, avg_polarity))

    
    def __get_polarity_at_required_quantile(self):
        avg_polarities = [book[AVG_POLARITY_IDX] for book in self.sorted_books_by_polarity]
        polarity_at_quantile = np.quantile(avg_polarities, self.quantile)
        return polarity_at_quantile


    def start(self):
        self.mq_connection_handler.channel.start_consuming()

