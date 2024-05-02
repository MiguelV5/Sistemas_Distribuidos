from shared.mq_connection_handler import MQConnectionHandler
import logging
import signal
from shared import constants

TITLE_IDX = 0
AVG_POLARITY_IDX = 1

class FilterBySentimentQuantile:
    def __init__(self, 
                 input_exchange_name: str, 
                 output_exchange_name: str, 
                 input_queue_name: str, 
                 output_queue_name: str,
                 quantile: float):
        self.output_queue = output_queue_name
        self.quantile = quantile
        self.sorted_books_by_polarity = []
        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue_name, self.__filter_by_quantile)
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down Sentiment Analizer")
        self.mq_connection_handler.close_connection()
        
            
    def __filter_by_quantile(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            self.__handle_eof_reviews()
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            book = msg.split(",")
            title = book[TITLE_IDX]
            avg_polarity = float(book[AVG_POLARITY_IDX])
            self.__insert_in_sorted_books(title, avg_polarity)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def __handle_eof_reviews(self):
        index_at_quantile, polarity_at_quantile = self.__get_items_at_required_quantile()
        logging.info(f"The required quantile: {self.quantile} has a value with average polarity of {polarity_at_quantile}")

        for idx, book in enumerate(self.sorted_books_by_polarity):
            if idx < index_at_quantile:
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

    
    def __get_items_at_required_quantile(self):
        index_at_quantile = round(self.quantile * (len(self.sorted_books_by_polarity) - 1))
        polarity_at_quantile = self.sorted_books_by_polarity[index_at_quantile][AVG_POLARITY_IDX]
        return index_at_quantile, polarity_at_quantile


    def start(self):
        self.mq_connection_handler.channel.start_consuming()

