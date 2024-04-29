from shared.mq_connection_handler import MQConnectionHandler
import logging
import csv
import io

AUTHORS_IDX = 0
DECADE_IDX = 1

class AuthorExpander:
    def __init__(self, input_exchange, output_exchange, input_queue_of_books, output_queues: dict[str,str]):
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
        self.input_queue_of_books = input_queue_of_books
        self.output_queues = {}
        for queue_name in output_queues.values():
            self.output_queues[queue_name] = [queue_name]
        self.mq_connection_handler = None
        
        
    def start(self):
        try:  
            self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange,
                                                             output_queues_to_bind=self.output_queues,
                                                             input_exchange_name=self.input_exchange,
                                                             input_queues_to_recv_from=[self.input_queue_of_books])
            logging.info(f"Create a MQConnectionHandler object for the author expander with the following parameters: output_exchange_name={self.output_exchange}, output_queues_to_bind={self.output_queues}, input_exchange_name={self.input_exchange}, input_queues_to_recv_from={self.input_queue_of_books}")
        except Exception as e:
            logging.error(f"Error starting author expander: {str(e)}")
            
        try:
            self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue_of_books, self.__expand_authors)
            self.mq_connection_handler.start_consuming()
        except Exception as e:
            logging.error(f"Error setting up callback for input queue: {str(e)}")
            logging.error(f"input_queue_of_books: {self.input_queue_of_books}")
            raise e
    
    def __expand_authors(self, ch, method, properties, body):
        """ 
        The body is a csv batch with the following format in a line: "['author_1',...,'author_n'], decade" 
        The expansion should create multiple lines, one for each author, with the following format: "author_i, decade"
        """
        msg = body.decode()
        logging.debug(f"Received message from input queue: {msg}")
        if msg == "EOF":
            for queue_name in self.output_queues:
                self.mq_connection_handler.send_message(queue_name, "EOF")
                logging.info(f"Sent EOF message to queue {queue_name}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        batch = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
        for row in batch:
            authors = eval(row[AUTHORS_IDX])
            decade = row[DECADE_IDX]
            for author in authors:
                output_msg = f"{author},{decade}"
                self.mq_connection_handler.send_message(self.__select_queue(author), output_msg)
                logging.debug(f"Sent message to queue: {output_msg}")
                    
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def __select_queue(self, author: str) -> str:
        """
        Should return the queue name where the author should be sent to.
        It uses the hash of the author to select a queue on self.output_queues
        """
        
        hash_value = hash(author)
        queue_index = hash_value % len(self.output_queues)
        return list(self.output_queues.keys())[queue_index]    
            
        
        