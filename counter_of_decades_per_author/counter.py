from shared.mq_connection_handler import MQConnectionHandler
import logging


class Counter:
    def __init__(self, input_exchange, output_exchange, input_queue_of_authors, output_queue_of_authors):
        self.input_exchange = input_exchange
        self.output_exchange = output_exchange
        self.input_queue_of_authors = input_queue_of_authors
        self.output_queue_of_authors = output_queue_of_authors
        self.mq_connection_handler = None
        self.authors_decades = {}
        
        
    def start(self):
        try:  
            self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange,
                                                             output_queues_to_bind={self.output_queue_of_authors: [self.output_queue_of_authors]},
                                                             input_exchange_name=self.input_exchange,
                                                             input_queues_to_recv_from=[self.input_queue_of_authors])
        except Exception as e:
            logging.error(f"Error starting counter: {str(e)}")
            
        try:
            self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue_of_authors, self.__count_authors)
            self.mq_connection_handler.start_consuming()
        except Exception as e:
            logging.error(f"Error setting up callback for input queue: {str(e)}, {e.__traceback__}")
            
    def __count_authors(self, ch, method, properties, body):
        """ 
        The body is a csv line with the following format in the line: "author, decade" 
        The counter should count the number of authors per decade and send the result to the output queue.
        """
        msg = body.decode()
        logging.info(f"Received message: {msg}")
        if msg == "EOF":
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.__send_results()
            return
        logging.info(f"Processing message: {msg}")
        author, decade = msg.split(',')
        self.authors_decades.setdefault(author, set()).add(decade)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def __send_results(self):
        for author, decades in self.authors_decades.items():
            output_msg = ""
            output_msg += author + ',' + str(len(decades))
            self.mq_connection_handler.send_message(self.output_queue_of_authors, output_msg)
            logging.info(f"Sent message to output queue: {output_msg}")
        self.mq_connection_handler.send_message(self.output_queue_of_authors, "EOF")
        logging.info("Sent EOF message to output queue")