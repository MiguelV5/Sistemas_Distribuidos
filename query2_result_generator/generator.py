from shared.mq_connection_handler import MQConnectionHandler
import logging

class Generator:
    def __init__(self, input_exchange_name: str, output_exchange_name: str, input_queue_name: str, output_queue_name: str):
        self.input_exchange_name = input_exchange_name
        self.output_exchange_name = output_exchange_name
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name
        self.response_msg = "Q2 Results: "
        self.mq_connection_handler = None
        
    def start(self):
        try:
            self.mq_connection_handler = MQConnectionHandler(output_exchange_name=self.output_exchange_name, 
                                                         output_queues_to_bind={self.output_queue_name: [self.output_queue_name]}, 
                                                         input_exchange_name=self.input_exchange_name, 
                                                         input_queues_to_recv_from=[self.input_queue_name])
            logging.info(f"MQConnectionHandler created successfully. Parameters: {self.input_exchange_name}, {self.output_exchange_name}, {self.input_queue_name}, {self.output_queue_name}")
        except Exception as e:
            logging.error(f"Error while creating MQConnectionHandler: {e}")
            return
        try:
            self.mq_connection_handler.setup_callback_for_input_queue(self.input_queue_name, self.__get_results)
            self.mq_connection_handler.start_consuming()
        except Exception as e:
            logging.error(f"Error while setting up callback for input queue: {e}")
            return
        
    def __get_results(self, ch, method, properties, body):
        msg = body.decode()
        if msg == "EOF":
            logging.info("Received EOF")
            self.mq_connection_handler.send_message(self.output_queue_name, self.response_msg)
            logging.info(f"Sent response message to output queue: {self.response_msg}")
        else: 
            self.response_msg += '\n' + msg 
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        