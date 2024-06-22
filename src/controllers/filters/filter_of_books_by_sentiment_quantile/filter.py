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
                 num_of_sentiment_analyzers: int,
                 controller_name: str):
        super().__init__(controller_name)
        self.batch_size = batch_size
        self.quantile = quantile
        self.num_of_sentiment_analyzers = num_of_sentiment_analyzers
        self.output_queue = output_queue_name
        self.mq_connection_handler = MQConnectionHandler(
            output_exchange_name=output_exchange_name, 
            output_queues_to_bind={output_queue_name: [output_queue_name]}, 
            input_exchange_name=input_exchange_name, 
            input_queues_to_recv_from=[input_queue_name]
        )
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue_name, self.state_handler_callback, self.__filter_by_quantile) 

            
    def __filter_by_quantile(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_R:
            client_eofs_received = self.state.get(body.client_id, {}).get("eofs_received", 0) + 1
            self.state[body.client_id].update({"eofs_received": client_eofs_received})
            if int(client_eofs_received) == int(self.num_of_sentiment_analyzers):
                logging.info(f"Received all EOFs from [ client_{body.client_id} ].")
                self.__handle_final_eof(body.client_id)
                self.state[body.client_id].update({"eofs_received": 0})
        else:
            batch_of_books_with_avg_polarity = body.get_batch_iter_from_payload()
            for book in batch_of_books_with_avg_polarity:
                title = book[TITLE_IDX]
                avg_polarity = float(book[AVG_POLARITY_IDX])
                self.__insert_in_sorted_books(body.client_id, title, avg_polarity)
            
        
    def __handle_final_eof(self, client_id):
        polarity_at_quantile = self.__get_polarity_at_required_quantile(client_id)
        logging.info(f"([ client_{client_id} ]); [ {polarity_at_quantile} ] is the value of avg polarity for the required [ {self.quantile} ] quantile")
        
        sorted_books_by_polarity: list[tuple[str, float]] = self.state[client_id].get("sorted_books_by_polarity", [])
        remaining_amount_of_books = len(sorted_books_by_polarity)
        payload_current_size = 0
        payload_to_send = ""
        while (remaining_amount_of_books > 0) and (payload_current_size < self.batch_size):
            book = sorted_books_by_polarity.pop(0)
            if book[AVG_POLARITY_IDX] >= polarity_at_quantile:
                payload_to_send += f"{book[TITLE_IDX]},{book[AVG_POLARITY_IDX]}" + "\n"
                payload_current_size += 1
                remaining_amount_of_books -= 1
                if payload_current_size == self.batch_size or remaining_amount_of_books == 0:
                    seq_num_to_send = self.get_seq_num_to_send(client_id, self.controller_name)
                    self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.DATA, client_id, self.controller_name, seq_num_to_send, payload_to_send).encode_to_str())
                    self.update_self_seq_number(client_id, seq_num_to_send)
                    payload_to_send = ""
                    payload_current_size = 0
            else:
                # From this point onwards all books are not suitable as the books are sorted.
                if payload_to_send:
                    seq_num_to_send = self.get_seq_num_to_send(client_id, self.controller_name)
                    self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.DATA, client_id, self.controller_name, seq_num_to_send, payload_to_send).encode_to_str())
                    self.update_self_seq_number(client_id, seq_num_to_send)
                break
       
        seq_num_to_send = self.get_seq_num_to_send(client_id, self.controller_name)
        self.mq_connection_handler.send_message(self.output_queue, SystemMessage(SystemMessageType.EOF_R, client_id, self.controller_name, seq_num_to_send).encode_to_str())
        self.update_self_seq_number(client_id, seq_num_to_send)
        self.state[client_id].update({"sorted_books_by_polarity": []})

        
        
    # ==============================================================================================================


    def __insert_in_sorted_books(self, client_id, title, avg_polarity):
        sorted_books_by_polarity: list[tuple[str, float]] = self.state[client_id].get("sorted_books_by_polarity", [])
        if len(sorted_books_by_polarity) == 0:
            sorted_books_by_polarity.append((title, avg_polarity))
        else:
            for idx, book in enumerate(sorted_books_by_polarity):
                if avg_polarity > book[AVG_POLARITY_IDX]:
                    sorted_books_by_polarity.insert(idx, (title, avg_polarity))
                    break
            else:
                sorted_books_by_polarity.append((title, avg_polarity))
        self.state[client_id].update({"sorted_books_by_polarity": sorted_books_by_polarity})

    
    def __get_polarity_at_required_quantile(self, client_id):
        sorted_books_by_polarity: list[tuple[str, float]] = self.state[client_id].get("sorted_books_by_polarity", [])
        avg_polarities = [book[AVG_POLARITY_IDX] for book in sorted_books_by_polarity]
        polarity_at_quantile = np.quantile(avg_polarities, self.quantile)
        return polarity_at_quantile


    # ==============================================================================================================


    def start(self):
        self.mq_connection_handler.channel.start_consuming()

