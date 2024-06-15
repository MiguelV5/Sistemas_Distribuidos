from shared.mq_connection_handler import MQConnectionHandler
import logging
import csv
import io
from shared import constants
from shared.monitorable_process import MonitorableProcess
from shared.protocol_messages import SystemMessage, SystemMessageType

TITLE_IDX = 0
AUTHORS_IDX = 2
PUBLISHER_IDX = 5
PUBLISHED_DATE_IDX = 6
CATEGORIES_IDX = 8
REQUIRED_SIZE_OF_ROW = 10

class BookSanitizer(MonitorableProcess):

    def __init__(self, 
                 input_exchange: str, 
                 input_queue: str, 
                 output_exchange: str, 
                 output_queue: str, 
                 controller_name: str):
        super().__init__(controller_name)

        self.output_queue = output_queue
        self.mq_connection_handler = MQConnectionHandler(output_exchange, 
                                                         {output_queue: [output_queue]},
                                                         input_exchange,
                                                         [input_queue])
        
        self.mq_connection_handler.setup_callbacks_for_input_queue(input_queue, self.state_handler_callback, self.__process_msg_from_sv)


    def __process_msg_from_sv(self, body: SystemMessage):
        if body.type == SystemMessageType.EOF_B:
            self.__handle_eof(body)
        elif body.type == SystemMessageType.DATA:
            self.__sanitize_books_and_send(body)

    
    def __handle_eof(self, body: SystemMessage):
        logging.info(f"Received EOF_B from client: {body.client_id}")
        seq_num_to_send = self.get_next_seq_number(body.client_id, self.controller_name)
        msg_to_send = SystemMessage(SystemMessageType.EOF_B, body.client_id, self.controller_name, seq_num_to_send).encode_to_str()
        self.mq_connection_handler.send_message(self.output_queue, msg_to_send)
        self.update_self_seq_number(body.client_id, seq_num_to_send)

    def __sanitize_books_and_send(self, body: SystemMessage):
        books_batch = body.get_batch_iter_from_payload()
        payload_to_send = ""
        for book in books_batch:
            if len(book) < REQUIRED_SIZE_OF_ROW:
                    continue
            title = book[TITLE_IDX]
            authors = book[AUTHORS_IDX]
            publisher = book[PUBLISHER_IDX]
            published_date = book[PUBLISHED_DATE_IDX]
            categories = book[CATEGORIES_IDX]
            if not title or not authors or not published_date or not categories:
                continue

            title = self.__fix_title_format(title)
            authors = self.__fix_authors_format(authors)
            publisher = self.__fix_publisher_format(publisher)
            categories = self.__fix_categories_format(categories)

            payload_to_send += self.__format_sanitized_book(title, authors, publisher, published_date, categories)
        
        if payload_to_send:
            seq_num_to_send = self.get_next_seq_number(body.client_id, self.controller_name)
            msg_to_send = SystemMessage(SystemMessageType.DATA, body.client_id, self.controller_name, seq_num_to_send, payload_to_send).encode_to_str()
            self.mq_connection_handler.send_message(self.output_queue, msg_to_send)
            self.update_self_seq_number(body.client_id, seq_num_to_send)
    

    def __fix_title_format(self, title):
        return title.replace("\n", " ").replace("\r", "").replace(",", ";").replace('"', "`").replace("'", "`")

    def __fix_authors_format(self, authors):
        return self.__make_list_format_consisent(authors)

    def __fix_publisher_format(self, publisher):
        return publisher.replace(",", ";")
    
    def __fix_categories_format(self, categories):
        return self.__make_list_format_consisent(categories)
    
    def __make_list_format_consisent(self, list_as_str):
        list_as_str = list_as_str.replace('"', "").replace("'","")
        fixed_list = ""
        for i in range(len(list_as_str)):
            if i + 1 == len(list_as_str):
                fixed_list += "'" + "]"
            elif i == 0:
                fixed_list += "[" + "'"
            else:
                if list_as_str[i + 1] == ",":
                    fixed_list += list_as_str[i] + "'" + "," + "'"
                elif list_as_str[i] == ",":
                    continue
                else:
                    fixed_list += list_as_str[i]
                    
        fixed_list = fixed_list.replace("',',','", "")  # if it had commas in the middle, this avoids a wrong element named ','
        fixed_list = fixed_list.replace("',' ", "', '")  # restore original spacing
        return fixed_list

    def __format_sanitized_book(self, title, authors, publisher, published_date, categories):
        return f"{title},\"{authors}\",{publisher},{published_date},\"{categories}\"" + "\n"


    def start(self):
        self.mq_connection_handler.start_consuming()
