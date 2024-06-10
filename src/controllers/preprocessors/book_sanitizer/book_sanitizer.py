from shared.mq_connection_handler import MQConnectionHandler
import logging
import csv
import io
from shared import constants
from shared.monitorable_process import MonitorableProcess

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
                 worker_name: str):
        super().__init__(worker_name)
        self.output_queue = output_queue
        self.mq_connection_handler = MQConnectionHandler(output_exchange, 
                                                         {output_queue: [output_queue]},
                                                         input_exchange,
                                                         [input_queue])
        
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue, self.__sanitize_batch_of_books)


    def __sanitize_batch_of_books(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            self.mq_connection_handler.send_message(self.output_queue, msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            batch_as_csv = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            batch_to_send = ""
            for row in batch_as_csv:
                if len(row) < REQUIRED_SIZE_OF_ROW:
                    continue
                title = row[TITLE_IDX]
                authors = row[AUTHORS_IDX]
                publisher = row[PUBLISHER_IDX]
                published_date = row[PUBLISHED_DATE_IDX]
                categories = row[CATEGORIES_IDX]
                if not title or not authors or not published_date or not categories:
                    continue

                title = self.__fix_title_format(title)
                authors = self.__fix_authors_format(authors)
                publisher = self.__fix_publisher_format(publisher)
                categories = self.__fix_categories_format(categories)

                batch_to_send += f"{title},\"{authors}\",{publisher},{published_date},\"{categories}\"" + "\n"

            if batch_to_send:
                self.mq_connection_handler.send_message(self.output_queue, batch_to_send)
            ch.basic_ack(delivery_tag=method.delivery_tag)

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



    def start(self):
        self.mq_connection_handler.start_consuming()
