import io
from shared.mq_connection_handler import MQConnectionHandler
import logging
import csv
from shared import constants
import re
from shared.monitorable_process import MonitorableProcess

TITLE_IDX = 0
AUTHORS_IDX = 1
PUBLISHER_IDX = 2
PUBLISHED_DATE_IDX = 3
CATEGORIES_IDX = 4
REQUIRED_SIZE_OF_ROW = 5


class YearPreprocessor(MonitorableProcess):
    def __init__(self, input_exchange: str, input_queue: str, output_exchange: str, output_queue_towards_preproc: str, output_queue_towards_filter: str):
        super().__init__()
        self.output_queue_towards_preproc = output_queue_towards_preproc
        self.output_queue_towards_filter = output_queue_towards_filter
        self.mq_connection_handler = MQConnectionHandler(output_exchange, 
                                                         {output_queue_towards_preproc: [output_queue_towards_preproc],
                                                          output_queue_towards_filter: [output_queue_towards_filter]},
                                                         input_exchange,
                                                         [input_queue])
        
        self.mq_connection_handler.setup_callback_for_input_queue(input_queue, self.__preprocess_batch)

    def __preprocess_batch(self, ch, method, properties, body):
        msg = body.decode()
        if msg == constants.FINISH_MSG:
            self.mq_connection_handler.send_message(self.output_queue_towards_filter, msg)
            self.mq_connection_handler.send_message(self.output_queue_towards_preproc, msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            batch = csv.reader(io.StringIO(msg), delimiter=',', quotechar='"')
            batch_to_send_towards_preproc = ""
            batch_to_send_towards_filter = ""
            for row in batch:
                if len(row) != REQUIRED_SIZE_OF_ROW:
                    continue
                title = row[TITLE_IDX]
                authors = row[AUTHORS_IDX]
                publisher = row[PUBLISHER_IDX]
                published_date = row[PUBLISHED_DATE_IDX]
                categories = row[CATEGORIES_IDX]

                year = self.__extract_year(published_date)
                if year is None:
                    continue

                batch_to_send_towards_preproc += f"{title},\"{authors}\",{year},\"{categories}\"" + "\n"
                batch_to_send_towards_filter += f"{title},\"{authors}\",{publisher},{year},\"{categories}\"" + "\n"
            
            if batch_to_send_towards_preproc and batch_to_send_towards_filter:
                self.mq_connection_handler.send_message(self.output_queue_towards_preproc, batch_to_send_towards_preproc)
                self.mq_connection_handler.send_message(self.output_queue_towards_filter, batch_to_send_towards_filter)
            ch.basic_ack(delivery_tag=method.delivery_tag)



    def __extract_year(self, date):        
        if date:
            year_regex = re.compile('[^\d]*(\d{4})[^\d]*')
            result = year_regex.search(date)
            return int(result.group(1)) if result else None
        return None

                

                
                




    def start(self):
        self.mq_connection_handler.start_consuming()
