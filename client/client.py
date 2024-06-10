import logging
from shared.socket_connection_handler import SocketConnectionHandler
from shared import constants
import signal


KiB = 1024
MAX_SIZE_FOR_LARGE_RESULTS_OUTPUT = 2 * KiB

class Client:
    def __init__(self, server_ip, server_port, reviews_file_path, books_file_path, batch_size):
        self.server_ip = server_ip
        self.server_port = server_port
        self.reviews_file_path = reviews_file_path
        self.books_file_path = books_file_path
        self.batch_size = batch_size
        self.connection_handler = None
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down client")
        if self.connection_handler:
            self.connection_handler.close()

    def start(self):
        logging.info("Starting client")
        try:
            self.connection_handler = SocketConnectionHandler.connect_and_create(self.server_ip, self.server_port)

            logging.info("Connected to server at {}:{}".format(self.server_ip, self.server_port))
            self.send_file_data(self.books_file_path, "books")
            self.send_file_data(self.reviews_file_path, "reviews")       
            self.receive_results() 
        except Exception as e:
            logging.error("Failed to connect to server: {}".format(str(e)))
            
    def send_file_data(self, file, type_of_content_in_file):
        """
        Sends the file data to the server
        """
        with open(file, 'r') as file:
            self.connection_handler.send_message(f"Start: {type_of_content_in_file}")
            completed = False

            _csv_header = file.readline()
            while not completed:
                batch = ""
                for _ in range(self.batch_size):                    
                    line = file.readline()
                    if not line or line == "\n":
                        completed = True
                        break
                    batch += line
                if not batch:
                    break
                self.connection_handler.send_message(batch)   
                response = self.connection_handler.read_message()
                if response != constants.OK_MSG:
                    logging.error("Server response: {}".format(response))
                    break  
        self.connection_handler.send_message(constants.FINISH_MSG)
        logging.info(f"Finish data sent to server ({type_of_content_in_file})")

        
    def receive_results(self):
        """
        Receives the results from the server
        """
        finished_receiving = False
        while finished_receiving is False:
            results, size, size_in_lines = self.connection_handler.read_message_with_size_in_lines()
            if results == constants.FINISH_MSG:
                finished_receiving = True
            else:
                if size < MAX_SIZE_FOR_LARGE_RESULTS_OUTPUT:
                    logging.info(f"\nResults received from server:\n {results}")
                    logging.info(f"  <<< Total size of result: {size_in_lines} rows >>>\n")
                else:
                    logging.info(f"\nResult received from server:\n {results[:MAX_SIZE_FOR_LARGE_RESULTS_OUTPUT]}(...)\n =========================================== [...] ===========================================\n(...){results[-MAX_SIZE_FOR_LARGE_RESULTS_OUTPUT:]}")
                    logging.info(f"  <<< Total size of result: {size_in_lines} rows >>>\n")
        self.connection_handler.close()
        logging.info("Client connection closed")