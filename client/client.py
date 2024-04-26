import logging
from shared.socket_connection_handler import SocketConnectionHandler
import socket
import signal

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
        # init connection with server using ip and port, using tcp socket
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.connect((self.server_ip, self.server_port))           
            self.connection_handler = SocketConnectionHandler(server_socket)
            logging.info("Connected to server at {}:{}".format(self.server_ip, self.server_port))
            self.send_file_data(self.books_file_path, "books")
            # self.send_file_data(self.reviews_file_path, "reviews")        
        except Exception as e:
            logging.error("Failed to connect to server: {}".format(str(e)))
            
    def send_file_data(self, file, type_of_content_in_file):
        """
        Sends the file data to the server
        """
        logging.info(f"Sending data to server. File: {file}")
        with open(file, 'r') as file:
            # read the data from the csv file, start sending from the second line, sending in batches of configurable size and awaiting confirmation from the server before sending the next batch. This should read line by line, since we don't have infinite memory.
            self.connection_handler.send_message(f"Start: {type_of_content_in_file}")
            completed = False
            while not completed:
                batch = ""
                for i in range(self.batch_size):

                    # TODO: !IMPORTANT should use csv reader because of commas in reviews, \n in book descriptions, etc.
                    
                    line = file.readline()
                    batch += line + "\n"
                    if not line:
                        completed = True
                        break
                self.connection_handler.send_message(batch)   
                response = self.connection_handler.read_message()
                if response != "OK":
                    logging.error("Server response: {}".format(response))
                    break  
        logging.info(f"Finish data sent to server. File: {file}")
        self.__connection_handler.send_message("EOF")

        