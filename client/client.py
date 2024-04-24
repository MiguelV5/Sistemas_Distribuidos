import logging
from shared.direct_connection_handler import SocketConnectionHandler
import socket

class Client:
    def __init__(self, server_ip, server_port, reviews_file, books_file, batch_size):
        self._server_ip = server_ip
        self._server_port = server_port
        self._reviews_file = reviews_file
        self._books_file = books_file
        self._batch_size = batch_size
        self._connection_handler = None
        
    def start(self):
        logging.info("Starting client")
        # init connection with server using ip and port, using tcp socket
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.connect((self._server_ip, self._server_port))           
            self._connection_handler = SocketConnectionHandler(server_socket)
            logging.info("Connected to server at {}:{}".format(self._server_ip, self._server_port))
            self.send_file_data(self._books_file)
            self.send_file_data(self._reviews_file)        
        except Exception as e:
            logging.error("Failed to connect to server: {}".format(str(e)))
            
    def send_file_data(self, file):
        """
        Sends the file data to the server
        """
        logging.info(f"Sending data to server. File: {file}")
        with open(file, 'r') as file:
            # read the data from the csv file, start sending from the second line, sending in batches of configurable size and awaiting confirmation from the server before sending the next batch. This should read line by line, since we don't have infinite memory.
        
            completed = False
            while not completed:
                batch = ""
                for i in range(self._batch_size):
                    line = file.readline()
                    batch += line + "\n"
                    if not line:
                        completed = True
                        break
                self._connection_handler.send_message(batch)   
                response = self._connection_handler.read_message()
                if response != "OK":
                    logging.error("Server response: {}".format(response))
                    break  
        logging.info(f"Finish data sent to server. File: {file}")

        