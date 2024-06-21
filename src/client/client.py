import io
import os
import logging
import multiprocessing
import socket
from shared.protocol_messages import QueryMessage, QueryMessageType
from shared.socket_connection_handler import SocketConnectionHandler
from shared import constants
import signal
from multiprocessing.connection import Connection as PipeConnection


CONTINUE_WITH_REVIEWS_DATA = "CONTINUE"
CLIENT_RESULTS_BASE_PATH = "./results/client_"


class Client:
    def __init__(self, server_ip, server_port, reviews_file_path, books_file_path, batch_size, client_id):
        self.server_ip = server_ip
        self.server_port = server_port
        self.reviews_file_path = reviews_file_path
        self.books_file_path = books_file_path
        self.batch_size = batch_size
        self.client_id = client_id
        self.results_dir_path = f"{CLIENT_RESULTS_BASE_PATH}{client_id}/"
        self.data_connection_handler = None
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down client")
        if self.data_connection_handler:
            self.data_connection_handler.close()

    def start(self):
        logging.info("Starting client")
        try:
            receiver_pipe, sender_pipe = multiprocessing.Pipe()
            p = multiprocessing.Process(target=self.__handle_server_results_streaming, args=(sender_pipe,))
            p.start()
            self.data_connection_handler = SocketConnectionHandler.connect_and_create(self.server_ip, self.server_port)
            logging.info("Connected to server at {}:{}".format(self.server_ip, self.server_port))
            self.send_files_data(receiver_pipe)
            p.join()
        except Exception as e:
            logging.error("Failed to connect to server: {}".format(str(e)))


    # ==============================================================================================================


    def __handle_server_results_streaming(self, sender_pipe: PipeConnection):
        listener_for_sv_results = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener_for_sv_results.bind(('', constants.CLIENT_RESULTS_PORT))
        listener_for_sv_results.listen()
        logging.info("Listening for server results at port {}".format(constants.CLIENT_RESULTS_PORT))
        finished_receiving_results = False
        self.__clear_results_dir_for_client()
        while not finished_receiving_results:
            try:
                sv_sock, _ = listener_for_sv_results.accept()
                results_connection_handler = SocketConnectionHandler.create_from_socket(sv_sock)
                received_msg_str, size_in_lines = results_connection_handler.read_message_with_size_in_lines()
                received_msg = QueryMessage.decode_from_str(received_msg_str)
                if received_msg.type == QueryMessageType.CONTINUE:
                    sender_pipe.send(CONTINUE_WITH_REVIEWS_DATA)
                elif received_msg.type == QueryMessageType.SV_RESULT:
                    logging.info(f"\t \t  <<< Size of received result: {size_in_lines - 1} rows >>>")
                    logging.info(f"\t \t  {received_msg.payload}\n")
                    self.__save_results_to_file(received_msg.payload)
                elif received_msg.type == QueryMessageType.SV_FINISHED:
                    logging.info("[ SERVER FINISHED SENDING RESULTS ]")
                    finished_receiving_results = True
                results_connection_handler.close()
            except Exception as e:
                logging.error("Error handling server results: {}".format(str(e)))

    def __clear_results_dir_for_client(self):
        if not os.path.exists(self.results_dir_path):
            os.makedirs(self.results_dir_path)
        else:
            for file in os.listdir(self.results_dir_path):
                os.remove(os.path.join(self.results_dir_path, file))
            
    def __save_results_to_file(self, results_payload: str): 
        file_path = self.results_dir_path
        for i, payload_header in enumerate(constants.PAYLOAD_HEADERS):
            if results_payload.startswith(payload_header):
                file_path += f"query{i+1}.csv"
                results_payload = results_payload.lstrip(payload_header)
                break
        
        with open(file_path, 'a') as file:
            file.write(results_payload)



    # ==============================================================================================================


    def send_files_data(self, receiver_pipe: PipeConnection):
        logging.info("Sending files data to server")
        with open(self.books_file_path, 'r') as file:
            self.send_books_data(receiver_pipe, file) 
        with open(self.reviews_file_path, 'r') as file:
            self.send_reviews_data(file)


    def send_books_data(self, receiver_pipe: PipeConnection, file: io.TextIOWrapper):
        logging.info("[ SENDING BOOKS DATA ] STARTED")
        completed_books = False
        _csv_header = file.readline()
        while not completed_books:
            batch = self.__get_next_batch_from_file(file)
            if not batch:
                completed_books = True
                msg_for_server = QueryMessage(QueryMessageType.EOF_B, self.client_id).encode_to_str()
                self.data_connection_handler.send_message(msg_for_server)
            else: 
                msg_for_server = QueryMessage(QueryMessageType.DATA_B, self.client_id, batch).encode_to_str()
                self.data_connection_handler.send_message(msg_for_server)
            response = QueryMessage.decode_from_str(self.data_connection_handler.read_message())
            if response.type == QueryMessageType.DATA_ACK:
                continue
            elif response.type == QueryMessageType.WAIT_FOR_SV:
                logging.info("All books data was sent. Waiting for server to confirm continuation with reviews data")
                msg = receiver_pipe.recv()
                if msg == CONTINUE_WITH_REVIEWS_DATA:
                    logging.info("Server confirmed continuation")
                else:
                    logging.error("Server did not confirm continuation")
                    break

    def send_reviews_data(self, file):
        logging.info("[ SENDING REVIEWS DATA ] STARTED")
        completed_reviews = False
        _csv_header = file.readline()
        while not completed_reviews:
            batch = self.__get_next_batch_from_file(file)
            if not batch:
                completed_reviews = True
                msg_for_server = QueryMessage(QueryMessageType.EOF_R, self.client_id).encode_to_str()
                self.data_connection_handler.send_message(msg_for_server)
            else: 
                msg_for_server = QueryMessage(QueryMessageType.DATA_R, self.client_id, batch).encode_to_str()
                self.data_connection_handler.send_message(msg_for_server)
            response = QueryMessage.decode_from_str(self.data_connection_handler.read_message())
            if response.type == QueryMessageType.DATA_ACK:
                continue
            else:
                logging.error("Error sending reviews data")
                break

    def __get_next_batch_from_file(self, file):
        batch = ""
        for _ in range(self.batch_size):                    
            line = file.readline()
            if not line or line == "\n":
                completed = True
                break
            batch += line
        return batch

        
    