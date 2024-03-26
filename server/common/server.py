import multiprocessing
import signal
import socket
import logging
from common import utils


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)

        self._server_must_shutdown = False
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

        self._clients_that_notified_completion = multiprocessing.Value('i', 0)
        self._storefile_lock = multiprocessing.Lock()

    def __handle_shutdown(self, _signum, _frame):
        logging.info("action: exiting_due_to_signal | result: in_progress")
        self._server_must_shutdown = True
        self._server_socket.shutdown(socket.SHUT_RDWR)
        logging.info("action: socket_closing | result: success")

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        while not self._server_must_shutdown:
            client_sock = self.__accept_new_connection()
            if client_sock is not None:
                client_conn_process = multiprocessing.Process(target=self.__handle_client_connection, args=(client_sock,))
                client_conn_process.start()
                
        for client_conn_process in multiprocessing.active_children():
            client_conn_process.join()                




    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        while not self._server_must_shutdown:
            received_msg = self.__receive_message(client_sock)
            if received_msg is None:
                return
            
            if received_msg.startswith(utils.BETS_MSG_HEADER_FROM_CL):
                self.__handle_bet_chunk_msg(client_sock, received_msg)
            elif received_msg.startswith(utils.NOTIFY_MSG_HEADER_FROM_CL):
                self.__handle_notify_msg(client_sock, received_msg)
            elif received_msg.startswith(utils.QUERY_RESULTS_MSG_HEADER_FROM_CL):
                self.__handle_query_results_msg(client_sock, received_msg)

        client_sock.close()


    def __handle_bet_chunk_msg(self, client_sock, received_msg):
        received_chunk_of_bets = utils.decode_bets(received_msg)
        logging.info(f'action: chunk_received | result: success | agency: {received_chunk_of_bets[0].agency} | number_of_bets: {len(received_chunk_of_bets)}')

        with self._storefile_lock:
            utils.store_bets(received_chunk_of_bets)
        self.__send_message(client_sock, utils.CHUNK_RECEIVED_MSG)


    def __handle_notify_msg(self, client_sock, received_msg):
        received_notifier_agency = utils.decode_notify(received_msg)
        logging.info(f'action: notify_received | result: success | agency: {received_notifier_agency}')
        with self._clients_that_notified_completion.get_lock():
            self._clients_that_notified_completion.value += 1
            if self._clients_that_notified_completion.value == utils.NEEDED_AGENCIES_TO_START_LOTTERY:
                logging.info('action: sorteo | result: success')
        self.__send_message(client_sock, utils.ACK_NOTIFY_MSG)


    def __handle_query_results_msg(self, client_sock, received_msg):
        received_query_agency = utils.decode_query_for_results(received_msg)
        logging.info(f'action: results_query_received | result: success | agency: {received_query_agency}')

        with self._clients_that_notified_completion.get_lock():
            if self._clients_that_notified_completion.value != utils.NEEDED_AGENCIES_TO_START_LOTTERY:
                msg_to_send = utils.WAIT_MSG + utils.DELIMITER_AS_STR
            else:
                msg_to_send = utils.RESULTS_MSG_HEADER
                another_had_also_won = False
                with self._storefile_lock:
                    for bet in utils.load_bets():
                        if bet.agency == received_query_agency and utils.has_won(bet):
                            if another_had_also_won:
                                msg_to_send += ","
                            msg_to_send += "{" + utils.RESULT_MSG_INNER_FORMAT.format(bet.document) + "}"
                            another_had_also_won = True
                    msg_to_send += utils.DELIMITER_AS_STR

        self.__send_message(client_sock, msg_to_send)




    def __receive_message(self, client_sock):
        """
        Tries to read a complete message from a specific client.
        It avoids short-reads
        """
        msg = b''
        msg_completely_received = False
        while not msg_completely_received:

            msg_piece = client_sock.recv(utils.KiB)
            if not msg_piece:
                logging.debug('action: receive_message | result: fail | error: connection_closed_by_client')
                client_sock.close()
                return None
            
            msg += msg_piece

            if msg.endswith(utils.DELIMITER):
                msg_completely_received = True

        addr = client_sock.getpeername()
        return msg.decode('utf-8')


    def __send_message(self, client_sock, msg):
        """
        Send a message to a client
        It avoids short-writes
        """
        try:
            client_sock.sendall(msg.encode('utf-8'))
        except OSError as e:
            logging.error(f'action: send_message | result: fail | error: {e}')
            client_sock.close()
        

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        try:
            logging.info('action: accept_connections | result: in_progress')
            c, addr = self._server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        
        except OSError as e:
            return None
