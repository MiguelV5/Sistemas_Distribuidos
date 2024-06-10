import signal
import socket
from shared.protocol_messages import SystemMessage, SystemMessageType
from shared.socket_connection_handler import SocketConnectionHandler
import logging
from multiprocessing import Process
from shared.mq_connection_handler import MQConnectionHandler
from typing import Optional
import json
from shared.atomic_writer import AtomicWriter


HEALTH_CHECK_PORT = 5000

class MonitorableProcess:
    def __init__(self, worker_name: str, worker_id: int = 1):
        self.worker_name = worker_name
        # defaults to 1, as there are processes that are not replicated
        self.worder_id = worker_id
        self.health_check_connection_handler: Optional[SocketConnectionHandler] = None
        self.mq_connection_handler: Optional[MQConnectionHandler] = None
        self.joinable_processes: list[Process] = []
        self.state_file_path = f"/{worker_name}_state.json"
        self.state = self.__load_state_file()
        p = Process(target=self.__accept_incoming_health_checks)
        self.joinable_processes.append(p)
        p.start()
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        if self.health_check_connection_handler:
            self.health_check_connection_handler.close()
        if self.mq_connection_handler:
            self.mq_connection_handler.close_connection()
        for process in self.joinable_processes:
            process.terminate()       


    def __accept_incoming_health_checks(self):
        logging.info("[MONITORABLE] Starting to receive health checks")
        self.listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        self.listening_socket.bind((self.worker_name, HEALTH_CHECK_PORT))
        self.listening_socket.listen()
        while True:
            client_socket, _ = self.listening_socket.accept()
            self.health_check_connection_handler = SocketConnectionHandler.create_from_socket(client_socket)
            message = self.health_check_connection_handler.read_message()
            if message == SystemMessage(SystemMessageType.HEALTH_CHECK, worker_id=self.worder_id).encode_to_str():
                self.health_check_connection_handler.send_message(SystemMessage(SystemMessageType.ALIVE, worker_id=self.worder_id).encode_to_str())
            self.health_check_connection_handler.close()
            
    def __load_state_file(self) -> dict:
        try:
            with open(self.state_file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        
    def save_state_file(self, state: dict):
        writer = AtomicWriter(self.state_file_path)
        writer.write(json.dumps(state))                          