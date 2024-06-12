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
    def __init__(self, controller_name: str):
        self.controller_name = controller_name
        self.health_check_connection_handler: Optional[SocketConnectionHandler] = None
        self.mq_connection_handler: Optional[MQConnectionHandler] = None
        self.joinable_processes: list[Process] = []
        self.state_file_path = f"/{controller_name}_state.json"
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
        
        self.listening_socket.bind((self.controller_name, HEALTH_CHECK_PORT))
        self.listening_socket.listen()
        while True:
            client_socket, _ = self.listening_socket.accept()
            self.health_check_connection_handler = SocketConnectionHandler.create_from_socket(client_socket)
            message = SystemMessage.decode_from_bytes(self.health_check_connection_handler.read_message_raw())

            if message.msg_type == SystemMessageType.HEALTH_CHECK:
                alive_msg = SystemMessage(msg_type=SystemMessageType.ALIVE, client_id=0, controller_name=self.controller_name, controller_seq_num=0).encode_to_str()
                self.health_check_connection_handler.send_message(alive_msg)
            self.health_check_connection_handler.close()
            

    def __load_state_file(self) -> dict:
        try:
            with open(self.state_file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        
    def save_state_file(self):
        writer = AtomicWriter(self.state_file_path)
        writer.write(json.dumps(self.state))                          
            
