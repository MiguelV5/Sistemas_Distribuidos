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
        self.state: dict[str, dict[str, dict[str, int]]] = self.__load_state_file()
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
        
    def generic_callback(self, ch, method, properties, body, actual_callback):
        msg = SystemMessage.decode_from_bytes(body)
        last_message_seq_num = self.state.get("last_message_state", {}).get(msg.client_id, {}).get(msg.controller_name, 0)
            
        if msg.controller_seq_num == last_message_seq_num:
            logging.debug(f"Duplicate message: {msg}")
            ch.basic_nack(delivery_tag=method.delivery_tag)
        else:
            # Call the actual callback of the controller
            actual_callback(ch, method, properties, msg)
            #Update last seq num from sender controller
            self.state.update({"last_message_state": {msg.client_id: {msg.controller_name: msg.controller_seq_num}}})
            # Buffer state should be handled by the actual callback if needed as it is specific to the controller
            # Also for the self seq number, it should be handled by the actual callback as it can send multiple messages from a chunk
            # Update the state file only after the actual callback has been executed
            # Ack only after the state is stored to avoid losing messages, it can result in handling the same message twice but it ensures no message is lost as the state is updated correctly
            self.save_state_file()
            ch.basic_ack(delivery_tag=method.delivery_tag)                  
            
            
    def get_next_seq_number(self, client_id: int, controller_name: str) -> int:
        last_message_seq_num = self.state.get("last_message_state", {}).get(client_id, {}).get(controller_name, 0)
        return last_message_seq_num + 1
    
    def update_self_seq_number(self, client_id: int, seq_num: int):
        self.state.update({client_id: {self.controller_name: seq_num}})
