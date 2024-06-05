import signal
import socket
import docker
import docker.models
import docker.models.containers
import docker.types
from shared.socket_connection_handler import SocketConnectionHandler
import logging
from multiprocessing import Process
from shared.mq_connection_handler import MQConnectionHandler
from typing import Optional

class MonitorableProcess:
    def __init__(self):
        self.health_check_connection_handler: Optional[SocketConnectionHandler] = None
        self.mq_connection_handler: Optional[MQConnectionHandler] = None
        self.joinable_processes: list[Process] = []
        p = Process(target=self.__acept_incoming_health_checks)
        self.joinable_processes.append(p)
        p.start()
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down...")
        if self.health_check_connection_handler:
            self.health_check_connection_handler.close()
        if self.mq_connection_handler:
            self.mq_connection_handler.close_connection()
        for process in self.joinable_processes:
            process.terminate()       
    
    def __acept_incoming_health_checks(self):
        logging.info("Starting to receive health checks")
        self.listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        current_container_name = docker.from_env().containers.get(socket.gethostname()).name
        logging.info(f"Current container name: {current_container_name}")
        
        self.listening_socket.bind((current_container_name, 5000)) # Port can be configured
        self.listening_socket.listen()
        while True:
            client_socket, _ = self.listening_socket.accept()
            self.health_check_connection_handler = SocketConnectionHandler(client_socket)
            message = self.health_check_connection_handler.read_message()
            if message == "health":
                self.health_check_connection_handler.send_message("ok")
            self.health_check_connection_handler.close()