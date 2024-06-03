import socket
import docker
import docker.models
import docker.models.containers
import docker.types
from shared.socket_connection_handler import SocketConnectionHandler
import signal
import logging
from multiprocessing import Process
import time

class HealthChecker:
    def __init__(self):
        self.joinable_processes = [Process]
        self.socket_connection_handler = None
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down Health Checker")
        for process in self.joinable_processes:
            process.terminate()
       
    def start(self):
        client = docker.from_env()
        containers = client.containers.list()
        logging.info(f"Containers found: {containers}")
        
        p = Process(target=self.__handle_incoming_health_check)
        p.start()
        
        for container in containers:
            p = Process(target=self.__check_container_health, args=(container,))
            self.joinable_processes.append(p)
            p.start()
            
    def __check_container_health(self, container: docker.models.containers.Container):
        while True:
            try:
                container_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                logging.info(f"Connecting to container {container.name}")
                container_socket.connect((container.name, 5000))
                container_socket.settimeout(0.5)
                self.socket_connection_handler = SocketConnectionHandler(container_socket)
                self.socket_connection_handler.send_message("health")
                response = self.socket_connection_handler.read_message()
                if response != "ok":
                    logging.error(f"Container {container.name} is not healthy")
                    self.__revive_container(container)
                else:
                    logging.info(f"Container {container.name} is healthy")
            except Exception as e:
                logging.error(f"Failed to connect to container {container.name}: {str(e)}")
                self.__revive_container(container)
            
            time.sleep(1)
    
    def __revive_container(self, container: docker.models.containers.Container):
        container.start()
        logging.info(f"Container {container.name} has been restarted")
        
    def __handle_incoming_health_check(self):
        logging.info("Starting to receive health checks")
        self.listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        current_container_name = docker.from_env().containers.get(socket.gethostname()).name
        logging.info(f"Current container name: {current_container_name}")
        
        self.listening_socket.bind((current_container_name, 5000))
        self.listening_socket.listen()
        while True:
            client_socket, _ = self.listening_socket.accept()
            self.socket_connection_handler = SocketConnectionHandler(client_socket)
            message = self.socket_connection_handler.read_message()
            if message == "health":
                self.socket_connection_handler.send_message("ok")
            self.socket_connection_handler.close()
        
        
        