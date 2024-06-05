import socket
import docker
import docker.models
import docker.models.containers
import docker.types
from shared.socket_connection_handler import SocketConnectionHandler
import logging
from multiprocessing import Process
import time
from shared.monitorable_process import MonitorableProcess

class HealthChecker(MonitorableProcess):
    def __init__(self):
        super().__init__()
        self.socket_connection_handler = None
       
    def start(self):
        client = docker.from_env()
        containers = client.containers.list()
        logging.info(f"Containers found: {containers}")
        
        
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
        
        
        