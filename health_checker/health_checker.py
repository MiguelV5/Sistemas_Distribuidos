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
        # Read container names from file containers_data.txt
        containers = []
        with open('/containers_data.txt', 'r') as file:
            containers = file.read().splitlines()
        
        for container in containers:
            p = Process(target=self.__check_container_health, args=(container,))
            self.joinable_processes.append(p)
            p.start()
            
    def __check_container_health(self, container: str):
        while True:
            try:
                container_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                logging.debug(f"Connecting to container {container}")
                container_socket.connect((container, 5000))
                container_socket.settimeout(1)
                self.socket_connection_handler = SocketConnectionHandler(container_socket)
                self.socket_connection_handler.send_message("health")
                response = self.socket_connection_handler.read_message()
                if response != "ok":
                    logging.error(f"Container {container} is not healthy")
                    self.__revive_container(container)
                else:
                    logging.debug(f"Container {container} is healthy")
            except Exception as e:
                logging.error(f"Failed to connect to container {container}: {str(e)}")
                self.__revive_container(container)
            
            time.sleep(5)
    
    def __revive_container(self, container: str):
        docker.APIClient().start(container)
        logging.info(f"Container {container} has been restarted")
        
        
        