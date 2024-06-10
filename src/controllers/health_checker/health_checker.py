import docker
import docker.models
import docker.models.containers
import docker.types
from shared.protocol_messages import SystemMessage, SystemMessageType
from shared.socket_connection_handler import SocketConnectionHandler
import logging
from multiprocessing import Process
import time
from shared.monitorable_process import MonitorableProcess

# Not an env var due to docker pathing within image
CONTROLLERS_NAMES_PATH = '/monitorable_controllers.txt'
HEALTH_CHECK_PORT = 5000

class HealthChecker(MonitorableProcess):
    def __init__(self, health_check_interval: int, health_check_timeout: int, worker_name: str, worker_id: int):
        super().__init__(worker_name, worker_id)
        self.health_check_interval = health_check_interval
        self.health_check_timeout = health_check_timeout
        self.worker_id = worker_id
        self.worker_name = worker_name
        self.socket_connection_handler = None
       
    def start(self):
        controllers = []
        with open(CONTROLLERS_NAMES_PATH, 'r') as file:
            controllers = file.read().splitlines()
        for controller in controllers:
            if controller == self.worker_name: 
                continue
            p = Process(target=self.__check_controllers_health, args=(controller,))
            self.joinable_processes.append(p)
            p.start()
            

    def __check_controllers_health(self, container: str):
        while True:
            try:
                logging.debug(f"Connecting to container {container}")
                self.socket_connection_handler = SocketConnectionHandler.connect_and_create(container, HEALTH_CHECK_PORT, self.health_check_timeout)

                msg = SystemMessage(SystemMessageType.HEALTH_CHECK, worker_id=self.worker_id)
                self.socket_connection_handler.send_message(msg.encode_to_str())
                response = self.socket_connection_handler.read_message_raw()
                response_msg = SystemMessage.decode_from_bytes(response)
                if response_msg.msg_type != SystemMessageType.ALIVE:
                    logging.error(f"Container {container} is not healthy")
                    self.__revive_controller(container)
                else:
                    logging.debug(f"Container {container} is healthy")
            except Exception as e:
                logging.error(f"Failed to connect to container {container}: {str(e)}")
                self.__revive_controller(container)
            
            time.sleep(self.health_check_interval)
    
    def __revive_controller(self, controller: str):
        docker.APIClient().start(controller)
        logging.info(f"Container {controller} has been restarted")
        
        
        