import signal
import docker
import random
import time
import logging

CONTROLLERS_NAMES_PATH = '/monitorable_controllers.txt'

class Killer:
    def __init__(self, interval, kill_percentage, num_of_healthcheckers):
        self.interval = interval
        self.kill_percentage = kill_percentage
        self.max_health_checkers_to_kill = num_of_healthcheckers - 1
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down killer")
        

    def start(self):
        controllers = []
        with open(CONTROLLERS_NAMES_PATH, 'r') as file:
            controllers = file.read().splitlines()
        while True:
            health_checkers_to_kill = 0
            controllers_to_kill = []
            for controller in controllers:
                if random.randint(0, 100) < self.kill_percentage:
                    # do not kill all healthcheckers
                    if controller.startswith("health_checker"):
                        if health_checkers_to_kill < self.max_health_checkers_to_kill:
                            health_checkers_to_kill += 1
                            controllers_to_kill.append(controller)
                    else:
                        controllers_to_kill.append(controller)
            time.sleep(self.interval)
     
            logging.info(f"\n \n \t \t <<< KILLING NODES >>>:\n  {controllers_to_kill}\n")
            for controller in controllers_to_kill:
                try: 
                    docker.APIClient().kill(container=controller)
                except Exception as e:
                    logging.error(f"Failed to kill controller {controller}: {str(e)}")       