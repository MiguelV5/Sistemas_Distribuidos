from health_checker import HealthChecker
from shared.initializers import init_configs
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "HEALTH_CHECK_INTERVAL", "HEALTH_CHECK_TIMEOUT", "CONTROLLER_NAME", "NUM_OF_HEALTH_CHECKERS"])
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',level=config_params["LOGGING_LEVEL"],datefmt='%Y-%m-%d %H:%M:%S')
    logging.info("Starting Health Checker")
    health_checker = HealthChecker(int(config_params["HEALTH_CHECK_INTERVAL"]), 
                                   int(config_params["HEALTH_CHECK_TIMEOUT"]),
                                   config_params["CONTROLLER_NAME"], 
                                   int(config_params["NUM_OF_HEALTH_CHECKERS"]))
                                      
    health_checker.start()
    
if __name__ == "__main__":
    main()
    