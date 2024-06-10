from health_checker import HealthChecker
from shared.initializers import init_configs
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "HEALTH_CHECK_INTERVAL", "HEALTH_CHECK_TIMEOUT", "WORKER_ID"])
    logging.basicConfig(level=config_params["LOGGING_LEVEL"])
    logging.info("Starting Health Checker")
    health_checker = HealthChecker(config_params["HEALTH_CHECK_INTERVAL"], 
                                   config_params["HEALTH_CHECK_TIMEOUT"],
                                   config_params["WORKER_ID"])
    health_checker.start()
    
if __name__ == "__main__":
    main()
    