from health_checker import HealthChecker
from shared.initializers import init_configs
from shared.initializers import init_log
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL"])
    logging.basicConfig(level=config_params["LOGGING_LEVEL"])
    logging.info("Starting Health Checker")
    health_checker = HealthChecker()
    health_checker.start()
    
if __name__ == "__main__":
    main()
    