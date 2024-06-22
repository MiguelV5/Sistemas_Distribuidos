import logging
from shared.initializers import init_configs
from shared.initializers import init_log
from killer import Killer

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INTERVAL", "KILL_PERCENTAGE", "NUM_OF_HEALTH_CHECKERS"])
    init_log(config_params["LOGGING_LEVEL"])
    logging.info("Starting Killer")
    killer = Killer(int(config_params["INTERVAL"]), 
                    int(config_params["KILL_PERCENTAGE"]),
                    int(config_params["NUM_OF_HEALTH_CHECKERS"]))
    killer.start()
