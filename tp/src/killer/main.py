import logging
from shared.initializers import init_log 
from killer import Killer
from argparse import ArgumentParser

def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--interval", type=int, required=True)
    parser.add_argument("--kill_percentage", type=int, required=True)
    parser.add_argument("--num_of_health_checkers", type=int, required=True)
    return parser.parse_args()

def main():
    config_params = parse_args()
    init_log("INFO")
    logging.info("Starting Killer")
    killer = Killer(int(config_params.interval), 
                    int(config_params.kill_percentage),
                    int(config_params.num_of_health_checkers))
    killer.start()

if __name__ == "__main__":
    main()