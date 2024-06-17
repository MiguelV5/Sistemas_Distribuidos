import logging
from client import Client
from shared.initializers import init_log, init_configs


def main():
    config_params = init_configs(["LOGGING_LEVEL", "SERVER_IP", "SERVER_PORT", "REVIEWS_FILE_PATH", "BOOKS_FILE_PATH", "BATCH_SIZE", "CLIENT_ID"])
    init_log(config_params["LOGGING_LEVEL"])

    logging.info("Starting client")
    client = Client(server_ip=config_params["SERVER_IP"], 
                    server_port=int(config_params["SERVER_PORT"]), 
                    reviews_file_path=config_params["REVIEWS_FILE_PATH"], 
                    books_file_path=config_params["BOOKS_FILE_PATH"], 
                    batch_size=int(config_params["BATCH_SIZE"]),
                    client_id=int(config_params["CLIENT_ID"]))
    client.start()
    

if __name__ == "__main__":
    main()
