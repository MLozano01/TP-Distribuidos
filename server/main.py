
import logging
from common.server import Server
from common.config_init import initialize_config
from utils.utils import config_logger

def main():

    config = initialize_config()
    config_logger(config["logging_level"])

    server = Server(config["port"], config["listen_backlog"])
    logging.info("Server started")
    try:
        server.run()
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
    except Exception as e:
        logging.error(f"Server error: {e}")
    finally:
        server.end_server()

if __name__ == "__main__":
    main()