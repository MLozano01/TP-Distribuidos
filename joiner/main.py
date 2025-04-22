import logging
import common.config_init as config_init
from utils.utils import config_logger
from common.joiner import Joiner

def main():
    joiner_instance = None # Define outside try for finally block
    try:
        config = config_init.initialize_config()
        config_logger(config["logging_level"])

        replica_id = config.get("replica_id", "N/A") # Get replica ID for logging
        logging.info(f"Joiner Replica {replica_id} started")

        joiner_instance = Joiner(**config)
        joiner_instance.start() # This will block until stopped

    except KeyboardInterrupt:
        logging.info(f"Joiner Replica {replica_id} stopped by user")
    except Exception as e:
        logging.error(f"Joiner Replica {replica_id} error: {e}", exc_info=True)
    finally:
        if joiner_instance:
            joiner_instance.stop()
        logging.info(f"Joiner Replica {replica_id} shutdown complete.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
    logging.info("Starting Joiner module")
    main() 