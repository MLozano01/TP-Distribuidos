import common.controller
import logging
import common.config_init as config_init
from utils.utils import config_logger

def main():
    config, comms = config_init.initialize_config()
    config_logger(config["logging_level"])

    try: 
        controller = common.controller.Controller(config, comms)
        controller.start()
    except KeyboardInterrupt:
        logging.info("Aggregator stopped by user")
    except Exception as e:
        logging.error(f"Aggregator error: {e}")
    finally:
        controller.stop()
        logging.info("Aggregator stopped")

    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting aggregator module")
    main()