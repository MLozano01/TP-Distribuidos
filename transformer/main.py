import logging
from common.controller import Controller
import common.config_init as config_init
from utils.utils import config_logger

def main():
    config, comms = config_init.initialize_config()
    config_logger(config["logging_level"])

    logging.info("Transformer started")

    try:
        controller = Controller(config, comms)
        controller.start()
    except KeyboardInterrupt:
        logging.info("Transformer stopped by user")
    except Exception as e:
        logging.error(f"Transformer error: {e}")
    finally:
        if controller:
            controller.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Transformer module")
    main()
