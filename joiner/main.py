import logging
import common.config_init as config_init
from protocol.utils.logger import config_logger
import common.controller

def main():
    try:
        config, comms_config = config_init.initialize_config()
        config_logger(config["logging_level"])

        controller = common.controller.Controller(config, comms_config)
        controller.start()

    except KeyboardInterrupt:
        logging.info("Joiner stopped by user")
        controller.stop()
    except Exception as e:
        logging.error(f"Joiner error: {e}", exc_info=True)
        controller.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
    logging.info("Starting Joiner module")
    main() 