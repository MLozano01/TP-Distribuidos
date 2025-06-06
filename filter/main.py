import logging
import common.config_init as config_init
from protocol.utils.logger import config_logger
import common.controller

def main():
    config, comms = config_init.config_filter()
    config_logger(config["logging_level"])

    try: 
        controller = common.controller.Controller(config, comms)
        controller.start()
    except KeyboardInterrupt:
        logging.info("Filter stopped by user")
        controller.stop()
        logging.info("Filter stopped")
    except Exception as e:
        logging.error(f"Filter error: {e}")
        controller.stop()
        logging.info("Filter stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting filter module")
    main()