import logging
import common.config_init as config_init
from utils.utils import config_logger
import common.controller

def main():
    config = config_init.initialize_config()
    config_logger(config["logging_level"])

    try: 
        controller = common.controller.Controller(config)
        controller.start()
    except KeyboardInterrupt:
        logging.info("Filter stopped by user")
    except Exception as e:
        logging.error(f"Filter error: {e}")
    finally:
        controller.stop()
        logging.info("Filter stopped")
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting filter module")
    main()