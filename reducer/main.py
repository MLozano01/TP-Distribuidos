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
        logging.info("Reducer stopped by user")
        controller.stop()
    except Exception as e:
        logging.error(f"Reducer error: {e}")
        controller.stop()
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Reducer Mdule")
    main()