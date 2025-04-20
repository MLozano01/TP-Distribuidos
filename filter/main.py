import logging
import common.config_init as config_init
from utils.utils import config_logger
from common.filter import Filter

def main():
    config = config_init.initialize_config()
    config_logger(config["logging_level"])

    config_params = config_init.config_init("/filter_2000_movies.ini")
    logging.info("Filter started")

    try:
        filter_instance = Filter(**config_params)
        filter_instance.start()
    except KeyboardInterrupt:
        logging.info("Filter stopped by user")
    except Exception as e:
        logging.error(f"Filter error: {e}")
    finally:
        filter_instance.end_filter()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting filter module")
    main()