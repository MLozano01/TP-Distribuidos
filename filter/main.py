import logging
import common.config_init as config_init
from utils.utils import config_logger
from common.filter import Filter

def main():
    config = config_init.initialize_config("/filter_by_arg_spa.ini")
    config_logger(config["logging_level"])

    logging.info("Filter started")

    config.pop("logging_level", None)

    for filter in config:
        if filter.startswith("filter_"):
            logging.info(f"Filter {filter} is set")

    try:
        filter_instance = Filter(**config)
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