import logging
import logging
import filter.config_init as config_init
from filter.filter import Filter

def main():
    config = config_init.initialize_config()
    logging.basicConfig(level=config["logging_level"])
    logging.info("Filter started")

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