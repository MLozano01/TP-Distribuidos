import logging
import common.config_init as config_init
from utils.utils import config_logger
import common.healthcheck

def main():
    config = config_init.initialize_config()
    config_logger(config["logging_level"])

    try: 
        healthcheck = common.healthcheck.HealthCheck(config)
        healthcheck.start()
    except KeyboardInterrupt:
        logging.info("Filter stopped by user")
        healthcheck.stop()
        logging.info("Filter stopped")
    except Exception as e:
        logging.error(f"Filter error: {e}")
        healthcheck.stop()
        logging.info("Filter stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting filter module")
    main()