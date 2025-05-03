import logging
from common.aggregator import Aggregator
import common.config_init as config_init
from utils.utils import config_logger

def main():
    config = config_init.initialize_config()
    config_logger(config["logging_level"])

    try: 
        aggregator_instance = Aggregator(**config)
        aggregator_instance.run()
    except KeyboardInterrupt:
        logging.info("Aggregator stopped by user")
    except Exception as e:
        logging.error(f"Aggregator error: {e}")
    finally:
        aggregator_instance.stop()
        logging.info("Aggregator stopped")

    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting aggregator module")
    main()