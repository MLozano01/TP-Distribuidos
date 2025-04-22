import logging
import common.config_init as config_init
from utils.utils import config_logger
from common.transformer import Transformer

def main():
    config = config_init.initialize_config()
    config_logger(config["logging_level"])

    logging.info("Transformer started")

    try:
        transformer_instance = Transformer(**config)
        transformer_instance.start()
    except KeyboardInterrupt:
        logging.info("Transformer stopped by user")
    except Exception as e:
        logging.error(f"Transformer error: {e}")
    finally:
        if transformer_instance:
            transformer_instance.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Transformer module")
    main()
