import logging
import common.config_init as config_init
from common.logger import config_logger
from common.communicator import Communicator
from common.transformer import Transformer
from multiprocessing import Process


def main():
    config = config_init.initialize_config()
    config_logger(config["logging_level"])

    logging.info("Transformer started")

    try:
        comms = Communicator(config['port'])
        config.pop('port')

        comms_process = Process(target=comms.start, args=())
        comms_process.start()

        transformer = Transformer(**config)
        transformer.start()
    except KeyboardInterrupt:
        logging.info("Transformer stopped by user")
    except Exception as e:
        logging.error(f"Transformer error: {e}")
    finally:
        if comms_process:
            comms_process.terminate()
            comms_process.join()
        if transformer:
            transformer.stop()
        logging.info("Transformer stopped")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Transformer module")
    main()
