import logging
import common.config_init as config_init
from common.logger import config_logger
from common.communicator import Communicator
from common.aggregator import Aggregator
from multiprocessing import Process

def main():
    config = config_init.initialize_config()
    config_logger(config["logging_level"])

    try: 
        comms = Communicator(config['port'])
        config.pop('port')

        comms_process = Process(target=comms.start, args=())
        comms_process.start()

        aggregator = Aggregator(**config)
        aggregator.run()
    except KeyboardInterrupt:
        logging.info("Aggregator stopped by user")
    except Exception as e:
        logging.error(f"Aggregator error: {e}")
    finally:
        comms_process.terminate()
        aggregator.stop()
        comms_process.join()
        logging.info("Aggregator stopped")

    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting aggregator module")
    main()