import logging
from common.config_init import initialize_config
from common.data_controller import DataController
from common.communicator import Communicator
from multiprocessing import Process


def main():
    config = initialize_config()
    logging.basicConfig(level=config["logging_level"])
    
    try:
        comms = Communicator(config['port'])
        config.pop('port')

        comms_process = Process(target=comms.start, args=())
        comms_process.start()

        data_controller = DataController(**config)
        data_controller.run()
    except KeyboardInterrupt:
        logging.info("DataController stopped by user")
    except Exception as e:
        logging.error(f"DataController error: {e}")
    finally:
        comms_process.terminate()
        data_controller.stop()
        comms_process.join()
        logging.info("DataController stopped")


if __name__ == "__main__":
    main()