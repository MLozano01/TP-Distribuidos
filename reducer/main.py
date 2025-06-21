import logging
import common.communicator
import common.config_init as config_init
from protocol.utils.logger import config_logger
import common.reducer, common.communicator
from common.state_persistence import StatePersistence

from multiprocessing import Process

def main():
    config = config_init.config_reducer()
    config_logger(config["logging_level"])
    config.pop("logging_level")

    state_manager = StatePersistence(config['backup_file'], serializer="json")
    backup_info = state_manager.load(default_factory=dict)
    logging.info(f"Backup Info: {backup_info}")

    try: 
        comms = common.communicator.Communicator(config['port'])
        config.pop('port')

        comms_process = Process(target=comms.start, args=())
        comms_process.start()

        red = common.reducer.Reducer(config, backup_info, state_manager)
        red.start()
        comms_process.join()
    except KeyboardInterrupt:
        logging.info("Reducer stopped by user")
        comms_process.terminate()
        red.stop()
    except Exception as e:
        logging.error(f"Reducer error: {e}")
        comms_process.terminate()
        red.stop()
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Reducer Mdule")
    main()