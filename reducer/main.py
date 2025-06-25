import logging
import common_reducer.config_init as config_init
from common.logger import config_logger
from common_reducer import reducer
from common.state_persistence import StatePersistence
from common.communicator import Communicator


from multiprocessing import Process

def main():
    config = config_init.config_reducer()
    config_logger(config["logging_level"])
    config.pop("logging_level")

    state_manager = StatePersistence(config['backup_file'], node_info=config['reducer_name'], serializer="json")
    partial_result_backup_info = state_manager.load(default_factory=dict)
    logging.info(f"Partial Result Backup Info: {partial_result_backup_info}")

    secuence_number_backup_info = state_manager.load_saved_secuence_number_data()
    logging.info(f"Secuence Number Backup Info: {secuence_number_backup_info}")

    try: 
        comms = Communicator(config['port'])
        config.pop('port')

        comms_process = Process(target=comms.start, args=())
        comms_process.start()

        red = reducer.Reducer(config, partial_result_backup_info, secuence_number_backup_info, state_manager)
        red.start()

    except KeyboardInterrupt:
        logging.info("Reducer stopped by user")
    except Exception as e:
        logging.error(f"Reducer error: {e}")
    finally:
        comms_process.terminate()
        red.stop()
        comms_process.join()
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Reducer Mdule")
    main()