import logging
from common.controller import Controller
from common.config_init import initialize_config
from common.data_controller import DataController

def main():
    config, movies_comm_config, credits_comm_config, ratings_comm_config = initialize_config()
    logging.basicConfig(level=config["logging_level"])
    
    try:
        controller = Controller(config, movies_comm_config, credits_comm_config, ratings_comm_config)
        controller.start()
    except KeyboardInterrupt:
        logging.info("DataController stopped by user")
    except Exception as e:
        logging.error(f"DataController error: {e}")
    finally:
        controller.stop()

if __name__ == "__main__":
    main()