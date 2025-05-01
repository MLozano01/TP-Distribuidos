import logging
from common.config_init import initialize_config
from common.data_controller import DataController

def main():
    config = initialize_config()
    logging.basicConfig(level=config["logging_level"])
    controller = DataController(**config)
    logging.info("DataController started")
    try:
        controller.start()
    except KeyboardInterrupt:
        logging.info("DataController stopped by user")
    except Exception as e:
        logging.error(f"DataController error: {e}")
    finally:
        controller.stop()

if __name__ == "__main__":
    main()