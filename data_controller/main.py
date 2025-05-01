import logging
from common.config_init import initialize_config
from common.data_controller import DataController

def main():
    config = initialize_config()
    logging.basicConfig(level=config["logging_level"])
    controller = DataController(**config)
    logging.info("DataController started")

if __name__ == "__main__":
    main()