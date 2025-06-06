import logging
import common.config_init as config_init
from protocol.utils.logger import config_logger
import common.healthcheck

def main():
    config = config_init.config_healthcheck()
    config_logger(config["logging_level"])

    config.pop("logging_level")

    try: 
        healthcheck = common.healthcheck.HealthCheck(config)
        healthcheck.run()
    except KeyboardInterrupt:
        logging.info("Healthcheck stopped by user")
        healthcheck.stop()
        logging.info("Healthcheck stopped")
    except Exception as e:
        logging.error(f"Healthcheck error: {e}")
        healthcheck.stop()
        logging.info("Healthcheck stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Healthcheck module")
    main()