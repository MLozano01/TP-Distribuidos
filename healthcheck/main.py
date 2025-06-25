import logging
from healthcheck.common import config_init as config_init
from protocol.utils.logger import config_logger
from healthcheck.common import healthcheck as hc_module

def main():
    config = config_init.config_healthcheck()
    config_logger(config["logging_level"])

    config.pop("logging_level")

    try: 
        healthcheck = hc_module.HealthCheck(config)
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