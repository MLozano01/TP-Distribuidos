import logging
import common.config_init as config_init
from common.logger import config_logger
from multiprocessing import Process
from protocol.protocol import Protocol
from common.communicator import Communicator
from common.filter import Filter
from common.publisher.single_publisher import SinglePublisher
from common.publisher.sharded_publisher import ShardedPublisher



def main():
    config = config_init.config_filter()
    config_logger(config["logging_level"])

    try: 
        comms = Communicator(config['port'])
        config.pop('port')

        comms_process = Process(target=comms.start, args=())
        comms_process.start()

        publisher_instance = get_publisher_instance(config)

        filter = Filter(publisher_instance, **config)
        filter.run()

    except KeyboardInterrupt:
        logging.info("Filter stopped by user")
    except Exception as e:
        logging.error(f"Filter error: {e}")
    finally:
        comms_process.terminate()
        filter.stop()
        comms_process.join()
        logging.info("Filter stopped")


def get_publisher_instance(config):
    protocol = Protocol()
    if config.get("publish_to_joiners", False):
        publisher_instance = ShardedPublisher(
            protocol=protocol,
            exchange_snd_ratings=config["exchange_snd_ratings"],
            exc_snd_type_ratings=config["exc_snd_type_ratings"],
            exchange_snd_credits=config["exchange_snd_credits"],
            exc_snd_type_credits=config["exc_snd_type_credits"],
            q_name_credits=config["queue_snd_name_credits"],
            q_name_ratings=config["queue_snd_name_ratings"]
        )
    else:
        publisher_instance = SinglePublisher(
            protocol=protocol,
            exchange_snd=config["exchange_snd"],
            queue_snd_name=config["queue_snd_name"],
            routing_snd_key=config["routing_snd_key"],
            exc_snd_type=config["exc_snd_type"],
        )

    return publisher_instance

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting filter module")
    main()