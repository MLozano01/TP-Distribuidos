import logging
import os
import sys

# Add the parent directory of 'joiner' to the Python path
# This is to allow imports from 'protocol' and other packages in the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from config.config_init import initialize_config
from logic.strategy_factory import get_join_strategy
from joiner_node import JoinerNode
from protocol.utils.logger import config_logger

def main():
    joiner_instance = None
    try:
        config = initialize_config()
        config_logger(config["logging_level"])

        replica_id = config.get("replica_id", "N/A")
        logging.info(f"Joiner Replica {replica_id} started")
        
        # Get the join strategy from the factory
        join_strategy_name = config.get("other_data_type", "RATINGS")
        join_strategy = get_join_strategy(join_strategy_name)

        joiner_instance = JoinerNode(config, join_strategy)
        joiner_instance.start()

    except KeyboardInterrupt:
        logging.info(f"Joiner Replica {replica_id} stopped by user")
    except Exception as e:
        logging.error(f"Joiner Replica {replica_id} error: {e}", exc_info=True)
    finally:
        if joiner_instance:
            joiner_instance.stop()
        logging.info(f"Joiner Replica {replica_id} shutdown complete.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
    logging.info("Starting Joiner module")
    main() 