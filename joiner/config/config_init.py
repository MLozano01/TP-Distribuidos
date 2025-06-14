from configparser import ConfigParser
import os
import logging

CONFIG_FILE = "config.ini"

def initialize_config():
    """ Parse env variables or config file to find program config params."""
    config = ConfigParser(os.environ)
    config.read(CONFIG_FILE)

    config_params = {}
    try:
        # General Config
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["other_data_type"] = os.getenv('OTHER_DATA_TYPE', config["DEFAULT"]["OTHER_DATA_TYPE"]).upper() # Read and uppercase

        # Validate OTHER_DATA_TYPE
        allowed_types = ["RATINGS", "CREDITS"]
        if config_params["other_data_type"] not in allowed_types:
            raise ValueError(f"Invalid OTHER_DATA_TYPE '{config_params['other_data_type']}'. Must be one of {allowed_types}")

        # Replica Info (Injected by Docker Compose)
        # Handles explicit JOINER_REPLICA_ID=0 for single replica,
        # and converts 1-based Task.Slot for multiple replicas.
        replica_id_env = os.getenv('JOINER_REPLICA_ID')
        if replica_id_env is not None:
            try:
                replica_id_int = int(replica_id_env)
                if replica_id_int == 0: # Explicitly set ID=0 for single replica case
                    config_params["replica_id"] = 0
                elif replica_id_int > 0: # Assume > 0 means 1-based Task.Slot from Docker Swarm
                    config_params["replica_id"] = replica_id_int - 1 # Convert 1-based to 0-based index
                    # Double-check just in case the calculation somehow went wrong
                    if config_params["replica_id"] < 0:
                        raise ValueError(f"Calculated negative replica ID ({config_params['replica_id']}) from env value {replica_id_int}")
                else: # Negative value from env is invalid
                    raise ValueError(f"Invalid negative JOINER_REPLICA_ID from env: {replica_id_int}")
            except ValueError as e_int:
                # Catch potential errors from int() conversion
                raise ValueError(f"Could not parse JOINER_REPLICA_ID from env '{replica_id_env}': {e_int}")
        else:
             # Default to 0 if env var is not set (e.g., running outside compose)
             config_params["replica_id"] = 0

        # config_params["replica_count"] = int(os.getenv('JOINER_REPLICA_COUNT', 1))

        # RabbitMQ Config
        config_params["rabbit_host"] = os.getenv('RABBIT_HOST', config["RABBITMQ"]["RABBIT_HOST"])

        config_params["exchange_movies"] = os.getenv('EXCHANGE_MOVIES', config["RABBITMQ"]["EXCHANGE_MOVIES"])
        config_params["queue_movies_base"] = os.getenv('QUEUE_MOVIES_BASE_NAME', config["RABBITMQ"]["QUEUE_MOVIES_BASE_NAME"])

        config_params["exchange_other"] = os.getenv('EXCHANGE_OTHER', config["RABBITMQ"]["EXCHANGE_OTHER"])
        config_params["queue_other_base"] = os.getenv('QUEUE_OTHER_BASE_NAME', config["RABBITMQ"]["QUEUE_OTHER_BASE_NAME"])
        # config_params["other_data_type"] = os.getenv('OTHER_DATA_TYPE', config["RABBITMQ"].get("OTHER_DATA_TYPE")) # Example if needed

        config_params["exchange_control"] = os.getenv('EXCHANGE_CONTROL', config["RABBITMQ"]["EXCHANGE_CONTROL"])

        config_params["exchange_output"] = os.getenv('EXCHANGE_OUTPUT', config["RABBITMQ"]["EXCHANGE_OUTPUT"])
        config_params["queue_output_name"] = os.getenv('QUEUE_OUTPUT_NAME', config["RABBITMQ"]["QUEUE_OUTPUT_NAME"])
        config_params["routing_key_output"] = os.getenv('ROUTING_KEY_OUTPUT', config["RABBITMQ"]["ROUTING_KEY_OUTPUT"])

    except KeyError as e:
        raise KeyError(f"Key was not found. Error: {e}. Aborting server")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}. Aborting server")

    # Construct specific queue names based on replica ID
    replica_id = config_params["replica_id"]
    config_params["queue_movies_name"] = f"{config_params['queue_movies_base']}{replica_id}"
    config_params["queue_other_name"] = f"{config_params['queue_other_base']}{replica_id}"
    # Control queue name is generated dynamically in Joiner class

    logging.info(f"Joiner Config Initialized for {config_params['other_data_type']} join. Replica ID (0-based): {config_params['replica_id']}")
    # logging.debug(f"Full Joiner Config: {config_params}")
    return config_params 