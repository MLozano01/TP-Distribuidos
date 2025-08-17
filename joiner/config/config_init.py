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
        config_params["joiner_name"] = os.getenv('JOINER_NAME', config["DEFAULT"]["JOINER_NAME"])
        config_params["other_data_type"] = os.getenv('OTHER_DATA_TYPE', config["DEFAULT"]["OTHER_DATA_TYPE"]).upper()
        config_params['hc_port'] = int(os.getenv('HC_PORT', config['DEFAULT']['HC_PORT']))
        config_params['listen_backlog'] = int(os.getenv('LISTEN_BACKLOG', config['DEFAULT']['LISTEN_BACKLOG']))

        # Replica Info
        config_params["replica_id"] = int(os.getenv('JOINER_REPLICA_ID', config['DEFAULT'].get('JOINER_REPLICA_ID', 0)))
        config_params["replicas_count"] = int(os.getenv('JOINER_REPLICA_COUNT', config['DEFAULT'].get('JOINER_REPLICA_COUNT', 1)))

        # RabbitMQ Config
        config_params["rabbit_host"] = os.getenv('RABBIT_HOST', config["RABBITMQ"]["RABBIT_HOST"])

        config_params["exchange_movies"] = os.getenv('EXCHANGE_MOVIES', config["RABBITMQ"]["EXCHANGE_MOVIES"])
        config_params["queue_movies_base"] = os.getenv('QUEUE_MOVIES_BASE_NAME', config["RABBITMQ"]["QUEUE_MOVIES_BASE_NAME"])

        config_params["exchange_other"] = os.getenv('EXCHANGE_OTHER', config["RABBITMQ"]["EXCHANGE_OTHER"])
        config_params["queue_other_base"] = os.getenv('QUEUE_OTHER_BASE_NAME', config["RABBITMQ"]["QUEUE_OTHER_BASE_NAME"])
        
        config_params["exchange_control"] = os.getenv('EXCHANGE_CONTROL', config["RABBITMQ"]["EXCHANGE_CONTROL"])

        config_params["exchange_output"] = os.getenv('EXCHANGE_OUTPUT', config["RABBITMQ"]["EXCHANGE_OUTPUT"])
        config_params["queue_output_name"] = os.getenv('QUEUE_OUTPUT_NAME', config["RABBITMQ"]["QUEUE_OUTPUT_NAME"])
        config_params["routing_key_output"] = os.getenv('ROUTING_KEY_OUTPUT', config["RABBITMQ"]["ROUTING_KEY_OUTPUT"])

        default_backup_name = f"{config_params['joiner_name']}_state_{config_params['replica_id']}.json"
        config_params["backup_file"] = os.getenv('BACKUP_FILE', default_backup_name)

    except KeyError as e:
        raise KeyError(f"Key was not found. Error: {e}. Aborting server")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}. Aborting server")

    # Construct specific queue names based on replica ID
    replica_id = config_params["replica_id"]
    config_params["queue_movies_name"] = f"{config_params['queue_movies_base']}{replica_id}"
    config_params["queue_other_name"] = f"{config_params['queue_other_base']}{replica_id}"
    routing_key_base = os.getenv('ROUTING_KEY_BASE', config["RABBITMQ"]["ROUTING_KEY_BASE"])
    config_params["routing_key"] = f"routing_{routing_key_base}_{replica_id}"
    
    logging.info(f"Joiner Config Initialized. Replica ID: {config_params['replica_id']}")
    return config_params 