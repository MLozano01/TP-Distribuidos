from configparser import ConfigParser
import os
import logging

CONFIG_FILE = "config.ini"

def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """

    config_params = {}
    communication_config = {}

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read(CONFIG_FILE)
    config_params = {}

    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["rabbit_host"] = os.getenv('RABBIT_HOST', config["RABBITMQ"]["RABBIT_HOST"])
        config_params["queue_rcv_name"] = os.getenv('QUEUE_RCV_NAME', config["RABBITMQ"]["QUEUE_RCV_NAME"])
        config_params["routing_rcv_key"] = os.getenv('ROUTING_KEY_RCV', config["RABBITMQ"]["ROUTING_KEY_RCV"])
        config_params["exchange_rcv"] = os.getenv('EXCHANGE_RCV', config["RABBITMQ"]["EXCHANGE_RCV"])
        config_params["exc_rcv_type"] = os.getenv('TYPE_RCV', config["RABBITMQ"]["TYPE_RCV"]) 
        config_params["queue_snd_name"] = os.getenv('QUEUE_SND_NAME', config["RABBITMQ"]["QUEUE_SND_NAME"])
        config_params["routing_snd_key"] = os.getenv('ROUTING_KEY_SND', config["RABBITMQ"]["ROUTING_KEY_SND"])
        config_params["exchange_snd"] = os.getenv('EXCHANGE_SND', config["RABBITMQ"]["EXCHANGE_SND"])
        config_params["exc_snd_type"] = os.getenv('TYPE_SND', config["RABBITMQ"]["TYPE_SND"])
        

        comm_name = os.getenv('QUEUE_COMMUNICATION', config["RABBITMQ"]["QUEUE_COMMUNICATION"])
        env_id = os.getenv('TRANSFORMER_REPLICA_ID')
        communication_config["queue_communication_name"] = f"{comm_name}_{env_id}" if env_id else comm_name
        communication_config["routing_communication_key"] = os.getenv('ROUTING_KEY_COMMUNICATION', config["RABBITMQ"]["ROUTING_KEY_COMMUNICATION"])
        communication_config["exchange_communication"] = os.getenv('EXCHANGE_COMMUNICATION', config["RABBITMQ"]["EXCHANGE_COMMUNICATION"])
        communication_config["exc_communication_type"] = os.getenv('TYPE_COMMUNICATION', config["RABBITMQ"]["TYPE_COMMUNICATION"])
        communication_config[f"transformer_replicas_count"] = int(os.getenv("TRANSFORMER_REPLICAS_COUNT"))

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params, communication_config

