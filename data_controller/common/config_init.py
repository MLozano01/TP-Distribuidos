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

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read(CONFIG_FILE)
    config_params = {}

    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        
        # Input configuration
        config_params["queue_rcv_name"] = os.getenv('QUEUE_RCV_NAME', config["RABBITMQ"]["QUEUE_RCV_NAME"])
        config_params["routing_rcv_key"] = os.getenv('ROUTING_KEY_RCV', config["RABBITMQ"]["ROUTING_KEY_RCV"])
        config_params["exchange_rcv"] = os.getenv('EXCHANGE_RCV', config["RABBITMQ"]["EXCHANGE_RCV"])
        config_params["exc_rcv_type"] = os.getenv('TYPE_RCV', config["RABBITMQ"]["TYPE_RCV"])

        # Output configuration
        config_params["exchange_snd"] = os.getenv('EXCHANGE_SND', config["RABBITMQ"]["EXCHANGE_SND"])
        config_params["exc_snd_type"] = os.getenv('TYPE_SND', config["RABBITMQ"]["TYPE_SND"])
        config_params["queue_snd_name"] = os.getenv('QUEUE_SND_NAME', config["RABBITMQ"]["QUEUE_SND_NAME"])
        if config_params["exc_snd_type"] == "direct":
            config_params["routing_snd_key"] = os.getenv('ROUTING_KEY_SND', config["RABBITMQ"]["ROUTING_KEY_SND"])
        else:
        #     config_params["queue_snd_name"] = ""
            config_params["routing_snd_key"] = ""
        
        # Inter replica communication configuration
        env_id = os.getenv('DATA_CONTROLLER_REPLICA_ID')
        config_params["replica_id"] = env_id if env_id else "unknown"

        config_params["port"] = int(os.getenv('COMM_PORT', config["COMM"]["COMM_PORT"]))
        config_params["id"] = os.getenv("DATA_CONTROLLER_REPLICA_ID")
        config_params["name"] = os.getenv('COMM_NAME', config["COMM"]["COMM_NAME"])
        config_params[f"replicas_count"] = int(os.getenv("DATA_CONTROLLER_REPLICA_COUNT"))

        

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params
