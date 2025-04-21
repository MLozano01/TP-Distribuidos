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

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read(CONFIG_FILE)
    config_params = {}

    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["filter_num"] = int(os.getenv('FILTER_NUM', config["DEFAULT"]["FILTER_NUM"]))
        for i in range(config_params["filter_num"]):
            config_params[f"filter_{i}"] = os.getenv(f'FILTER_{i}', config["DEFAULT"][f"FILTER_{i}"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def config_filter(filter_file):
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """
    config_params = {}

    
    filter_config = ConfigParser(os.environ)
    filter_config.read(filter_file)

    try:
        config_params["queue_rcv_name"] = os.getenv('QUEUE_RCV_NAME', filter_config["DEFAULT"]["QUEUE_RCV_NAME"])
        config_params["routing_rcv_key"] = os.getenv('ROUTING_KEY_RCV', filter_config["DEFAULT"]["ROUTING_KEY_RCV"])
        config_params["exchange_rcv"] = os.getenv('EXCHANGE_RCV', filter_config["DEFAULT"]["EXCHANGE_RCV"])
        config_params["exc_rcv_type"] = os.getenv('TYPE_RCV', filter_config["DEFAULT"]["TYPE_RCV"]) 
        config_params["queue_snd_name"] = os.getenv('QUEUE_SND_NAME', filter_config["DEFAULT"]["QUEUE_SND_NAME"])
        config_params["routing_snd_key"] = os.getenv('ROUTING_KEY_SND', filter_config["DEFAULT"]["ROUTING_KEY_SND"])
        config_params["exchange_snd"] = os.getenv('EXCHANGE_SND', filter_config["DEFAULT"]["EXCHANGE_SND"])
        config_params["exc_snd_type"] = os.getenv('TYPE_SND', filter_config["DEFAULT"]["TYPE_SND"])

        config_params["file_name"] = os.getenv('FILE', filter_config["DEFAULT"]["FILE"])

        config_params[f"filter_by"] = os.getenv("FILTER", filter_config["DEFAULT"]["FILTER"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params