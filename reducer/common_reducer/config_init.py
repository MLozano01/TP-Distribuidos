from configparser import ConfigParser
import os
import logging

CONFIG_FILE = "config.ini"

def config_reducer():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """
    config_params = {}

    reducer_config = ConfigParser(os.environ)
    reducer_config.read(CONFIG_FILE)

    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', reducer_config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["queue_rcv_name"] = os.getenv('QUEUE_RCV_NAME', reducer_config["DEFAULT"]["QUEUE_RCV_NAME"])
        config_params["routing_rcv_key"] = os.getenv('ROUTING_KEY_RCV', reducer_config["DEFAULT"]["ROUTING_KEY_RCV"])
        config_params["exchange_rcv"] = os.getenv('EXCHANGE_RCV', reducer_config["DEFAULT"]["EXCHANGE_RCV"])
        config_params["exc_rcv_type"] = os.getenv('TYPE_RCV', reducer_config["DEFAULT"]["TYPE_RCV"]) 
        config_params["queue_snd_name"] = os.getenv('QUEUE_SND_NAME', reducer_config["DEFAULT"]["QUEUE_SND_NAME"])
        config_params["routing_snd_key"] = os.getenv('ROUTING_KEY_SND', reducer_config["DEFAULT"]["ROUTING_KEY_SND"])
        config_params["exchange_snd"] = os.getenv('EXCHANGE_SND', reducer_config["DEFAULT"]["EXCHANGE_SND"])
        config_params["exc_snd_type"] = os.getenv('TYPE_SND', reducer_config["DEFAULT"]["TYPE_SND"])
        config_params[f"reduce_by"] = os.getenv("REDUCER", reducer_config["DEFAULT"]["REDUCER"])

        config_params["port"] = int(os.getenv('PORT', reducer_config["DEFAULT"]["PORT"]))
        config_params["backup_file"] = os.getenv("BACKUP_FILE")

        config_params["query_id"] = int(os.getenv('QUERY_ID', reducer_config["DEFAULT"].get('QUERY_ID', 0)))
        config_params["reducer_name"] = os.getenv('REDUCER_NAME')
        
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params