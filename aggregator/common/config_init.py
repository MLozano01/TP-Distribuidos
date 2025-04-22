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
        config_params["aggr_num"] = int(os.getenv('AGGR_NUM', config["RABBITMQ"]["AGGR_NUM"]))
        for i in range(config_params["aggr_num"]):
            config_params[f"aggr_{i}"] = os.getenv(f'AGGR_{i}', config["RABBITMQ"][f"AGGR_{i}"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def config_aggregator(aggregator_file):
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """
    config_params = {}

    
    aggr_config = ConfigParser(os.environ)
    aggr_config.read(aggregator_file)
    logging.info(f"File: {aggregator_file}")
    try:
        config_params["queue_rcv_name"] = os.getenv('QUEUE_RCV_NAME', aggr_config["DEFAULT"]["QUEUE_RCV_NAME"])
        config_params["routing_rcv_key"] = os.getenv('ROUTING_KEY_RCV', aggr_config["DEFAULT"]["ROUTING_KEY_RCV"])
        config_params["exchange_rcv"] = os.getenv('EXCHANGE_RCV', aggr_config["DEFAULT"]["EXCHANGE_RCV"])
        config_params["exc_rcv_type"] = os.getenv('TYPE_RCV', aggr_config["DEFAULT"]["TYPE_RCV"]) 
        config_params["queue_snd_name"] = os.getenv('QUEUE_SND_NAME', aggr_config["DEFAULT"]["QUEUE_SND_NAME"])
        config_params["routing_snd_key"] = os.getenv('ROUTING_KEY_SND', aggr_config["DEFAULT"]["ROUTING_KEY_SND"])
        config_params["exchange_snd"] = os.getenv('EXCHANGE_SND', aggr_config["DEFAULT"]["EXCHANGE_SND"])
        config_params["exc_snd_type"] = os.getenv('TYPE_SND', aggr_config["DEFAULT"]["TYPE_SND"])

        config_params["file_name"] = os.getenv('FILE', aggr_config["DEFAULT"]["FILE"])

        config_params[f"field"] = os.getenv("FIELD_AGG", aggr_config["DEFAULT"]["FIELD_AGG"])
        config_params[f"key"] = os.getenv("AGG_KEY", aggr_config["DEFAULT"]["AGG_KEY"])
        config_params[f"operations"] = os.getenv("OPERATIONS", aggr_config["DEFAULT"]["OPERATIONS"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params