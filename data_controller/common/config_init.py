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
        config_params["rabbit_host"] = os.getenv('RABBIT_HOST', config["RABBITMQ"]["RABBIT_HOST"])
        
        # Movies queue config
        config_params["movies_queue"] = os.getenv('MOVIES_QUEUE', config["queues"]["movies_queue"])
        config_params["movies_exchange"] = os.getenv('MOVIES_EXCHANGE', config["queues"]["movies_exchange"])
        config_params["movies_routing_key"] = os.getenv('MOVIES_ROUTING_KEY', config["queues"]["movies_routing_key"])
        
        # Ratings queue config
        config_params["ratings_exchange"] = os.getenv('RATINGS_EXCHANGE', config["queues"]["ratings_exchange"])
        
        # Credits queue config
        config_params["credits_exchange"] = os.getenv('CREDITS_EXCHANGE', config["queues"]["credits_exchange"])
        
        # Control exchanges
        config_params["finished_file_exchange"] = os.getenv('FINISHED_FILE_EXCHANGE', config["control_exchanges"]["finished_file_exchange"])
        config_params["finished_movies_exchange"] = os.getenv('FINISHED_MOVIES_EXCHANGE', config["control_exchanges"]["finished_movies_exchange"])

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params
