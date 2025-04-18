from configparser import ConfigParser
import os

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
    config.read("config.ini")

    config_params = {}

    try:
        config_params["num_filters"] = int(os.getenv('NUM_FILTERS'))
        for i in range(config_params["num_filters"]):
            config_params[f"filter_{i}"] = os.getenv(f'FILTER_{i}')
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["name"] = os.getenv('NAME', config["DEFAULT"]["NAME"])
        config_params["exchange"] = os.getenv('EXCHANGE', config["DEFAULT"]["EXCHANGE"])
        config_params["queue_name"] = os.getenv('QUEUE_NAME', config["DEFAULT"]["QUEUE_NAME"])
        config_params["routing_key"] = os.getenv('ROUTING_KEY', config["DEFAULT"]["ROUTING_KEY"])
        config_params["type"] = os.getenv('TYPE', config["DEFAULT"]["TYPE"]) 
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params