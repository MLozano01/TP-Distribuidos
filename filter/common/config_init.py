from configparser import ConfigParser
import os

CONFIG_FILE = "config.ini"

def initialize_config(filter_file):
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
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))
    
    filter_config = ConfigParser(os.environ)
    filter_config.read(filter_file)


    try:
        config_params["exchange"] = os.getenv('EXCHANGE', filter_config["DEFAULT"]["EXCHANGE"])
        config_params["num_filters"] = int(os.getenv('NUM_FILTERS',  filter_config["DEFAULT"]["NUM_FILTERS"]))
        config_params["queue_name"] = os.getenv('QUEUE_NAME', filter_config["DEFAULT"]["QUEUE_NAME"])
        config_params["routing_key"] = os.getenv('ROUTING_KEY', filter_config["DEFAULT"]["ROUTING_KEY"])
        config_params["exc_type"] = os.getenv('TYPE', filter_config["DEFAULT"]["TYPE"]) 
        for i in range(1, config_params["num_filters"]):
            wanted_filter = f'FILTER_{i}'
            config_params[f"filter_{i}"] = os.getenv(wanted_filter, filter_config["DEFAULT"][wanted_filter]) 
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params