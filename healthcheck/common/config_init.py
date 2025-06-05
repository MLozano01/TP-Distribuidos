import os
import json


def config_healthcheck():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """
    config_params = {}
    
    try:
        # LOGGING
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL')

        # HC INFO
        config_params[f"hc_id"] = int(os.getenv("HEALTHCHECK_REPLICA_ID"))
        config_params[f"hc_count"] = int(os.getenv("HEALTHCHECK_REPLICA_COUNT"))
        config_params[f"port"] = int(os.getenv("PORT"))
        config_params[f"listen_backlog"] = int(os.getenv('LISTEN_BACKLOG'))

        config_params["nodes_info"] = json.loads(os.getenv('NODES'))

    except KeyError as e:
        raise KeyError(f"Required key was not found in Env Vars. Error: {e} .Aborting server")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed in Env Vars. Error: {e}. Aborting server")

    return config_params