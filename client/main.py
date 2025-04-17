#!/usr/bin/env python3

from configparser import ConfigParser
import logging
import os

from client.client import Client

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
    config.read("config.ini")

    config_params = {}
    try:
        config_params["server_addr"] = int(os.getenv('SERVER_ADRESS', config["DEFAULT"]["SERVER_ADRESS"]))
        config_params["client_id"] = int(os.getenv('CLIENT_ID', config["DEFAULT"]["CLIENT_ID"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["max_batch_size"] = os.getenv('MAX_BATCH_SIZE', config["DEFAULT"]["MAX_BATCH_SIZE"])

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    server_addr = config_params["server_addr"]
    client_id = config_params["client_id"]
    max_batch_size = config_params["max_batch_size"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | server_address: {server_addr} | "
                  f"client_id: {client_id} | logging_level: {logging_level}")

    
    client = Client(server_addr, client_id, max_batch_size)
    client.run()
    

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()