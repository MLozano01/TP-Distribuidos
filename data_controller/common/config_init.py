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
    movies_communication_config = {}
    credits_communication_config = {}
    ratings_communication_config = {}
    # If config.ini does not exists original config object is not modified
    config.read(CONFIG_FILE)
    config_params = {}

    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["rabbit_host"] = os.getenv('RABBIT_HOST', config["RABBITMQ"]["RABBIT_HOST"])
        
        # Input configuration
        config_params["input_exchange"] = os.getenv('INPUT_EXCHANGE', config["input"]["server_exchange"])
        config_params["input_routing_key"] = os.getenv('INPUT_ROUTING_KEY', config["input"]["server_routing_key"])
        config_params["input_queue"] = os.getenv('INPUT_QUEUE', config["input"]["queue_rcv"])
        
        # Output movies configuration
        config_params["movies_queue"] = os.getenv('MOVIES_QUEUE', config["output_movies"]["queue"])
        config_params["movies_exchange"] = os.getenv('MOVIES_EXCHANGE', config["output_movies"]["exchange"])
        config_params["movies_routing_key"] = os.getenv('MOVIES_ROUTING_KEY', config["output_movies"]["routing_key"])
        
        # Output ratings configuration
        config_params["ratings_exchange"] = os.getenv('RATINGS_EXCHANGE', config["output_ratings"]["exchange"])
        
        # Output credits configuration
        config_params["credits_exchange"] = os.getenv('CREDITS_EXCHANGE', config["output_credits"]["exchange"])
        
        # Inter replica communication configuration
        movies_comm_name = os.getenv('QUEUE_COMMUNICATION', config["replicas_control"]["movies_queue_communication"])
        credits_comm_name = os.getenv('CREDITS_QUEUE_COMMUNICATION', config["replicas_control"]["credits_queue_communication"])
        ratings_comm_name = os.getenv('RATINGS_QUEUE_COMMUNICATION', config["replicas_control"]["ratings_queue_communication"])

        env_id = os.getenv('DATA_CONTROLLER_REPLICA_ID')
        movies_communication_config["queue_communication_name"] = f"{movies_comm_name}_{env_id}" if env_id else movies_comm_name
        movies_communication_config["routing_communication_key"] = os.getenv('ROUTING_KEY_COMMUNICATION', config["replicas_control"]["movies_routing_key_communication"])
        movies_communication_config["exchange_communication"] = os.getenv('EXCHANGE_COMMUNICATION', config["replicas_control"]["movies_exchange_communication"])
        movies_communication_config["exc_communication_type"] = os.getenv('TYPE_COMMUNICATION', config["replicas_control"]["TYPE_COMMUNICATION"])
        movies_communication_config["data_controller_replicas_count"] = int(os.getenv("DATA_CONTROLLER_REPLICA_COUNT"))

        credits_communication_config["queue_communication_name"] = f"{credits_comm_name}_{env_id}" if env_id else credits_comm_name
        credits_communication_config["routing_communication_key"] = os.getenv('ROUTING_KEY_COMMUNICATION', config["replicas_control"]["credits_routing_key_communication"])
        credits_communication_config["exchange_communication"] = os.getenv('EXCHANGE_COMMUNICATION', config["replicas_control"]["credits_exchange_communication"])
        credits_communication_config["exc_communication_type"] = os.getenv('TYPE_COMMUNICATION', config["replicas_control"]["TYPE_COMMUNICATION"])
        credits_communication_config["data_controller_replicas_count"] = int(os.getenv("DATA_CONTROLLER_REPLICA_COUNT"))

        ratings_communication_config["queue_communication_name"] = f"{ratings_comm_name}_{env_id}" if env_id else ratings_comm_name
        ratings_communication_config["routing_communication_key"] = os.getenv('ROUTING_KEY_COMMUNICATION', config["replicas_control"]["ratings_routing_key_communication"])
        ratings_communication_config["exchange_communication"] = os.getenv('EXCHANGE_COMMUNICATION', config["replicas_control"]["ratings_exchange_communication"])
        ratings_communication_config["exc_communication_type"] = os.getenv('TYPE_COMMUNICATION', config["replicas_control"]["TYPE_COMMUNICATION"])
        ratings_communication_config["data_controller_replicas_count"] = int(os.getenv("DATA_CONTROLLER_REPLICA_COUNT"))


    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params, movies_communication_config, credits_communication_config, ratings_communication_config
