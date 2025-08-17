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

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["listen_backlog"] = int(os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SERVER_LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])

        config_params["q_rcv_name_movies"] = os.getenv('QUEUE_RCV_NAME_MOVIES', config["RABBITMQ"]["QUEUE_RCV_NAME_MOVIES"])
        config_params["q_rcv_name_credits"] = os.getenv('QUEUE_RCV_NAME_CREDITS', config["RABBITMQ"]["QUEUE_RCV_NAME_CREDITS"])
        config_params["q_rcv_name_ratings"] = os.getenv('QUEUE_RCV_NAME_RATINGS', config["RABBITMQ"]["QUEUE_RCV_NAME_RATINGS"])
        config_params["q_rcv_exc_movies"] = os.getenv('EXCHANGE_RCV_MOVIES', config["RABBITMQ"]["EXCHANGE_RCV_MOVIES"])
        config_params["q_rcv_exc_credits"] = os.getenv('EXCHANGE_RCV_CREDITS', config["RABBITMQ"]["EXCHANGE_RCV_CREDITS"])
        config_params["q_rcv_exc_raitings"] = os.getenv('EXCHANGE_RCV_RATINGS', config["RABBITMQ"]["EXCHANGE_RCV_RATINGS"])
        config_params["q_rcv_key_movies"] = os.getenv('ROUTING_KEY_RCV_MOVIES', config["RABBITMQ"]["ROUTING_KEY_RCV_MOVIES"])
        config_params["q_rcv_key_credits"] = os.getenv('ROUTING_KEY_RCV_CREDITS', config["RABBITMQ"]["ROUTING_KEY_RCV_CREDITS"])
        config_params["q_rcv_key_raitings"] = os.getenv('ROUTING_KEY_RCV_RATINGS', config["RABBITMQ"]["ROUTING_KEY_RCV_RATINGS"])
        config_params["type_rcv"] = os.getenv('TYPE_RCV', config["RABBITMQ"]["TYPE_RCV"])

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params