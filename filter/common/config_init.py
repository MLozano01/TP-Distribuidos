from configparser import ConfigParser
import os

CONFIG_FILE = "config.ini"

def config_filter():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """
    config_params = {}
    communication_config = {}
    filter_config = ConfigParser(os.environ)
    filter_config.read(CONFIG_FILE)

    try:
        # LOGGING
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', filter_config["DEFAULT"]["LOGGING_LEVEL"])

        # RCV QUEUE
        config_params["queue_rcv_name"] = os.getenv('QUEUE_RCV_NAME', filter_config["RABBITMQ"]["QUEUE_RCV_NAME"])
        config_params["routing_rcv_key"] = os.getenv('ROUTING_KEY_RCV', filter_config["RABBITMQ"]["ROUTING_KEY_RCV"])
        config_params["exchange_rcv"] = os.getenv('EXCHANGE_RCV', filter_config["RABBITMQ"]["EXCHANGE_RCV"])
        config_params["exc_rcv_type"] = os.getenv('TYPE_RCV', filter_config["RABBITMQ"]["TYPE_RCV"])

        # COMS QUEUE
        comm_name = os.getenv('QUEUE_COMMUNICATION', filter_config["RABBITMQ"]["QUEUE_COMMUNICATION"])
        env_id = os.getenv('FILTER_REPLICA_ID')
        communication_config["queue_communication_name"] = f"{comm_name}_{env_id}" if env_id else comm_name
        communication_config["routing_communication_key"] = os.getenv('ROUTING_KEY_COMMUNICATION', filter_config["RABBITMQ"]["ROUTING_KEY_COMMUNICATION"])
        communication_config["exchange_communication"] = os.getenv('EXCHANGE_COMMUNICATION', filter_config["RABBITMQ"]["EXCHANGE_COMMUNICATION"])
        communication_config["exc_communication_type"] = os.getenv('TYPE_COMMUNICATION', filter_config["RABBITMQ"]["TYPE_COMMUNICATION"])

        # FILTER DATA
        config_params[f"filter_by"] = os.getenv("FILTER", filter_config["FILTERING_INFO"]["FILTER"])
        config_params[f"filter_name"] = os.getenv("FILTER_NAME", filter_config["FILTERING_INFO"]["FILTER_NAME"])
        communication_config[f"filter_replicas_count"] = int(os.getenv("FILTER_REPLICA_COUNT"))

        # JOINER AND SENDER QUEUE
        publish_by_id_str = os.getenv('PUBLISH_TO_JOINERS', filter_config["RABBITMQ"]["PUBLISH_TO_JOINERS"]).strip().lower()
        if publish_by_id_str == 'true':
            config_params["publish_to_joiners"] = True
        elif publish_by_id_str == 'false':
            config_params["publish_to_joiners"] = False
        else:
            raise ValueError(f"Invalid boolean value for PUBLISH_TO_JOINERS: {publish_by_id_str}")
 
        if config_params["publish_to_joiners"]:
            config_params["exchange_snd_ratings"] = os.getenv('EXCHANGE_SND_RATINGS', filter_config["RABBITMQ"]["EXCHANGE_SND_RATINGS"])
            config_params["exc_snd_type_ratings"] = os.getenv('TYPE_SND_RATINGS', filter_config["RABBITMQ"]["TYPE_SND_RATINGS"])
            config_params["exchange_snd_credits"] = os.getenv('EXCHANGE_SND_CREDITS', filter_config["RABBITMQ"]["EXCHANGE_SND_CREDITS"])
            config_params["exc_snd_type_credits"] = os.getenv('TYPE_SND_CREDITS', filter_config["RABBITMQ"]["TYPE_SND_CREDITS"])

        else:
            config_params["queue_snd_name"] = os.getenv('QUEUE_SND_NAME', filter_config["RABBITMQ"]["QUEUE_SND_NAME"])
            config_params["routing_snd_key"] = os.getenv('ROUTING_KEY_SND', filter_config["RABBITMQ"]["ROUTING_KEY_SND"])
            config_params["exchange_snd"] = os.getenv('EXCHANGE_SND', filter_config["RABBITMQ"]["EXCHANGE_SND"])
            config_params["exc_snd_type"] = os.getenv('TYPE_SND', filter_config["RABBITMQ"]["TYPE_SND"])

    except KeyError as e:
        raise KeyError(f"Required key was not found in {CONFIG_FILE} or Env Vars. Error: {e} .Aborting server")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed in {CONFIG_FILE} or Env Vars. Error: {e}. Aborting server")

    return config_params, communication_config