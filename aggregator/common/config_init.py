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
    communication_config = {}
    logging.info(config)
    try:
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["queue_rcv_name"] = os.getenv('QUEUE_RCV_NAME', config["RABBIT_MQ"]["QUEUE_RCV_NAME"])
        config_params["routing_rcv_key"] = os.getenv('ROUTING_KEY_RCV', config["RABBIT_MQ"]["ROUTING_KEY_RCV"])
        config_params["exchange_rcv"] = os.getenv('EXCHANGE_RCV', config["RABBIT_MQ"]["EXCHANGE_RCV"])
        config_params["exc_rcv_type"] = os.getenv('TYPE_RCV', config["RABBIT_MQ"]["TYPE_RCV"]) 
        config_params["queue_snd_name"] = os.getenv('QUEUE_SND_NAME', config["RABBIT_MQ"]["QUEUE_SND_NAME"])
        config_params["routing_snd_key"] = os.getenv('ROUTING_KEY_SND', config["RABBIT_MQ"]["ROUTING_KEY_SND"])
        config_params["exchange_snd"] = os.getenv('EXCHANGE_SND', config["RABBIT_MQ"]["EXCHANGE_SND"])
        config_params["exc_snd_type"] = os.getenv('TYPE_SND', config["RABBIT_MQ"]["TYPE_SND"])

        config_params["file_name"] = os.getenv('FILE', config["AGGREGATOR"]["FILE"])

        config_params["field"] = os.getenv("FIELD_AGG", config["AGGREGATOR"]["FIELD_AGG"])
        config_params["key"] = os.getenv("AGG_KEY", config["AGGREGATOR"]["AGG_KEY"])
        config_params["operations"] = os.getenv("OPERATIONS", config["AGGREGATOR"]["OPERATIONS"])
        config_params["expected_finished_msg_amount"] = int(os.getenv('EXPECTED_FINISHED_MSG_AMOUNT', config["AGGREGATOR"].get('EXPECTED_FINISHED_MSG_AMOUNT', 1)))

        communication_config["id"] = os.getenv('AGGR_REPLICA_ID')
        communication_config["comm_port"] = os.getenv('COMM_PORT', config["COMM"]["COMM_PORT"])
        communication_config["name"] = os.getenv('COMM_NAME', config["COMM"]["COMM_NAME"])
        communication_config["replicas_count"] = int(os.getenv("AGGR_REPLICA_COUNT"))
        
    except KeyError as e:
        raise KeyError(f"Key was not found. Error: {e} .Aborting server")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}. Aborting server")

    return config_params, communication_config