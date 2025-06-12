from configparser import ConfigParser
import sys
import os
import json

FILE_NAME = "docker-compose.yaml"

MOVIES_DATASET = "movies_metadata.csv"
CREDITS_DATASET = "credits.csv"
RATINGS_DATASET = "ratings.csv"

NETWORK_NAME = "tp_network"
NETWORK_IP = " 172.25.125.0/24"

CONFIG_FILE = "config.ini"

REDUCER_COMMANDS_TOP5 = "top5.ini"
REDUCER_COMMANDS_TOP10 = "top10.ini"
REDUCER_COMMANDS_MAX_MIN = "max-min.ini"
REDUCER_COMMANDS_AVERAGE = "average.ini"
REDUCER_COMMANDS_QUERY1 = "query1.ini"

JOINER_RATINGS_CONFIG_SOURCE = "./joiner/config/joiner-ratings.ini"
JOINER_CREDITS_CONFIG_SOURCE = "./joiner/config/joiner-credits.ini"
CONFIG_FILE_TARGET = "/config.ini" # Target path inside container


def docker_yaml_generator(replicas, datasets, config_files):
    # Check for joiner config files existence
    required_joiner_configs = [JOINER_RATINGS_CONFIG_SOURCE, JOINER_CREDITS_CONFIG_SOURCE]
    for config_path in required_joiner_configs:
        if not os.path.exists(config_path):
            print(f"Error: Required joiner configuration file not found: {config_path}")
            sys.exit(1)

    with open(FILE_NAME, 'w') as f:
        f.write(create_yaml_file(replicas, datasets, config_files))


def create_yaml_file(replicas, datasets, config_files):
    print(f"Creating:")
    print(f"  Clients: {replicas['client_amount']}")
    print(f"  Transformers: {replicas['transformer']}")
    print(f"  Joiners: {replicas['joiner-ratings']} (ratings) - {replicas['joiner-credits']} (credits)")
    print(f"  Filters: {replicas['filter-2000-movies']} (2000) - {replicas['filter-arg-spa-movies']} (arg/spa) - {replicas['filter-arg-movies']} (arg) - {replicas['filter-single-country-movies']} (1 country) - {replicas['filter-decade-movies']} (decade)")
    print(f"  Aggregators: {replicas['aggregator-sent']} (sent) - {replicas['aggregator-budget']} (budget) - {replicas['aggregator-ratings']} (ratings)")
    print(f"  Data controllers: {replicas['data-controller-movies']} (movies) - {replicas['data-controller-credits']} (credits) - {replicas['data-controller-ratings']} (ratings)")
    print(f"  Healthcheckers: {replicas['healthcheck_replicas']}")
    
    clients = join_clients(replicas['client_amount'], datasets)
    server = create_server(replicas['client_amount'])
    network = create_network()
    rabbit = create_rabbit()
    filters = write_filters(replicas, config_files)
    transformer = create_transformers(replicas['transformer'])
    aggregator = create_aggregators(replicas, config_files)
    reducer = create_reducers()
    data_controller_services = write_data_controllers(replicas, config_files)
    
    # Loop to create multiple joiner services
    joiner_ratings_services = ""
    for i in range(1, replicas['joiner-ratings'] + 1):
        joiner_ratings_services += create_joiner("joiner-ratings", i, replicas['joiner-ratings'], JOINER_RATINGS_CONFIG_SOURCE)
        
    joiner_credits_services = ""
    for i in range(1, replicas['joiner-credits'] + 1):
        joiner_credits_services += create_joiner("joiner-credits", i, replicas['joiner-credits'], JOINER_CREDITS_CONFIG_SOURCE)

    healthcheckers = create_healthcheckers(replicas)
    content = f"""
version: "3.8"
services:
  {rabbit}
  {clients}
  {server}
  {filters}
  {data_controller_services}
  {transformer}
  {aggregator}
  {reducer}
  {joiner_ratings_services}
  {joiner_credits_services}
  {healthcheckers}
networks:
  {network}
"""
    return content

def join_clients(amount, datasets):
    clients = ""
    for client in range(1, amount+1):
        clients += create_client(client, datasets)
    return clients

def create_client(id, datasets):
    path_result = f"results_client{id}.json"
    client = f"""
  client{id}:
    container_name: client{id}
    image: client:latest
    profiles: [clients]
    environment:
      - CLI_ID={id}
    networks:
      - {NETWORK_NAME}
    depends_on:
      server:
        condition: service_started
    volumes:
      - ./client/{CONFIG_FILE}:/{CONFIG_FILE}
      - ./data/{datasets['credits']}:/{CREDITS_DATASET}
      - ./data/{datasets['ratings']}:/{RATINGS_DATASET}
      - ./data/{datasets['movies']}:/{MOVIES_DATASET}
      - ./data/{path_result}:/results.json
    """ 
    #creo archivo por cada cliente
    try:
      with open(f"data/{path_result}", "x") as f:
          pass
    except:
        pass #ya esta creado
    return client

def create_server(client_amount):
    server = f"""server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - NUM_CLIENTS={client_amount}
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
    links:
      - rabbitmq
    volumes:
      - ./server/{CONFIG_FILE}:/{CONFIG_FILE}
    """
    return server

def create_network():
    network = f"""{NETWORK_NAME}:
    ipam:
      driver: default
      config:
        - subnet: {NETWORK_IP}
    """
    return network

def create_rabbit():  
    rabbit = f"""rabbitmq:
    build:
      context: ./rabbitmq
      dockerfile: rabbitmq.dockerfile
    hostname: rabbitmq
    ports:
      - 5672:5672
      - 15672:15672
    networks:
      - {NETWORK_NAME}
    volumes:
      - ./rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf
    healthcheck:
        test: ["CMD", "curl", "-f", "http://localhost:15672"]
        interval: 10s
        timeout: 5s
        retries: 10
    """
    return rabbit

def write_filters(replicas, config_files):

    filters = ""
    filter_names = ["filter-2000-movies", "filter-arg-spa-movies", "filter-arg-movies", "filter-single-country-movies", "filter-decade-movies"]

    for filter in filter_names:
        for i in range(1, replicas[filter] + 1):
            filters += create_filter(filter, i, config_files[filter], replicas[filter])
    
    return filters

def create_filter(filter_name, filter_replica, filter_path, replica_count):

    filter_name = f"{filter_name}-{filter_replica}"

    filter_cont = f"""
  {filter_name}:
    container_name: {filter_name}
    image: filter:latest
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
    links:
      - rabbitmq
    volumes:
      - ./filter/filters/{filter_path}:{CONFIG_FILE_TARGET}
    environment:
      - FILTER_REPLICA_ID={filter_replica}
      - FILTER_REPLICA_COUNT={replica_count}
    """
    return filter_cont

def create_reducers():
    reducers = ""
    reducers_files = ["top5", "top10", "max-min", "average", "query1"]

    for reducer in reducers_files:
      reducer_name = f'reducer-{reducer}'
      reducer_file = f'{reducer}.ini'
      reducers += create_reducer(reducer_name, reducer_file)
    
    return reducers

def create_reducer(reducer_name, file_name):
    reducer_cont = f"""
  {reducer_name}:
    container_name: {reducer_name}
    image: reducer:latest
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
    links:
      - rabbitmq
    volumes:
      - ./reducer/reducers/{file_name}:/{CONFIG_FILE}
    """
    return reducer_cont


def create_aggregators(replicas, config_files):
    aggregators = ""
    aggregator_names = ["aggregator-sent", "aggregator-budget", "aggregator-ratings"]
    for aggregator in aggregator_names:
        for i in range(1, replicas[aggregator] + 1):
            aggregators += create_aggregator(aggregator, config_files[aggregator], i, replicas[aggregator])
    
    return aggregators

def create_aggregator(name, file, id, replicas=1):
  aggr_name = f"{name}-{id}"
  aggr_cont = f"""
  {aggr_name}:
    container_name: {aggr_name}
    image: aggregator:latest
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
    links:
      - rabbitmq
    volumes:
      - ./aggregator/aggregators/{file}:/{CONFIG_FILE}
    environment:
      - AGGR_REPLICA_ID={id}
      - AGGR_REPLICA_COUNT={replicas}
    """

  return aggr_cont

def create_transformers(replicas):
    transformers = ""
    for i in range(1, replicas + 1):
        transformers += create_transformer(i, replicas)
    
    return transformers

def create_transformer(id, replicas=1):
    transformer_name = f"transformer-{id}"
    transformer_yaml = f"""
  {transformer_name}:
    container_name: {transformer_name}
    image: transformer:latest
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
    links:
      - rabbitmq
    volumes:
      - ./transformer/{CONFIG_FILE}:/{CONFIG_FILE}
    environment:
      - TRANSFORMER_REPLICA_ID={id}
      - TRANSFORMER_REPLICAS_COUNT={replicas}
    """
    return transformer_yaml

def create_joiner(service_base_name, replica_id, total_replicas, config_source_path):
    """Creates a unique joiner service definition for docker compose up."""
    service_name = f"{service_base_name}-{replica_id}"
    container_name = service_name # Use the same unique name

    joiner_yaml = f"""
  {service_name}:
    container_name: {container_name}
    image: joiner:latest
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
    links:
      - rabbitmq
    volumes:
      - {config_source_path}:{CONFIG_FILE_TARGET}
    environment:
      - PYTHONUNBUFFERED=1
      - JOINER_REPLICA_ID={replica_id}
      - JOINER_REPLICA_COUNT={total_replicas}
    """
    return joiner_yaml


def write_data_controllers(replicas, config_files):
    data_controllers = ""

    dc_names = ["data-controller-movies", "data-controller-ratings", "data-controller-credits"]
    for data_controller in dc_names:
        for i in range(1, replicas[data_controller] + 1):
            data_controllers += create_data_controller(data_controller, i, replicas[data_controller], config_files[data_controller])
    
    return data_controllers

def create_data_controller(replica_name, replica_id, total_replicas, config_file):
    """Creates a unique data controller service definition for docker compose up."""
    service_name = f"{replica_name}-{replica_id}"
    container_name = service_name

    data_controller_yaml = f"""
  {service_name}:
    container_name: {container_name}
    image: data-controller:latest
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
    links:
      - rabbitmq
    volumes:
      - ./data_controller/data_controllers/{config_file}:/{CONFIG_FILE}
    environment:
      - PYTHONUNBUFFERED=1
      - DATA_CONTROLLER_REPLICA_ID={replica_id}
      - DATA_CONTROLLER_REPLICA_COUNT={total_replicas}
    """
    return data_controller_yaml

def create_healthcheckers(replicas):

  number_hc = replicas['healthcheck_replicas'] if replicas['healthcheck_replicas'] <= 11 else 11

  hc = {}

  replicas.pop('client_amount')
  replicas.pop('joiner-credits')
  replicas.pop('joiner-ratings')

  for i in range(1, number_hc + 1):
    hc[f"healthchecker-{i}"] = {}

  total_hc = replicas.pop("healthcheck_replicas")

  for key, value in sorted(replicas.items(), key=lambda x: x[1]):
    min_hc = min(hc, key=lambda k: sum(hc[k].values()))
    hc[min_hc][key] = value

  healthcheckers = ""

  for h in hc:
      nodes = json.dumps(hc[h])
      healthcheckers += create_hc(h.split("-")[1], total_hc, nodes)
  return healthcheckers


def create_hc(hc_id, total_hc, nodes):    
    hc_name = f"healthchecker-{hc_id}"

    hc_cont = f"""
  {hc_name}:
    container_name: {hc_name}
    image: healthcheck:latest
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
    links:
      - rabbitmq
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      HEALTHCHECK_REPLICA_ID: {hc_id}
      HEALTHCHECK_REPLICA_COUNT: {total_hc}
      PORT: 3030
      LOGGING_LEVEL: INFO
      LISTEN_BACKLOG: 10
      NODES: >
          {nodes}
    """
    return hc_cont

def create_replicas_dict(config):
    replicas = {}

    replicas['client_amount'] = int(config["REPLICAS"]["CLIENT_AMOUNT"])
    replicas['transformer'] = int(config["REPLICAS"]["TRANSFORMER_REPLICAS"])
    replicas['joiner-ratings'] = int(config["REPLICAS"]["JOINER_RATINGS_REPLICAS"])
    replicas['joiner-credits'] = int(config["REPLICAS"]["JOINER_CREDITS_REPLICAS"])
    replicas['filter-2000-movies'] = int(config["REPLICAS"]["FILTER_2000_REPLICAS"])
    replicas['filter-arg-spa-movies'] = int(config["REPLICAS"]["FILTER_ARG_SPA_REPLICAS"])
    replicas['filter-arg-movies'] = int(config["REPLICAS"]["FILTER_ARG_REPLICAS"])
    replicas['filter-single-country-movies'] = int(config["REPLICAS"]["FILTER_SINGLE_COUNTRY_REPLICAS"])
    replicas['filter-decade-movies'] = int(config["REPLICAS"]["FILTER_DECADE_REPLICAS"])
    replicas['aggregator-sent'] = int(config["REPLICAS"]["AGGR_SENT_REPLICAS"])
    replicas['aggregator-budget'] = int(config["REPLICAS"]["AGGR_BUDGET_REPLICAS"])
    replicas['aggregator-ratings'] = int(config["REPLICAS"]["AGGR_RATINGS_REPLICAS"])
    replicas['data-controller-movies'] = int(config["REPLICAS"]["DATA_CONTROLLER_MOVIES_REPLICAS"])
    replicas['data-controller-credits'] = int(config["REPLICAS"]["DATA_CONTROLLER_CREDITS_REPLICAS"])
    replicas['data-controller-ratings'] = int(config["REPLICAS"]["DATA_CONTROLLER_RATINGS_REPLICAS"])
    replicas['healthcheck_replicas'] = int(config["REPLICAS"]["HEALTHCHECK_REPLICAS"])

    return replicas

def create_datasets_dict(config):
    datasets = {}
    datasets["movies"] = config["DATASETS"]["MOVIES_DATASET"]
    datasets["credits"] = config["DATASETS"]["CREDITS_DATASET"]
    datasets["ratings"] = config["DATASETS"]["RATINGS_DATASET"]
    return datasets

def create_config_files_dict(config):
    config_files = {}
    config_files["filter-2000-movies"] = config["CONFIGS_NAMES"]["FILTER_MOVIES_BY_2000"]
    config_files["filter-arg-spa-movies"] = config["CONFIGS_NAMES"]["FILTER_MOVIES_BY_ARG_SPA"]
    config_files["filter-arg-movies"] = config["CONFIGS_NAMES"]["FILTER_MOVIES_BY_ARG"]
    config_files["filter-single-country-movies"] = config["CONFIGS_NAMES"]["FILTER_MOVIES_BY_SINGLE_COUNTRY"]
    config_files["filter-decade-movies"] = config["CONFIGS_NAMES"]["FILTER_MOVIES_DECADE"]
    
    config_files["reducer-top5"] = config["CONFIGS_NAMES"]["REDUCER_COMMANDS_TOP5"]
    config_files["reducer-top10"] = config["CONFIGS_NAMES"]["REDUCER_COMMANDS_TOP10"]
    config_files["reducer-max-min"] = config["CONFIGS_NAMES"]["REDUCER_COMMANDS_MAX_MIN"]
    config_files["reducer-average"] = config["CONFIGS_NAMES"]["REDUCER_COMMANDS_AVERAGE"]
    config_files["reducer-query1"] = config["CONFIGS_NAMES"]["REDUCER_COMMANDS_QUERY1"]
    
    config_files["aggregator-sent"] = config["CONFIGS_NAMES"]["AGGR_SENT_BY_REV"]
    config_files["aggregator-budget"] = config["CONFIGS_NAMES"]["AGGR_COUNTRY_BUDGET"]
    config_files["aggregator-ratings"] = config["CONFIGS_NAMES"]["AGGR_RATINGS"]
    
    config_files["joiner-ratings"] = config["CONFIGS_NAMES"]["JOINER_RATINGS_CONFIG_SOURCE"]
    config_files["joiner-credits"] = config["CONFIGS_NAMES"]["JOINER_CREDITS_CONFIG_SOURCE"]
    
    config_files["data-controller-movies"] = config["CONFIGS_NAMES"]["DATA_CONTROLLER_MOVIES"]
    config_files["data-controller-credits"] = config["CONFIGS_NAMES"]["DATA_CONTROLLER_CREDITS"]
    config_files["data-controller-ratings"] = config["CONFIGS_NAMES"]["DATA_CONTROLLER_RATINGS"]

    return config_files

def main():
    config = ConfigParser()
    # If config.ini does not exists original config object is not modified
    config.read("generator_values.ini")
    

    replicas = create_replicas_dict(config)
    datasets = create_datasets_dict(config)
    config_files = create_config_files_dict(config)

    docker_yaml_generator(replicas, datasets, config_files)


if __name__ == "__main__":
    main()