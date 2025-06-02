import sys
import os
import json

FILE_NAME = "docker-compose.yaml"

MOVIES_DATASET = "movies_metadata.csv"
CREDITS_DATASET = "credits.csv"
RATINGS_DATASET = "ratings.csv"

MOVIES_DATASET_USED = "movies_metadata.csv"
CREDITS_DATASET_USED = "credits.csv"
RATINGS_DATASET_USED = "ratings_small.csv"

NETWORK_NAME = "tp_network"
NETWORK_IP = " 172.25.125.0/24"

CONFIG_FILE = "config.ini"

FILTER_MOVIES_BY_2000 = "filter_2000_movies.ini"
FILTER_MOVIES_BY_ARG_SPA = "filter_Arg_Spa_movies.ini"
FILTER_MOVIES_BY_ARG = "filter_Arg_movies.ini"
FILTER_MOVIES_BY_SINGLE_COUNTRY = "filter_single_country_movies.ini"
FILTER_MOVIES_DECADE = "filter_decade_movies.ini"

REDUCER_COMMANDS_TOP5 = "top5.ini"
REDUCER_COMMANDS_TOP10 = "top10.ini"
REDUCER_COMMANDS_MAX_MIN = "max-min.ini"
REDUCER_COMMANDS_AVERAGE = "average.ini"
REDUCER_COMMANDS_QUERY1 = "query1.ini"

AGGR_SENT_BY_REV = "aggr_sent_revenue.ini"
AGGR_COUNTRY_BUDGET = "aggr_country_budget.ini"

JOINER_RATINGS_CONFIG_SOURCE = "./joiner/config/joiner-ratings.ini"
JOINER_CREDITS_CONFIG_SOURCE = "./joiner/config/joiner-credits.ini"
CONFIG_FILE_TARGET = "/config.ini" # Target path inside container

CLIENT_AMOUNT = 1
TRANSFORMER_REPLICAS = 2
JOINER_RATINGS_REPLICAS = 3
JOINER_CREDITS_REPLICAS = 4
FILTER_2000_REPLICAS = 5
FILTER_ARG_SPA_REPLICAS = 6
FILTER_ARG_REPLICAS = 7
FILTER_SINGLE_COUNTRY_REPLICAS = 8
FILTER_DECADE_REPLICAS = 9
AGGR_SENT_REPLICAS = 10
AGGR_BUDGET_REPLICAS = 11
DATA_CONTROLLER_REPLICAS = 12
HEALTHCHECK_REPLICAS = 13


def docker_yaml_generator(client_amount, replicas):
    # Check for joiner config files existence
    required_joiner_configs = [JOINER_RATINGS_CONFIG_SOURCE, JOINER_CREDITS_CONFIG_SOURCE]
    for config_path in required_joiner_configs:
        if not os.path.exists(config_path):
            print(f"Error: Required joiner configuration file not found: {config_path}")
            sys.exit(1)

    with open(FILE_NAME, 'w') as f:
        f.write(create_yaml_file(client_amount, replicas))


def create_yaml_file(client_amount, replicas):
    print(f"Creating:")
    print(f"  Clients: {client_amount}")
    print(f"  Transformers: {replicas['transformer']}")
    print(f"  Joiners: {replicas['joiner-ratings']} (ratings) - {replicas['joiner-credits']} (credits)")
    print(f"  Filters: {replicas['filter-2000-movies']} (2000) - {replicas['filter-arg-spa-movies']} (arg/spa) - {replicas['filter-arg-movies']} (arg) - {replicas['filter-single-country-movies']} (1 country) - {replicas['filter-decade-movies']} (decade)")
    print(f"  Aggregators: {replicas['aggregator-sent']} (sent) - {replicas['aggregator-budget']} (budget)")
    print(f"  Data controllers: {replicas['data-controller']}")
    print(f"  Healthcheckers: {replicas['healthcheck_replicas']}")
    clients = join_clients(client_amount)
    server = create_server(client_amount)
    network = create_network()
    rabbit = create_rabbit()
    filters = write_filters(replicas['filter-2000-movies'], replicas['filter-arg-spa-movies'], replicas['filter-arg-movies'], replicas['filter-single-country-movies'], replicas['filter-decade-movies'])
    transformer = create_transformers(replicas['transformer'])
    aggregator = create_aggregators(replicas['aggregator-sent'], replicas['aggregator-budget'])
    reducer = create_reducer()
    healthcheckers = create_healthcheckers(replicas)
    
    # Create data controller services
    data_controller_services = ""
    for i in range(1, replicas['data-controller'] + 1):
        data_controller_services += create_data_controller(i, replicas['data-controller'])

    # Loop to create multiple joiner services
    joiner_ratings_services = ""
    for i in range(1, replicas['joiner-ratings'] + 1):
        joiner_ratings_services += create_joiner("joiner-ratings", i, replicas['joiner-ratings'], JOINER_RATINGS_CONFIG_SOURCE)
        
    joiner_credits_services = ""
    for i in range(1, replicas['joiner-credits'] + 1):
        joiner_credits_services += create_joiner("joiner-credits", i, replicas['joiner-credits'], JOINER_CREDITS_CONFIG_SOURCE)

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

def join_clients(amount):
    clients = ""
    for client in range(1, amount+1):
        clients += create_client(client)
    return clients

def create_client(id):
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
      - ./data/{CREDITS_DATASET_USED}:/{CREDITS_DATASET}
      - ./data/{RATINGS_DATASET_USED}:/{RATINGS_DATASET}
      - ./data/{MOVIES_DATASET_USED}:/{MOVIES_DATASET}
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

def write_filters(filter_2000=1, filter_arg_spa=1, filter_arg=1, filter_single_country=1, filter_decade=1):

    filters = ""

    for i in range(1, filter_2000 + 1):
        filters += create_filter("filter-2000-movies", i, FILTER_MOVIES_BY_2000, filter_2000)
    for i in range(1, filter_arg_spa + 1):
        filters += create_filter("filter-arg-spa-movies", i, FILTER_MOVIES_BY_ARG_SPA, filter_arg_spa)
    for i in range(1, filter_arg + 1):
        filters += create_filter("filter-arg-movies", i, FILTER_MOVIES_BY_ARG, filter_arg)
    for i in range(1, filter_single_country + 1):
        filters += create_filter("filter-single-country-movies", i, FILTER_MOVIES_BY_SINGLE_COUNTRY, filter_single_country)
    for i in range(1, filter_decade + 1):
        filters += create_filter("filter-decade-movies", i, FILTER_MOVIES_DECADE, filter_decade)

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

def create_reducer():
    reducer_cont = f"""
  reducer:
    container_name: reducer
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
      - ./reducer/{CONFIG_FILE}:/{CONFIG_FILE}
      - ./reducer/reducers/{REDUCER_COMMANDS_AVERAGE}:/{REDUCER_COMMANDS_AVERAGE}
      - ./reducer/reducers/{REDUCER_COMMANDS_TOP5}:/{REDUCER_COMMANDS_TOP5}
      - ./reducer/reducers/{REDUCER_COMMANDS_TOP10}:/{REDUCER_COMMANDS_TOP10}
      - ./reducer/reducers/{REDUCER_COMMANDS_MAX_MIN}:/{REDUCER_COMMANDS_MAX_MIN}
      - ./reducer/reducers/{REDUCER_COMMANDS_QUERY1}:/{REDUCER_COMMANDS_QUERY1}
    """
    return reducer_cont


def create_aggregators(replicas_sent, replicas_budget):
    aggregators = ""
    for i in range(1, replicas_sent + 1):
        aggregators += create_aggregator("aggregator-sent", AGGR_SENT_BY_REV, i, replicas_sent)
    for i in range(1, replicas_budget + 1):
        aggregators += create_aggregator("aggregator-budget", AGGR_COUNTRY_BUDGET, i, replicas_budget)

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
      - JOINER_REPLICA_ID={replica_id} # Direct integer ID
      - JOINER_REPLICA_COUNT={total_replicas} # Direct integer count
    """
    return joiner_yaml

def create_data_controller(replica_id, total_replicas):
    """Creates a unique data controller service definition for docker compose up."""
    service_name = f"data-controller-{replica_id}"
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
      - ./data_controller/{CONFIG_FILE}:/{CONFIG_FILE}
    environment:
      - PYTHONUNBUFFERED=1
      - DATA_CONTROLLER_REPLICA_ID={replica_id}
      - DATA_CONTROLLER_REPLICA_COUNT={total_replicas}
    """
    return data_controller_yaml

def create_healthcheckers(replicas):
  number_hc = replicas['healthcheck_replicas'] if replicas['healthcheck_replicas'] <= 11 else 11

  hc = {}

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
    environment:
      HEALTHCHECK_REPLICA_ID: {hc_id}
      HEALTHCHECK_REPLICA_COUNT: {total_hc}
      PORT: 3030
      NODES: >
          {nodes}
    """
    return hc_cont

def parse_args(args, arg_to_parse):
    if len(args) <= arg_to_parse:
      return 1

    try:
        replicas = int(args[arg_to_parse])
        if replicas < 1:
            print("Argument must be 1 or greater.")
            sys.exit(1)

    except ValueError:
        print("Argument must be an integer.")
        sys.exit(1)

    return replicas

def main():
    if len(sys.argv) < 2:
        print("Usage: python compose-generator.py <client_amount> [transformer_replicas] [joiner_ratings_replicas] [joiner_credits_replicas] [filter_2000_replicas] [filter_arg_spa_replicas] [filter_arg_replicas] [filter_single_country_replicas] [filter_decade_replicas] [aggr_sent_replicas] [aggr_budget_replicas] [data_controller_replicas]")
        sys.exit(1)

    replicas = {}

    client_amount = parse_args(sys.argv, CLIENT_AMOUNT)
    replicas['transformer'] = parse_args(sys.argv, TRANSFORMER_REPLICAS)
    replicas['joiner-ratings'] = parse_args(sys.argv, JOINER_RATINGS_REPLICAS)
    replicas['joiner-credits'] = parse_args(sys.argv, JOINER_CREDITS_REPLICAS)
    replicas['filter-2000-movies'] = parse_args(sys.argv, FILTER_2000_REPLICAS)
    replicas['filter-arg-spa-movies'] = parse_args(sys.argv, FILTER_ARG_SPA_REPLICAS)
    replicas['filter-arg-movies'] = parse_args(sys.argv, FILTER_ARG_REPLICAS)
    replicas['filter-single-country-movies'] = parse_args(sys.argv, FILTER_SINGLE_COUNTRY_REPLICAS)
    replicas['filter-decade-movies'] = parse_args(sys.argv, FILTER_DECADE_REPLICAS)
    replicas['aggregator-sent'] = parse_args(sys.argv, AGGR_SENT_REPLICAS)
    replicas['aggregator-budget'] = parse_args(sys.argv, AGGR_BUDGET_REPLICAS)
    replicas['data-controller'] = parse_args(sys.argv, DATA_CONTROLLER_REPLICAS)
    replicas['healthcheck_replicas'] = parse_args(sys.argv, HEALTHCHECK_REPLICAS)

    # replicas['total_replicas'] = sum(replicas.values())

    docker_yaml_generator(client_amount, replicas)


if __name__ == "__main__":
    main()