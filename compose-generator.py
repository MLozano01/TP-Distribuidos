import sys
import os

FILE_NAME = "docker-compose.yaml"

MOVIES_DATASET = "movies_metadata.csv"
CREDITS_DATASET = "credits.csv"
RATINGS_DATASET = "ratings_filtered.csv"

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


def docker_yaml_generator(client_amount, transformer_replicas, joiner_ratings_replicas, joiner_credits_replicas, f_2000, f_arg_spa, f_arg, f_single_country, f_decade):
    # Check for joiner config files existence
    required_joiner_configs = [JOINER_RATINGS_CONFIG_SOURCE, JOINER_CREDITS_CONFIG_SOURCE]
    for config_path in required_joiner_configs:
        if not os.path.exists(config_path):
            print(f"Error: Required joiner configuration file not found: {config_path}")
            sys.exit(1)

    with open(FILE_NAME, 'w') as f:
        f.write(create_yaml_file(client_amount, transformer_replicas, joiner_ratings_replicas, joiner_credits_replicas, f_2000, f_arg_spa, f_arg, f_single_country, f_decade))


def create_yaml_file(client_amount, transformer_replicas, joiner_ratings_replicas, joiner_credits_replicas, f_2000, f_arg_spa, f_arg, f_single_country, f_decade):
    clients = join_clients(client_amount)
    server = create_server(client_amount)
    network = create_network()
    rabbit = create_rabbit()
    filters = write_filters(f_2000, f_arg_spa, f_arg, f_single_country, f_decade)
    transformer = create_transformer(transformer_replicas)
    aggregator = create_aggregator()
    reducer = create_reducer()
    
    # Loop to create multiple joiner services
    joiner_ratings_services = ""
    for i in range(1, joiner_ratings_replicas + 1):
        joiner_ratings_services += create_joiner("joiner-ratings", i, joiner_ratings_replicas, JOINER_RATINGS_CONFIG_SOURCE)
        
    joiner_credits_services = ""
    for i in range(1, joiner_credits_replicas + 1):
        joiner_credits_services += create_joiner("joiner-credits", i, joiner_credits_replicas, JOINER_CREDITS_CONFIG_SOURCE)

    content = f"""
version: "3.8"
services:
  {rabbit}
  {clients}
  {server}
  {filters}
  {transformer}
  {aggregator}
  {reducer}
  {joiner_ratings_services}
  {joiner_credits_services}
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
    client = f"""
  client{id}:
    container_name: client{id}
    image: client:latest
    environment:
      - CLI_ID={id}
    networks:
      - {NETWORK_NAME}
    depends_on:
      server:
        condition: service_started
    volumes:
      - ./client/{CONFIG_FILE}:/{CONFIG_FILE}
      - ./data/{CREDITS_DATASET}:/{CREDITS_DATASET}
      - ./data/{RATINGS_DATASET}:/{RATINGS_DATASET}
      - ./data/{MOVIES_DATASET}:/{MOVIES_DATASET}
    """ 
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
        filters += create_filter("filter_2000_movies", i, FILTER_MOVIES_BY_2000)
    for i in range(1, filter_arg_spa + 1):
        filters += create_filter("filter_arg_spa_movies", i, FILTER_MOVIES_BY_ARG_SPA)
    for i in range(1, filter_arg + 1):
        filters += create_filter("filter_arg_movies", i, FILTER_MOVIES_BY_ARG)
    for i in range(1, filter_single_country + 1):
        filters += create_filter("filter_single_country_movies", i, FILTER_MOVIES_BY_SINGLE_COUNTRY)
    for i in range(1, filter_decade + 1):
        filters += create_filter("filter_decade_movies", i, FILTER_MOVIES_DECADE)

    return filters

def create_filter(filter_name, filter_replica, filter_path):
    
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
    """
    return reducer_cont

def create_aggregator():
  aggr_cont = f"""
  aggregator:
    container_name: aggregator
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
      - ./aggregator/{CONFIG_FILE}:/{CONFIG_FILE}
      - ./aggregator/aggregators/{AGGR_SENT_BY_REV}:/{AGGR_SENT_BY_REV}
      - ./aggregator/aggregators/{AGGR_COUNTRY_BUDGET}:/{AGGR_COUNTRY_BUDGET}
    """
  return aggr_cont

def create_transformer(replicas=1):
    transformer_yaml = f"""
  transformer:
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
    """

    if replicas > 1:
        transformer_yaml += f"""deploy:
      replicas: {replicas}
    """
    # If replicas is 1, add back the container_name
    else: # Handles replicas == 1 case
         lines = transformer_yaml.strip().split('\n')
         lines.insert(1, f"    container_name: transformer")
         transformer_yaml = "\n".join(lines) + "\n"

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
        print("Usage: python compose-generator.py <client_amount> [transformer_replicas] [joiner_ratings_replicas] [joiner_credits_replicas]")
        sys.exit(1)
    
    client_amount = parse_args(sys.argv, CLIENT_AMOUNT)
    transformer_replicas = parse_args(sys.argv, TRANSFORMER_REPLICAS)
    joiner_ratings_replicas = parse_args(sys.argv, JOINER_RATINGS_REPLICAS)
    joiner_credits_replicas = parse_args(sys.argv, JOINER_CREDITS_REPLICAS)
    f_2000 = parse_args(sys.argv, FILTER_2000_REPLICAS)
    f_arg_spa = parse_args(sys.argv, FILTER_ARG_SPA_REPLICAS)
    f_arg = parse_args(sys.argv, FILTER_ARG_REPLICAS)
    f_single_country = parse_args(sys.argv, FILTER_SINGLE_COUNTRY_REPLICAS)
    f_decade = parse_args(sys.argv, FILTER_DECADE_REPLICAS)

    docker_yaml_generator(client_amount, transformer_replicas, joiner_ratings_replicas, joiner_credits_replicas, f_2000, f_arg_spa, f_arg, f_single_country, f_decade)

if __name__ == "__main__":
    main()