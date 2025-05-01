import sys
import os

FILE_NAME = "docker-compose.yaml"

MOVIES_DATASET = "movies_metadata_filtered.csv"
CREDITS_DATASET = "credits_filtered.csv"
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
        filters += create_filter("filter_Arg_Spa_movies", i, FILTER_MOVIES_BY_ARG_SPA)
    for i in range(1, filter_arg + 1):
        filters += create_filter("filter_Arg_movies", i, FILTER_MOVIES_BY_ARG)
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
    image: {filter_name}:latest
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
        restart: true
    links:
      - rabbitmq
    volumes:
      - ./filter/filters/{filter_path}:/{filter_path}
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

def main():
    if len(sys.argv) < 2:
        print("Usage: python compose-generator.py <client_amount> [transformer_replicas] [joiner_ratings_replicas] [joiner_credits_replicas]")
        sys.exit(1)

    try:
        client_amount = int(sys.argv[1])
        if client_amount < 1:
             print("client_amount must be 1 or greater.")
             sys.exit(1)
    except ValueError:
        print("client_amount must be an integer.")
        sys.exit(1)

    transformer_replicas = 1
    if len(sys.argv) > 2:
        try:
            transformer_replicas = int(sys.argv[2])
            if transformer_replicas < 1:
                print("transformer_replicas must be 1 or greater.")
                sys.exit(1)
        except ValueError:
            print("transformer_replicas must be an integer.")
            sys.exit(1)

    joiner_ratings_replicas = 1 # Default for ratings joiner
    if len(sys.argv) > 3:
        try:
            joiner_ratings_replicas = int(sys.argv[3])
            if joiner_ratings_replicas < 1:
                print("joiner_ratings_replicas must be 1 or greater.")
                sys.exit(1)
        except ValueError:
            print("joiner_ratings_replicas must be an integer.")
            sys.exit(1)

    joiner_credits_replicas = 1 # Default for credits joiner
    if len(sys.argv) > 4: # Check for credits joiner replicas argument
        try:
            joiner_credits_replicas = int(sys.argv[4])
            if joiner_credits_replicas < 1:
                print("joiner_credits_replicas must be 1 or greater.")
                sys.exit(1)
        except ValueError:
            print("joiner_credits_replicas must be an integer.")
            sys.exit(1)
    
    f_2000 = 1
    if len(sys.argv) > 5:
        try:
            f_2000 = int(sys.argv[5])
            if f_2000 < 1:
                print("f_2000 must be 1 or greater.")
                sys.exit(1)
        except ValueError:
            print("f_2000 must be an integer.")
            sys.exit(1)

    f_arg_spa = 1
    if len(sys.argv) > 6:
        try:
            f_arg_spa = int(sys.argv[6])
            if f_arg_spa < 1:
                print("f_arg_spa must be 1 or greater.")
                sys.exit(1)
        except ValueError:
            print("f_arg_spa must be an integer.")
            sys.exit(1)
    f_arg = 1
    if len(sys.argv) > 7:
        try:
            f_arg = int(sys.argv[7])
            if f_arg < 1:
                print("f_arg must be 1 or greater.")
                sys.exit(1)
        except ValueError:
            print("f_arg must be an integer.")
            sys.exit(1)
    f_single_country = 1
    if len(sys.argv) > 8:
        try:
            f_single_country = int(sys.argv[8])
            if f_single_country < 1:
                print("f_single_country must be 1 or greater.")
                sys.exit(1)
        except ValueError:
            print("f_single_country must be an integer.")
            sys.exit(1)
    f_decade = 1
    if len(sys.argv) > 9:
        try:
            f_decade = int(sys.argv[9])
            if f_decade < 1:
                print("f_decade must be 1 or greater.")
                sys.exit(1)
        except ValueError:
            print("f_decade must be an integer.")
            sys.exit(1)

    docker_yaml_generator(client_amount, transformer_replicas, joiner_ratings_replicas, joiner_credits_replicas, f_2000, f_arg_spa, f_arg, f_single_country, f_decade)

if __name__ == "__main__":
    main()