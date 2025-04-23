import sys

FILE_NAME = "docker-compose.yaml"

MOVIES_DATASET = "movies_metadata.csv"
CREDITS_DATASET = "credits.csv"
RATINGS_DATASET = "ratings.csv"

NETWORK_NAME = "tp_network"
NETWORK_IP = " 172.25.125.0/24"

CONFIG_FILE = "config.ini"

FILTER_MOVIES_BY_2000 = "filter_2000_movies.ini"
FILTER_MOVIES_BY_ARG_SPA = "filter_Arg_Spa_movies.ini"
FILTER_MOVIES_BY_ARG = "filter_Arg_movies.ini"
FILTER_MOVIES_BY_SINGLE_COUNTRY = "filter_single_country_movies.ini"

REDUCER_COMMANDS_TOP5 = "top5.ini"
REDUCER_COMMANDS_TOP10 = "top10.ini"
REDUCER_COMMANDS_MAX_MIN = "max-min.ini"
REDUCER_COMMANDS_AVERAGE = "average.ini"

AGGR_SENT_BY_REV = "aggr_sent_revenue.ini"


def docker_yaml_generator(client_amount, transformer_replicas):

    with open(FILE_NAME, 'w') as f:
        f.write(create_yaml_file(client_amount, transformer_replicas))

def create_yaml_file(client_amount, transformer_replicas):
    clients = join_clients(client_amount)
    server = create_server(client_amount)
    network = create_network()
    rabbit = create_rabbit()
    filter_cont = create_filter()
    transformer = create_transformer(transformer_replicas)
    aggregator = create_aggregator()
    reducer = create_reducer()
    content = f"""
version: "3.8"
services:
  {rabbit}
  {clients}
  {server}
  {filter_cont}
  {transformer}
  {aggregator}
  {reducer}
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
      - server
      - rabbitmq
    links:
      - rabbitmq
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
      - rabbitmq
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
    networks:
      - {NETWORK_NAME}
    """
    return rabbit

def create_filter():
    filter_cont = f"""
  filter:
    container_name: filter
    image: filter:latest
    networks:
      - {NETWORK_NAME}
    depends_on:
      - server
      - rabbitmq
    links:
      - rabbitmq
    volumes:
      - ./filter/{CONFIG_FILE}:/{CONFIG_FILE}
      - ./filter/filters/{FILTER_MOVIES_BY_2000}:/{FILTER_MOVIES_BY_2000}
      - ./filter/filters/{FILTER_MOVIES_BY_ARG_SPA}:/{FILTER_MOVIES_BY_ARG_SPA}
      - ./filter/filters/{FILTER_MOVIES_BY_ARG}:/{FILTER_MOVIES_BY_ARG}
      - ./filter/filters/{FILTER_MOVIES_BY_SINGLE_COUNTRY}:/{FILTER_MOVIES_BY_SINGLE_COUNTRY}
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
      - server
      - rabbitmq
    links:
      - rabbitmq
    volumes:
      - ./reducer/{CONFIG_FILE}:/{CONFIG_FILE}
      - ./reducer/reducers/{REDUCER_COMMANDS_AVERAGE}:/{REDUCER_COMMANDS_AVERAGE}
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
      - server
      - rabbitmq
    links:
      - rabbitmq
    volumes:
      - ./aggregator/{CONFIG_FILE}:/{CONFIG_FILE}
      - ./aggregator/aggregators/{AGGR_SENT_BY_REV}:/{AGGR_SENT_BY_REV}
    """ 
  return aggr_cont

def create_transformer(replicas=1):
    transformer_yaml = f"""
  transformer:
    image: transformer:latest
    networks:
      - {NETWORK_NAME}
    depends_on:
      - server
      - rabbitmq
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

def main(client_amount, transformer_replicas=1):
    docker_yaml_generator(client_amount, transformer_replicas)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python compose-generator.py <client_amount> [transformer_replicas]")
        sys.exit(1)

    try:
        client_amount = int(sys.argv[1])
        if client_amount < 1:
             print("client_amount must be 1 or greater.")
             sys.exit(1)
    except ValueError:
        print("client_amount must be an integer.")
        sys.exit(1)

    transformer_replicas = 1 # Default
    if len(sys.argv) > 2:
        try:
            transformer_replicas = int(sys.argv[2])
            if transformer_replicas < 1:
                print("transformer_replicas must be 1 or greater.")
                sys.exit(1)
        except ValueError:
            print("transformer_replicas must be an integer.")
            sys.exit(1)

    main(client_amount, transformer_replicas)