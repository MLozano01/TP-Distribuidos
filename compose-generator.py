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



def docker_yaml_generator(client_amount):

    with open(FILE_NAME, 'w') as f:
        f.write(create_yaml_file(client_amount))

def create_yaml_file(client_amount):
    clients = join_clients(client_amount)
    server = create_server(client_amount)
    network = create_network()
    rabbit = create_rabbit()
    filter_cont = create_filter()
    transformer = create_transformer()
    content = f"""
version: "3.8"
services:
  {rabbit}
  {clients}
  {server}
  {filter_cont}
  {transformer}
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
    """ 
    return filter_cont

def create_transformer():
    filter_cont = f"""
  transformer:
    container_name: transformer
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
    return filter_cont

def main(client_amount):
    docker_yaml_generator(client_amount)

if __name__ == "__main__":
    main(client_amount=int(sys.argv[1]))