FROM rabbitmq:3.9.16-management-alpine
RUN apk update && apk add curl

# Enable the consistent hash exchange plugin
RUN rabbitmq-plugins enable --offline rabbitmq_consistent_hash_exchange