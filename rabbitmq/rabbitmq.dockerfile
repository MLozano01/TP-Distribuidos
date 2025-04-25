FROM rabbitmq:3.9.16-management-alpine
RUN apk update && apk add curl

# Copy the custom config file
COPY rabbitmq.conf /etc/rabbitmq/rabbitmq.conf
# Ensure correct permissions
RUN chown rabbitmq:rabbitmq /etc/rabbitmq/rabbitmq.conf

# Enable the consistent hash exchange plugin
RUN rabbitmq-plugins enable --offline rabbitmq_consistent_hash_exchange