# hazelcast

Custom hazlecast container, with preloaded JavaCommons-dstack jars and additional configuration to make it easier to configure for a production use case within kubernetes (or other use cases)

This works by providing the following specific envrionment variable to ease configuration / setup.

# Configuration environment varaible options

```
# hazelcast cluster name to use
ENV CLUSTER_NAME "hazelcast"

# minimum init cluster size and wait timing
ENV CLUSTER_INIT_MIN_SIZE  2
ENV CLUSTER_INIT_WAIT_TIME 10

# Cluster discovery mode
ENV CLUSTER_MULTICAST  "true"
ENV CLUSTER_KUBERNETES "false"

# Cluster discovery DNS srvice
ENV CLUSTER_DISCOVERY "false"
ENV CLUSTER_DNS_DISCOVERY_SERVICE "localhost"
ENV CLUSTER_DNS_DISCOVERY_TIMEOUT 10

```
