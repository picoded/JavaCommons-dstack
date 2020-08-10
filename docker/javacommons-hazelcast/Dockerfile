#
# Does the code repo setup
#
FROM picoded/ubuntu-plus AS builder

# Clone the JC-dstack repo (including sub modules)
RUN apt-get update && apt-get install -y git 
RUN git clone https://github.com/picoded/JavaCommons-dstack.git
RUN cd /workspace/JavaCommons-dstack/ && ./git-init-submodules.sh

# download dependencies && build the project code
RUN cd /workspace/JavaCommons-dstack/ && ./gradlew source

# Build the the slim jar 
# (as we want to avoid dependency collisions)
RUN cd /workspace/JavaCommons-dstack/ && ./gradlew jar

# Lets copy over the built files
RUN mkdir -p /workspace/built-jars
RUN cp /workspace/JavaCommons-dstack/build/libs/*-dstack-*-nodep.jar /workspace/built-jars/
RUN cp /workspace/JavaCommons-dstack/JavaCommons-core/build/libs/*-core-*-all.jar /workspace/built-jars/

# Useful debugging ls
RUN ls /workspace/built-jars/

#
# Now we got JAR's - time to setup hazelcast
#
FROM hazelcast/hazelcast:4.0.2 as javacommons-hazelcast

# Copy over the relevent classpath
ENV CLASSPATH="/opt/hazelcast/CLASSPATH_EXT:/opt/hazelcast/CLASSPATH_EXT/*"
COPY --from=builder /workspace/built-jars/* /opt/hazelcast/CLASSPATH_EXT/

# Adding custom hazelcast.xml
ADD hazelcast.xml /hazelcast.xml

# cluster name support (also known as cluster name)
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

# Custom entrypoint
COPY jc-hazelcast-entrypoint.sh /jc-hazelcast-entrypoint.sh
RUN chmod +x /jc-hazelcast-entrypoint.sh
ENTRYPOINT /jc-hazelcast-entrypoint.sh
CMD []