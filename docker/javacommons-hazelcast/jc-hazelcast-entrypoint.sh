#!/bin/bash

# PS: This is moved from bottom of script
#-----------------------------------------
set -euo pipefail
eval JAVA_OPTS=\"${JAVA_OPTS}\"
eval CLASSPATH=\"${CLASSPATH}\"

#############################################################################################################################

echo "########################################"
echo "# Preparing hazelcast.xml "
echo "########################################"

# minimum init cluster size and wait timing
JAVA_OPTS="-Dhazelcast.initial.min.cluster.size=${CLUSTER_INIT_MIN_SIZE} -Dhazelcast.initial.wait.seconds=${CLUSTER_INIT_WAIT_TIME} ${JAVA_OPTS}"

# Add the XML config path and rest api support
JAVA_OPTS="-Dhazelcast.rest.enabled=true -Dhazelcast.config=/hazelcast.xml ${JAVA_OPTS}"

# Add environment variable support (not needed in 3.12 onward? - need to confirm)
JAVA_OPTS="-Denv.CLUSTER_NAME=${CLUSTER_NAME} ${JAVA_OPTS}"
JAVA_OPTS="-Denv.CLUSTER_INIT_MIN_SIZE=${CLUSTER_INIT_MIN_SIZE} ${JAVA_OPTS}"
JAVA_OPTS="-Denv.CLUSTER_INIT_WAIT_TIME=${CLUSTER_INIT_WAIT_TIME} ${JAVA_OPTS}"
JAVA_OPTS="-Denv.CLUSTER_MULTICAST=${CLUSTER_MULTICAST} ${JAVA_OPTS}"
JAVA_OPTS="-Denv.CLUSTER_KUBERNETES=${CLUSTER_KUBERNETES} ${JAVA_OPTS}"
JAVA_OPTS="-Denv.CLUSTER_DNS_DISCOVERY_SERVICE=${CLUSTER_DNS_DISCOVERY_SERVICE} ${JAVA_OPTS}"
JAVA_OPTS="-Denv.CLUSTER_DNS_DISCOVERY_TIMEOUT=${CLUSTER_DNS_DISCOVERY_TIMEOUT} ${JAVA_OPTS}"

# Special discovery handling
JAVA_OPTS="-Denv.CLUSTER_DISCOVERY=${CLUSTER_DISCOVERY} -Dhazelcast.discovery.enabled=${CLUSTER_DISCOVERY} ${JAVA_OPTS}"

echo "########################################"
echo "# Triggering hazelcast start script "
echo "########################################"

#############################################################################################################################
#
# This copies over the original bash script, with minor modification
#
# from https://github.com/hazelcast/hazelcast-docker/blob/4.0.1/hazelcast-oss/start-hazelcast.sh
# and https://github.com/hazelcast/hazelcast-docker/blob/4.0.1/hazelcast-oss/Dockerfile
#
# NOTE: At some point this needs to be changed when version bumping, to use the official script instead
#
#############################################################################################################################

# This is moved to start of the script (above)
#--------------------------------------------------
# set -euo pipefail
#
# eval JAVA_OPTS=\"${JAVA_OPTS}\"
# eval CLASSPATH=\"${CLASSPATH}\"

# This is taken from the original
#--------------------------------------------------
if [ -n "${CLASSPATH}" ]; then 
  export CLASSPATH="${CLASSPATH_DEFAULT}:${CLASSPATH}"
else
  export CLASSPATH="${CLASSPATH_DEFAULT}"
fi

if [ -n "${JAVA_OPTS}" ]; then
  export JAVA_OPTS="${JAVA_OPTS_DEFAULT} ${JAVA_OPTS}"
else
  export JAVA_OPTS="${JAVA_OPTS_DEFAULT}"
fi

# This is a custom implementation
#--------------------------------------------------
if [ -n "${MIN_HEAP_SIZE}" ]; then
  export JAVA_OPTS="-Xms${MIN_HEAP_SIZE} ${JAVA_OPTS}"
fi

if [ -n "${MAX_HEAP_SIZE}" ]; then
  export JAVA_OPTS="-Xmx${MAX_HEAP_SIZE} ${JAVA_OPTS}"
fi

# NOTE: Mancenter config from the cluster was dropped in 4.0 onwards
#       in favour on management center side config
# if [ -n "${MANCENTER_URL}" ]; then
#   export JAVA_OPTS="-Dhazelcast.mancenter.enabled=true -Dhazelcast.mancenter.url=${MANCENTER_URL} ${JAVA_OPTS}"
# else
#   export JAVA_OPTS="-Dhazelcast.mancenter.enabled=false -Dhazelcast.mancenter.url=localhost ${JAVA_OPTS}"
# fi

# This is removed from the original
#--------------------------------------------------

# if [ -n "${PROMETHEUS_PORT}" ]; then
#   export JAVA_OPTS="-javaagent:${HZ_HOME}/lib/jmx_prometheus_javaagent.jar=${PROMETHEUS_PORT}:${PROMETHEUS_CONFIG} ${JAVA_OPTS}"
# fi

# if [ -n "${LOGGING_LEVEL}" ]; then
#   sed -i "s/java.util.logging.ConsoleHandler.level = INFO/java.util.logging.ConsoleHandler.level = ${LOGGING_LEVEL}/g" logging.properties
#   sed -i "s/.level= INFO/.level= ${LOGGING_LEVEL}/g" logging.properties
# fi

# Starting it up, as per the original script
#--------------------------------------------------

echo "########################################"
echo "# JAVA_OPTS=${JAVA_OPTS}"
echo "# CLASSPATH=${CLASSPATH}"
echo "# starting now...."
echo "########################################"
set -x
exec java -server ${JAVA_OPTS} com.hazelcast.core.server.HazelcastMemberStarter

#############################################################################################################################
