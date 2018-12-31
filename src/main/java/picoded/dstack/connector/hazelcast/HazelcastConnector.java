package picoded.dstack.connector.hazelcast;

import com.hazelcast.core.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import picoded.core.exception.ExceptionMessage;
import picoded.core.struct.*;

/**
 * Provides an easy to initialize hazelcast instance wrapper,
 * with standard java map based configuration initialization.
 * and a static interface which automatically caches connections / 
 * instances within the same JVM
 */
public class HazelcastConnector {
	
	//----------------------------------------------------
	//
	//  getConnection / closeConnection implementation
	//
	//----------------------------------------------------
	
	/**
	 * Load from caches a hazelcast provider connnection.
	 * And initializes one if needed
	 * 
	 * @param  configMap config used to initialize the client / server respectively
	 */
	public static HazelcastInstance getConnection(GenericConvertMap<String, Object> configMap) {
		
		// Mandatory null argument check
		if (configMap == null) {
			throw new IllegalArgumentException(ExceptionMessage.unexpectedNullArgument);
		}
		
		// Get group name parameter, from config map if needed
		String groupName = configMap.getString("groupName");
		if (groupName == null) {
			throw new IllegalArgumentException("Missing groupName parameter");
		}
		
		// Get server/client mode
		String mode = configMap.getString("mode", "server");
		
		// Initialize the instance with the config
		if (mode.equalsIgnoreCase("client")) {
			return initializeClientInstanceFromConfig(groupName, configMap);
		} else if (mode.equalsIgnoreCase("server")) {
			return initializeServerInstanceFromConfig(groupName, configMap);
		} else {
			throw new IllegalArgumentException("Unknown hazelcast instance mode configured : " + mode);
		}
	}
	
	/**
	 * Close an existing connection (if it exists)
	 * 
	 * @param  groupName used for the cluster
	 */
	public static void closeConnection(HazelcastInstance connection) {
		// Mandatory null argument check
		if (connection == null) {
			throw new IllegalArgumentException(ExceptionMessage.unexpectedNullArgument);
		}
		
		// Close the connection
		connection.shutdown();
	}
	
	//----------------------------------------------------
	//
	//  HazelcastInstance initializer
	//
	//----------------------------------------------------
	
	/**
	 * Initialize the hazel cast server instance with the config map
	 * 
	 * @param groupName group name for the cluster 
	 * @param configMap configuration map to initialize the cluster with
	 * 
	 * @return initialized hazelcast instance
	 */
	protected static HazelcastInstance initializeServerInstanceFromConfig(String groupName,
		GenericConvertMap<String, Object> configMap) {
		// Initialize the config and set the group name
		Config cfg = new Config();
		cfg.getGroupConfig().setName(groupName);
		
		// Get network settings
		NetworkConfig network = cfg.getNetworkConfig();
		JoinConfig join = network.getJoin();
		
		// Configure multicast mode
		join.getMulticastConfig().setEnabled(configMap.getBoolean("multicast", true));
		
		// Get the port, and auto increment settings
		network.setPort(configMap.getInt("port", 5900));
		network.setPortAutoIncrement(configMap.getBoolean("portAutoIncrement", true));
		
		// Member TCP list, to setup
		List<String> memberTcpList = Arrays.asList(configMap.getStringArray("memberTcpList", "[]"));
		if (memberTcpList != null && memberTcpList.size() > 0) {
			join.getTcpIpConfig().setMembers(memberTcpList).setEnabled(true);
		}
		
		// Intialize the server instance and return 
		return Hazelcast.newHazelcastInstance(cfg);
	}
	
	/**
	 * Initialize the hazel cast client instance with the config map
	 * 
	 * @param groupName group name for the cluster 
	 * @param configMap configuration map to initialize the cluster with
	 * 
	 * @return initialized hazelcast instance
	 */
	protected static HazelcastInstance initializeClientInstanceFromConfig(String groupName,
		GenericConvertMap<String, Object> configMap) {
		throw new RuntimeException("Client mode - not yet supported");
		//return HazelcastClient.newHazelcastClient(cfg);
	}
}