package picoded.dstack.hazelcast;

import com.hazelcast.core.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import picoded.core.struct.*;

/**
 * Provides an easy to initialize hazelcast instance wrapper,
 * with standard java map based configuration initialization.
 * and a static interface which automatically caches connections / 
 * instances within the same JVM
 */
public class HazelcastLoader {
	
	//----------------------------------------------------
	//
	//  Load and cache a hazelcast provider connection
	//
	//----------------------------------------------------
	
	/**
	 * Load from the cache a hazelcast provider connnection.
	 * (Does not initialize, as no configMap was given)
	 * 
	 * @param  groupName used for the cluster
	 */
	public static HazelcastInstance getConnection(String groupName) {
		return connectionMap().get(groupName);
	}
	
	/**
	 * Load from caches a hazelcast provider connnection.
	 * And initializes one if needed
	 * 
	 * @param  groupName used for the cluster
	 * @param  configMap config used to initialize the client / server respectively
	 */
	public static HazelcastInstance getConnection(String groupName, Map<String, Object> configMap) {
		// Attempts to load an existing instance without lock
		HazelcastInstance ret = getConnection(groupName);
		if (ret != null) {
			return ret;
		}
		
		// Locking is required from here forth
		synchronized (HazelcastLoader.class) {
			// Load the connection if it was previously 
			// initialized with a race condition
			ret = _connectionMap.get(groupName);
			if (ret != null) {
				return ret;
			}
			
			// Initialize, cache and return the instance
			ret = initializeInstanceFromConfig(groupName, configMap);
			_connectionMap.put(groupName, ret);
			return ret;
		}
	}
	
	/**
	 * Close an existing connection (if it exists)
	 * 
	 * @param  groupName used for the cluster
	 */
	public static void closeConnection(String groupName) {
		// First check if connection exists, without locking
		if (getConnection(groupName) == null) {
			return;
		}
		
		// Connection exists, lets try to close the connection with locking
		synchronized (HazelcastLoader.class) {
			HazelcastInstance connection = _connectionMap.get(groupName);
			
			// Check if the connection was previously terminated
			if (connection == null) {
				return;
			}
			
			// Terminate and remove the connection
			connection.shutdown();
			// Remove if equals to the closed connection object
			_connectionMap.remove(groupName, connection);
		}
	}
	
	//----------------------------------------------------
	//
	//  Static connection map cache
	//
	//----------------------------------------------------
	
	/**
	 * Local JVM cache of hazelcast connections
	 */
	protected static volatile ConcurrentHashMap<String, HazelcastInstance> _connectionMap = null;
	
	/**
	 * Connection map singleton
	 * 
	 * @return  ConcurrentHashMap cache for the various connections 
	 */
	protected static ConcurrentHashMap<String, HazelcastInstance> connectionMap() {
		// Attempts to load an existing instance without lock
		if (_connectionMap != null) {
			return _connectionMap;
		}
		
		// Locking is required from here forth
		synchronized (HazelcastLoader.class) {
			// Load the map if it was previously 
			// initialized with a race condition
			if (_connectionMap != null) {
				return _connectionMap;
			}
			
			// Initialize and return the instance
			_connectionMap = new ConcurrentHashMap<>();
			return _connectionMap;
		}
	}
	
	//----------------------------------------------------
	//
	//  HazelcastInstance initializer
	//
	//----------------------------------------------------
	
	/**
	 * Initialize the hazel cast instance with the config map
	 * 
	 * @param groupName group name for the cluster 
	 * @param inConfigMap configuration map to initialize the cluster with
	 * 
	 * @return initialized hazelcast instance
	 */
	protected static HazelcastInstance initializeInstanceFromConfig(String groupName,
		Map<String, Object> inConfigMap) {
		GenericConvertMap<String, Object> configMap = GenericConvertMap.build(inConfigMap);
		
		// Get group name parameter, from config map if needed
		if (groupName == null) {
			groupName = configMap.getString("groupName");
		}
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