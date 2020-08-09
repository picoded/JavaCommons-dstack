package picoded.dstack.connector.hazelcast;

import com.hazelcast.core.*;
import com.hazelcast.config.*;
import com.hazelcast.client.config.*;
import com.hazelcast.client.HazelcastClient;

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
		
		// Get the clusterName config 
		// - fallbacks to groupName for legacy support
		String clusterName = configMap.getString("clusterName", configMap.getString("groupName"));
		if (clusterName == null) {
			throw new IllegalArgumentException(
				"Missing clusterName parameter (previously known as groupName)");
		}
		
		// Get caching configuration
		boolean ignoreInstanceCache = configMap.getBoolean("ignoreInstanceCache", false);
		
		// Lets initialize and return - without caching!
		if (ignoreInstanceCache) {
			return initializeInstanceFromConfig(clusterName, configMap);
		}
		
		// Lets handle this with caching, without locking
		HazelcastInstance ret = tryToGetFromCache(clusterName);
		if (ret != null) {
			return ret;
		}
		
		// Ok that failed, lets do this with locking
		return initializeCachedInstanceFromConfig(clusterName, configMap);
	}
	
	/**
	 * Close an existing connection (if it exists)
	 * 
	 * @param  clusterName used for the cluster
	 */
	public static void closeConnection(HazelcastInstance connection) {
		// Mandatory null argument check
		if (connection == null) {
			throw new IllegalArgumentException(ExceptionMessage.unexpectedNullArgument);
		}
		
		// Close the connection
		connection.shutdown();
		
		// Remove from cache
		removeFromCache(connection);
	}
	
	//----------------------------------------------------
	//
	//  Cache implementation
	//
	//----------------------------------------------------
	
	// Internal hazelcastInstance cache - that is only initialized when needed
	protected static volatile ConcurrentHashMap<String, HazelcastInstance> cacheMap = null;
	
	/**
	 * Varient of `initializeInstanceFromConfig` with class locking and caching
	 * 
	 * @param clusterName used as cache key
	 * @param configMap configuration for the instance
	 * @return the instance
	 */
	protected static HazelcastInstance initializeCachedInstanceFromConfig(String clusterName,
		GenericConvertMap<String, Object> configMap) {
		synchronized (HazelcastConnector.class) {
			// Initialize cache if needed
			if (cacheMap == null) {
				cacheMap = new ConcurrentHashMap<String, HazelcastInstance>();
			}
			
			// Race condition handling, get from cache if present
			HazelcastInstance ret = cacheMap.get(clusterName);
			if (ret != null) {
				return ret;
			}
			
			// Time to initialize!
			ret = initializeInstanceFromConfig(clusterName, configMap);
			cacheMap.put(clusterName, ret);
			
			// and return it
			return ret;
		}
	}
	
	/**
	 * Get and return a previously initialized connection from the cache
	 * This is done quickly without concurrency locking
	 * 
	 * @param clusterName used as cache key
	 * @return instance if found, else null
	 */
	protected static HazelcastInstance tryToGetFromCache(String clusterName) {
		// Skip if cache is not initialized
		if (cacheMap == null) {
			return null;
		}
		
		// Fetch from cache
		return cacheMap.get(clusterName);
	}
	
	/**
	 * Remove any previously cached instance of the given value
	 * @param instance
	 */
	protected static void removeFromCache(HazelcastInstance instance) {
		// Skip if cache is not initialized
		if (cacheMap == null) {
			return;
		}
		
		// Lets loop and remove the respective value - with a class safety lock
		synchronized (HazelcastConnector.class) {
			Set<String> keySet = new HashSet<String>(cacheMap.keySet());
			for (String key : keySet) {
				if (cacheMap.get(key) == instance) {
					cacheMap.remove(key);
				}
			}
		}
	}
	
	//----------------------------------------------------
	//
	//  HazelcastInstance initializer
	//
	//----------------------------------------------------
	
	protected static HazelcastInstance initializeInstanceFromConfig(String clusterName,
		GenericConvertMap<String, Object> configMap) {
		// Get server/client mode
		String mode = configMap.getString("mode", "server");
		
		// Initialize the instance with the config
		HazelcastInstance ret = null;
		if (mode.equalsIgnoreCase("client")) {
			ret = initializeClientInstanceFromConfig(clusterName, configMap);
		} else if (mode.equalsIgnoreCase("server")) {
			ret = initializeServerInstanceFromConfig(clusterName, configMap);
		} else {
			throw new IllegalArgumentException("Unknown hazelcast instance mode configured : " + mode);
		}
		return ret;
	}
	
	/**
	 * Initialize the hazel cast server instance with the config map
	 * 
	 * @param clusterName group name for the cluster 
	 * @param configMap configuration map to initialize the cluster with
	 * 
	 * @return initialized hazelcast instance
	 */
	protected static HazelcastInstance initializeServerInstanceFromConfig(String clusterName,
		GenericConvertMap<String, Object> configMap) {
		
		// Initialize the config 
		Config cfg = new Config();
		
		// Initialize the common group name config 
		// note that password support has been dropped from 4.0 onwards
		cfg.setClusterName(clusterName);
		
		// Get network settings
		NetworkConfig network = cfg.getNetworkConfig();
		JoinConfig join = network.getJoin();
		
		// Get the port, and auto increment settings
		network.setPort(configMap.getInt("port", 5701));
		network.setPortAutoIncrement(configMap.getBoolean("portAutoIncrement", true));
		
		// Member TCP list, to setup
		List<String> memberTcpList = Arrays.asList(configMap.getStringArray("memberTcpList", "[]"));
		if (memberTcpList != null && memberTcpList.size() > 0) {
			join.getTcpIpConfig().setMembers(memberTcpList).setEnabled(true);
			join.getMulticastConfig().setEnabled(false); // Disables multicast, cannot be used with TCP
		} else {
			// Falls back to multicast mode
			join.getMulticastConfig().setEnabled(configMap.getBoolean("multicast", true));
		}
		
		// // @TODO - consider configuring the network binding addresses
		// // //this sets the allowed connections to the cluster? necessary for multicast, too?
		// // network.getInterfaces().setEnabled(true).addInterface("192.168.0.*");
		
		// // @TODO - consider minimum cluster size support
		// // config.setProperty("hazelcast.initial.min.cluster.size","3");
		
		// Intialize the server instance and return 
		return Hazelcast.newHazelcastInstance(cfg);
	}
	
	/**
	 * Initialize the hazel cast client instance with the config map
	 * 
	 * @param clusterName group name for the cluster 
	 * @param configMap configuration map to initialize the cluster with
	 * 
	 * @return initialized hazelcast instance
	 */
	protected static HazelcastInstance initializeClientInstanceFromConfig(String clusterName,
		GenericConvertMap<String, Object> configMap) {
		
		// Initialize the config 
		ClientConfig cfg = new ClientConfig();
		
		// Initialize the common group name config 
		// note that password support has been dropped from 4.0 onwards
		cfg.setClusterName(clusterName);
		
		// Get network settings
		ClientNetworkConfig network = cfg.getNetworkConfig();
		
		// Member TCP list, to setup
		List<String> memberTcpList = Arrays.asList(configMap.getStringArray("memberTcpList", "[]"));
		if (memberTcpList != null && memberTcpList.size() > 0) {
			network.setAddresses(memberTcpList);
		} else {
			throw new IllegalArgumentException(
				"Missing `memberTcpList` config for hazelcast client : " + clusterName);
		}
		
		// Intialize the server instance and return 
		return HazelcastClient.newHazelcastClient(cfg);
	}
	
}