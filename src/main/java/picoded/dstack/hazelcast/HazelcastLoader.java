package picoded.dstack.hazelcast;

import com.hazelcast.core.*;

import java.util.Arrays;
import java.util.List;

import com.hazelcast.config.*;

import picoded.core.struct.*;

/**
 * Provides an easy to initialize hazelcast instance wrapper,
 * with standard java map based configuration initialization.
 * and a static interface which automatically caches connections / 
 * instances within the same JVM
 */
public static class HazelcastLoader {

	//----------------------------------------------------
	//
	//  Load and cache a hazelcast provider connection
	//
	//----------------------------------------------------

	/**
	 * Load from the cache a hazelcast provider connnection.
	 * (Does not initialize, as no configMap was given)
	 * 
	 * @param  groupName used to 
	 */
	public static HazelcastInstance getConnection(String groupName) {
		return connectionMap().get(groupName);
	}

	/**
	 * Load from caches a hazelcast provider connnection.
	 * And initializes one if needed
	 */
	public static HazelcastInstance getConnection(String groupName, Map<String,Object> configMap) {
		// Attempts to load an existing instance without lock
		HazelcastInstance ret = getConnection(groupName);
		if( ret != null ) {
			return ret;
		}

		// Locking is required from here forth
		synchronized(HazelcastWrapper.class) {
			// Load the connection if it was previously 
			// initialized with a race condition
			ret = _connectionMap.get(groupName);
			if( ret != null ) {
				return ret;
			}
	
			// Initialize, cache and return the instance
			ret = initializeInstanceFromConfig(groupName, configMap);
			_connectionMap.put(groupName, ret); 
			return ret;
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
	protected static volatile ConcurrentHashMap<String,HazelcastInstance> _connectionMap = null;

	/**
	 * Connection map singleton
	 * 
	 * @return  ConcurrentHashMap cache for the various connections 
	 */
	protected static ConcurrentHashMap<String,HazelcastInstance> connectionMap() {
		// Attempts to load an existing instance without lock
		if(_connectionMap != null) {
			return _connectionMap;
		}

		// Locking is required from here forth
		synchronized(HazelcastWrapper.class) {
			// Load the map if it was previously 
			// initialized with a race condition
			if(_connectionMap != null) {
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
	protected static HazelcastInstance initializeInstanceFromConfig(String groupName, Map<String,Object> inConfigMap) {
		GenericConvertMap configMap = GenericConvertMap.build(inConfigMap);

		// If gorup name is null, get from configmap
		if( groupName == null ) {
			groupName = configMap.getString("groupName");
		}

		// Initialize the config and set the group name
		Config cfg = new Config();
		cfg.getGroupConfig().setName(groupName);

		// Get server/client mode
		String mode = configMap.getString("mode", "server");

		// Get network settings
		NetworkConfig network = cfg.getNetworkConfig();
		Join join = network.getJoin();

		// Configure multicast mode
		join.getMulticastConfifg().setEnable( configMap.getBoolean("multicast", true) );

		// Port settings for server mode
		if(mode.equalsIgnoreCase("server")) {
			// Get the port, and auto increment settings
			cfg.setPort( configMap.getInt("port", 5900) );
			cfg.setPortAutoIncrement( configMap.getBoolean("portAutoIncrement", true) );

			// Member TCP list, to setup
			List<String> memberTcpList = Arrays.asList( configMap.getStringArray("memberTcpList", "[]") );
			if( memberTcpList != null && memberTcpList.size() > 0 ) {
				join.getTcpIpConfig().setMembers( memberTcpList ).setEnabled(true);
			}
		}
		
		// Initialize the instance with the config
		if( mode.equalsIgnoreCase("client") ) {
			return HazelcastClient.newHazelcastClient(cfg);
		} else if ( mode.equalsIgnoreCase("server") ) {
			return Hazelcast.newHazelcastInstance(cfg);
		} else {
			throw new IllegalArgumentException("Unknown hazelcast instance mode configured : "+mode);
		}
	}

}