package picoded.dstack.hazelcast.core;

// Java imports
import java.util.HashMap;

// JavaCommons imports
import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.*;
import picoded.dstack.*;
import picoded.dstack.connector.hazelcast.*;

// Hazelcast implementation
import com.hazelcast.core.*;
import com.hazelcast.config.*;

/**
 * [Internal use only]
 * 
 * Hazelcast configuration based stack provider
 **/
public abstract class HazelcastStack extends CoreStack {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/**
	 * The internal JSql connection
	 */
	protected HazelcastInstance conn = null;
	
	/**
	 * Constructor with configuration map
	 */
	public HazelcastStack(GenericConvertMap<String, Object> inConfig) {
		super(inConfig);
		
		// If name config is missing, throw
		String name = inConfig.getString("name");
		if (name == null) {
			throw new IllegalArgumentException(
				"Missing 'name' config object for Hazelcast stack provider");
		}
		
		// Extract the hazelcast connection config object
		GenericConvertMap<String, Object> hazelcastConfig = inConfig.fetchGenericConvertStringMap(
			"hazelcast", new HashMap<String, Object>());
		
		// If groupName is not set, configure using the stack name
		if (hazelcastConfig.getString("groupName") == null) {
			hazelcastConfig.put("groupName", name);
		}
		
		// Get the Hazelcast connection
		conn = HazelcastConnector.getConnection(hazelcastConfig);
	}
	
	//--------------------------------------------------------------------------
	//
	// Internal package methods
	//
	//--------------------------------------------------------------------------
	
	/**
	 * @return the internal hazelcastInstance connection
	 */
	protected HazelcastInstance getConnection() {
		return conn;
	}
	
	/**
	 * Given a hazelcast map config, setup common configuration settings based on the instance / data structure config.
	 * 
	 * Settings does not include structure specific settings (like name, map index).
	 * Settings includes common reused ones (like backupcount, and eviction policy).
	 * 
	 * Settings are resolved in the following order
	 * - data structure config
	 * - stack configuration
	 * - default value
	 * 
	 * This function is then overwritten for the HazelcastCacheStack, and HazelcastStoreStack implementation,
	 * to ensure consistnecy in code logic for each backend - with only changes in eviction policy control
	 * 
	 * @param mConfig                hazelcast map config to setup
	 * @param dataStructureConfig    data structure config to pass over
	 */
	protected void setupHazelcastMapConfig(MapConfig mConfig,
		GenericConvertMap<String, Object> dataStructureConfig) {
		//
		// Lets get some of the key settings
		//
		
		// Backup count
		int backupCount = dataStructureConfig.getInt("backupCount", config.getInt("backupCount", 2));
		int asyncBackupCount = dataStructureConfig.getInt("asyncBackupCount",
			config.getInt("asyncBackupCount", 0));
		
		// Enable or disable readBackupData, default is true IF asyncBackupCount == 0
		boolean readBackupData = dataStructureConfig.getBoolean("readBackupData",
			config.getBoolean("readBackupData", asyncBackupCount == 0));
		
		// Setup the respective configurations
		mConfig.setBackupCount(backupCount);
		mConfig.setAsyncBackupCount(asyncBackupCount);
		mConfig.setReadBackupData(readBackupData);
		
		//---------------------------------------------------------------
		// NOTE: Eviction policy controls are to be done by the stack implementation
		//
		// // Configure max size policy percentage to JVM heap
		// MaxSizeConfig maxSize = new MaxSizeConfig( //
		// 	configMap.getInt("freeHeapPercentage", 10), //
		// 	MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE //
		// ); //
		// mConfig.setMaxSizeConfig(maxSize);
		//
		// // Set LRU eviction policy
		// mConfig.setMapEvictionPolicy(new LRUEvictionPolicy());
		//---------------------------------------------------------------
	}
	
	//--------------------------------------------------------------------------
	//
	// Stack commands
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Initilize and return the requested data structure with the given name or type if its supported
	 * 
	 * @param  name  name of the datastructure to initialize
	 * @param  type  implmentation type (KeyValueMap / KeyLongMap / DataObjectMap / FileWorkspaceMap)
	 * 
	 * @return initialized data structure if type is supported
	 */
	protected Core_DataStructure initDataStructure(String name, String type) {
		// Initialize for the respective type
		if (type.equalsIgnoreCase("DataObjectMap")) {
			return new Hazelcast_DataObjectMap(this, name);
		}
		if (type.equalsIgnoreCase("KeyValueMap")) {
			return new Hazelcast_KeyValueMap(this, name);
		}
		// No valid type, return null
		return null;
	}
}
