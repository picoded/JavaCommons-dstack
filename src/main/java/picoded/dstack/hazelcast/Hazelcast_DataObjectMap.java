package picoded.dstack.hazelcast;

// Java imports
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Picoded imports
import picoded.core.conv.ConvertJSON;
import picoded.core.common.ObjectToken;
import picoded.dstack.*;
import picoded.dstack.core.*;

// Hazelcast implementation
import com.hazelcast.core.*;
import com.hazelcast.config.*;
import com.hazelcast.map.eviction.LRUEvictionPolicy;

/**
 * Hazelcast implementation of DataObjectMap data structure.
 *
 * Built ontop of the Core_DataObjectMap_struct implementation.
 **/
public class Hazelcast_DataObjectMap extends Core_DataObjectMap_struct {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/** HazelcastInstance representing the backend connection */
	HazelcastInstance hazelcast = null;
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param  hazelcast instance to perform operations on
	 * @param  name      of data object map to use
	 */
	public Hazelcast_DataObjectMap(HazelcastInstance inHazelcast, String name) {
		super();
		hazelcast = inHazelcast;
		configMap().put("name", name);
	}
	
	//--------------------------------------------------------------------------
	//
	// Local cache
	//
	//--------------------------------------------------------------------------
	
	/**
	 * @return name memoizer
	 */
	private String _name = null;
	
	/**
	 * @return Get the internal map name, required to be in configMap
	 */
	private String name() {
		// Return memorized name
		if (_name != null) {
			return _name;
		}
		
		// Attempt to load cachename from config
		_name = configMap().getString("name");
		if (_name == null || _name.equals("")) {
			throw new IllegalArgumentException("Missing name configuration");
		}
		
		// Return config cachename
		return _name;
	}
	
	/**
	 * @return Storage map used for the backend operations of one "DataObjectMap"
	 *         identical to valueMap, made to be compliant with Core_DataObjectMap_struct
	 */
	protected Map<String, Map<String, Object>> backendMap() {
		return hazelcast.getMap(name());
	}
	
	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown / maintenance (DStackCommon)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Setsup the backend storage table, etc. If needed
	 **/
	@Override
	public void systemSetup() {
		// Setup the map config
		MapConfig mConfig = new MapConfig(name());
		
		// Setup name, backup and async backup count
		mConfig.setName(name());
		mConfig.setBackupCount(configMap().getInt("backupCount", 2));
		mConfig.setAsyncBackupCount(configMap().getInt("asyncBackupCount", 0));
		
		// Enable or disable readBackupData, default is true IF asyncBackupCount == 0
		mConfig.setReadBackupData( //
			configMap().getBoolean("readBackupData", //
				configMap().getInt("asyncBackupCount", 0) == 0 //
				) //
			); //
		
		// Configure max size policy percentage to JVM heap
		MaxSizeConfig maxSize = new MaxSizeConfig( //
			configMap.getInt("freeHeapPercentage", 10), //
			MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE //
		); //
		mConfig.setMaxSizeConfig(maxSize);
		
		// Set LRU eviction policy
		mConfig.setMapEvictionPolicy(new LRUEvictionPolicy());
		
		// Enable query index for specific fields
		String[] indexArray = configMap().getStringArray("index", "[]");
		for (String indexName : indexArray) {
			mConfig.addMapIndexConfig(new MapIndexConfig(indexName, true));
		}
		
		// and apply it to the instance
		// see : https://docs.hazelcast.org/docs/latest-development/manual/html/Understanding_Configuration/Dynamically_Adding_Configuration_on_a_Cluster.html
		hazelcast.getConfig().addMapConfig(mConfig);
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	@Override
	public void systemDestroy() {
		// Since we do not have a proper map remove command,
		// the closest equivalent is to "clear"
		backendMap().clear();
	}
	
}
