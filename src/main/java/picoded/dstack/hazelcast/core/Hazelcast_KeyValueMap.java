package picoded.dstack.hazelcast.core;

// Java imports
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Picoded imports
import picoded.core.conv.ConvertJSON;
import picoded.core.common.ObjectToken;
import picoded.core.struct.MutablePair;
import picoded.dstack.*;
import picoded.dstack.core.*;

// Hazelcast implementation
import com.hazelcast.core.*;
import com.hazelcast.config.*;
import com.hazelcast.map.eviction.LRUEvictionPolicy;
import com.hazelcast.query.Predicates;

/**
 * Hazelcast implementation of KeyValueMap data structure.
 *
 * Built ontop of the Core_KeyValueMap implementation.
 **/
public class Hazelcast_KeyValueMap extends Core_KeyValueMap {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/** HazelcastInstance representing the backend connection */
	HazelcastInstance hazelcast = null;
	HazelcastStack hazelcastStack = null;
	
	/**
	 * (DO NOT USE) Legacy Constructor, with direct instance, without stack support 
	 * this is used for test cases coverage
	 * 
	 * @param  inStack   hazelcast stack to use
	 * @param  name      of data object map to use
	 */
	public Hazelcast_KeyValueMap(HazelcastInstance instance, String name) {
		super();
		hazelcast = instance;
		configMap().put("name", name);
	}
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param  inStack   hazelcast stack to use
	 * @param  name      of data object map to use
	 */
	public Hazelcast_KeyValueMap(HazelcastStack inStack, String name) {
		super();
		hazelcastStack = inStack;
		hazelcast = inStack.getConnection();
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
	 * @return backendmap memoizer
	 */
	private IMap<String, String> _backendMap = null;
	
	/**
	 * @return Storage map used for the backend operations of one "DataObjectMap"
	 *         identical to valueMap, made to be compliant with Core_DataObjectMap_struct
	 */
	protected IMap<String, String> backendMap() {
		if (_backendMap != null) {
			return _backendMap;
		}
		_backendMap = hazelcast.getMap(name());
		return _backendMap;
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
		
		// Setup the config based on the shared stack settings
		if (hazelcastStack != null) {
			hazelcastStack.setupHazelcastMapConfig(mConfig, configMap());
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
	
	/**
	 * Removes all data, without tearing down setup
	 *
	 * Handles re-entrant lock where applicable
	 **/
	@Override
	public void clear() {
		backendMap().clear();
	}
	
	/**
	 * Perform maintenance, mainly removing of expired data if applicable
	 **/
	@Override
	public void maintenance() {
		// does nothing
	}
	
	//--------------------------------------------------------------------------
	//
	// KeySet support implementation
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Search using the value, all the relevent key mappings
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key, note that null matches ALL
	 *
	 * @return array of keys
	 **/
	@Override
	public Set<String> keySet(String value) {
		if (value != null) {
			return backendMap().keySet(Predicates.equal("this", value));
		}
		return backendMap().keySet();
	}
	
	//--------------------------------------------------------------------------
	//
	// Fundemental set/get value (core)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the expire time stamp value, raw without validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param expire TIMESTAMP in milliseconds, 0 means NO expire
	 *
	 * @return 
	 **/
	public void setExpiryRaw(String key, long time) {
		if (time > 0) {
			backendMap().setTtl(key, Math.max(time - System.currentTimeMillis(), 1),
				TimeUnit.MILLISECONDS);
		} else {
			backendMap().setTtl(key, 0, TimeUnit.MILLISECONDS);
		}
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the value, with validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key
	 * @param value, null means removal
	 * @param expire TIMESTAMP, 0 means not timestamp
	 *
	 * @return null
	 **/
	public String setValueRaw(String key, String value, long expire) {
		// removal
		if (value == null) {
			backendMap().remove(key);
			return null;
		}
		
		// Setup key, value - with expirary?
		if (expire > 0) {
			backendMap().set(key, value, Math.max(expire - System.currentTimeMillis(), 1),
				TimeUnit.MILLISECONDS);
		} else {
			backendMap().set(key, value);
		}
		return null;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Returns the value and expiry, with validation against the current timestamp
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param now timestamp, 0 = no timestamp so skip timestamp checks
	 *
	 * @return String value, and expiry pair
	 **/
	public MutablePair<String, Long> getValueExpiryRaw(String key, long now) {
		// Get the entry view
		EntryView<String, String> entry = backendMap().getEntryView(key);
		if (entry == null) {
			return null;
		}
		
		// Get the value and expire object : milliseconds?
		String value = entry.getValue();
		Long expireObj = entry.getExpirationTime();
		if (expireObj == null) {
			expireObj = 0L;
		}
		
		// Note: 0 = no timestamp, hence valid value
		long expiry = expireObj.longValue();
		if (expiry != 0 && expiry < now) {
			return null;
		}
		
		// Return the expirary pair
		return new MutablePair<String, Long>(value, expiry);
	}
	
}
