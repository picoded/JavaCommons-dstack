package picoded.dstack.struct.simple;

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

// Cache2k implmentation
import org.cache2k.Cache2kBuilder;
import org.cache2k.Cache;

/**
 * Reference implementation of DataObjectMap data structure.
 * This is done via a minimal implementation via internal data structures.
 *
 * Built ontop of the Core_DataObjectMap implementation.
 **/
public class StructSimple_DataObjectMap extends Core_DataObjectMap {
	
	//--------------------------------------------------------------------------
	//
	// GLOBAL Static cache
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Global static cache map,
	 * Used to persist all the various cache maps used.
	 */
	protected static Map<String,okhttp3.Cache<String,Map<String,Object>>> globalCacheMap = new ConcurrentHashMap<String,Map<String,Map<String,Object>>>();

	//--------------------------------------------------------------------------
	//
	// Local cache
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Cachename memoizer
	 */
	private String _cacheName = null;

	/**
	 * Get the internal cachename, required to be in configMap
	 */
	private String cacheName() {
		// Return memorized name
		if( _cacheName != null ) {
			return _cacheName;
		}

		// Attempt to load cachename from config
		_cacheName = configMap().getString("name");
		if(_cacheName == null || _cacheName.equals("")) {
			throw new IllegalAccessException("Missing cache name configuration");
		}

		// Return config cachename
		return _cacheName;
	}

	/**
	 * Stores the key to value map
	 **/
	protected Cache<String, Map<String, Object>> _valueMap = null;
	
	/**
	 * Get the cachemap from global namespace by name
	 */
	private Cache<String, Map<String, Object>> valueMap() {
		// Return the value map if already initialized
		if( _valueMap != null ) {
			return _valueMap;
		}

		// Lets load from global cache map with cache name
		// and returns it.
		_valueMap = globalCacheMap.get( cacheName() );
		if(_valueMap == null ) {
			throw new IllegalAccessException("Missing StaticCache, please call systemSetup first : "+cacheName());
		}

		// Return the value map to use
		return _valueMap;
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
		// Value map already loaded, ignore this step
		if(_valueMap != null) {
			return;
		}

		// Lets load from global cache map with cache name if possible
		_valueMap = globalCacheMap.get( cacheName() );
		if(_valueMap != null) {
			return;
		}

		// Alright, time to build a new cache
		// We are in the era of GB ram computing, 100k cache would
		// be a good sane default in server environment.
		//
		// to consider : auto detect RAM size in KB - and use that?
		_valueMap = new Cache2kBuilder<String, Map<String,Object>>() {}
		.name(cacheName)
		.eternal(true)
		.entryCapacity( configMap().getInt("capacity", 100 * 1000) )
		.build();
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	@Override
	public void systemDestroy() {
		globalCacheMap.remove(cacheName());
		_valueMap = null;
	}
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		if( _valueMap != null ) {
			_valueMap.	removeAll();
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// Internal functions, used by DataObject
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the complete remote data map, for DataObject.
	 * This is used to nuke an entire object
	 *
	 * @param  Object ID to remove
	 *
	 * @return  nothing
	 **/
	public void DataObjectRemoteDataMap_remove(String oid) {
		_valueMap.remove(oid);
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * Returns null if not exists
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String oid) {
		Map<String, Object> storedValue = _valueMap.get(oid);
		if (storedValue == null) {
			return null;
		}
		Map<String, Object> ret = new HashMap<String, Object>();
		for (Entry<String, Object> entry : storedValue.entrySet()) {
			ret.put(entry.getKey(), deepCopy(storedValue.get(entry.getKey())));
		}
		return ret;
	}
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 **/
	public void DataObjectRemoteDataMap_update(String oid, Map<String, Object> fullMap,
		Set<String> keys) {
		// Get keys to store, null = all
		if (keys == null) {
			keys = fullMap.keySet();
		}
		
		// Makes a new map if needed
		Map<String, Object> storedValue = _valueMap.get(oid);
		if (storedValue == null) {
			storedValue = new ConcurrentHashMap<String, Object>();
		}
		
		// Get and store the required values
		for (String key : keys) {
			Object val = fullMap.get(key);
			if (val == null) {
				storedValue.remove(key);
			} else {
				storedValue.put(key, val);
			}
		}
		
		// Ensure the value map is stored
		_valueMap.put(oid, storedValue);
	}
	
	//--------------------------------------------------------------------------
	//
	// KeySet support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Get and returns all the GUID's, note that due to its
	 * potential of returning a large data set, production use
	 * should be avoided.
	 *
	 * @return set of keys
	 **/
	@Override
	public Set<String> keySet() {
		return _valueMap.asMap().keySet();
	}
	
}
