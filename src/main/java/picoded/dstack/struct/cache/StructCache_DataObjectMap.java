package picoded.dstack.struct.cache;

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
public class StructCache_DataObjectMap extends Core_DataObjectMap_struct {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Constructor, without name constructor (this is required)
	 */
	public StructCache_DataObjectMap() {
		super();
	}
	
	/**
	 * Constructor, with name constructor
	 */
	public StructCache_DataObjectMap(String name) {
		super();
		configMap().put("name", name);
	}
	
	//--------------------------------------------------------------------------
	//
	// GLOBAL Static cache
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Global static cache map,
	 * Used to persist all the various cache maps used.
	 */
	protected volatile static Map<String, Cache<String, Map<String, Object>>> globalCacheMap = new ConcurrentHashMap<String, Cache<String, Map<String, Object>>>();
	
	//--------------------------------------------------------------------------
	//
	// Local cache
	//
	//--------------------------------------------------------------------------
	
	/**
	 * @return Cachename memoizer
	 */
	private String _cacheName = null;
	
	/**
	 * @return Get the internal cachename, required to be in configMap
	 */
	private String cacheName() {
		// Return memorized name
		if (_cacheName != null) {
			return _cacheName;
		}
		
		// Attempt to load cachename from config
		_cacheName = configMap().getString("name");
		if (_cacheName == null || _cacheName.equals("")) {
			throw new IllegalArgumentException("Missing cache name configuration");
		}
		
		// Return config cachename
		return _cacheName;
	}
	
	/**
	 * @return The current cache namespace object
	 **/
	protected Cache<String, Map<String, Object>> _valueMap = null;
	
	/**
	 * @return Get the cachemap from global namespace by name
	 */
	private Cache<String, Map<String, Object>> valueMap() {
		// Return the value map if already initialized
		if (_valueMap != null) {
			return _valueMap;
		}
		
		// Lets load from global cache map with cache name
		// and returns it.
		_valueMap = globalCacheMap.get(cacheName());
		if (_valueMap == null) {
			throw new RuntimeException("Missing StaticCache, please call systemSetup first : "
				+ cacheName());
		}
		
		// Return the value map to use
		return _valueMap;
	}
	
	/**
	 * @return Storage map used for the backend operations of one "DataObjectMap"
	 *         identical to valueMap, made to be compliant with Core_DataObjectMap_struct
	 */
	protected Map<String, Map<String, Object>> backendMap() {
		return valueMap().asMap();
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
		if (_valueMap != null) {
			return;
		}
		
		// Lets load from global cache map with cache name if possible
		_valueMap = globalCacheMap.get(cacheName());
		if (_valueMap != null) {
			return;
		}
		
		// Alright, time to build a new cache
		// We are in the era of GB ram computing, 100k cache would
		// be a good sane default in server environment.
		//
		// to consider : auto detect RAM size in KB - and use that?
		int capicity = configMap().getInt("capacity", 100000);
		_valueMap = new Cache2kBuilder<String, Map<String, Object>>() {
		} //
		.name(cacheName())//
			.eternal(true)//
			.entryCapacity(capicity)//
			.build();
		
		// Add it back to the global cache
		globalCacheMap.put(cacheName(), _valueMap);
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	@Override
	public void systemDestroy() {
		globalCacheMap.remove(cacheName());
		_valueMap = null;
	}
	
}
