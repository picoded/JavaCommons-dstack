package picoded.dstack.struct.cache;

// Java imports
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// Picoded imports
import picoded.dstack.core.*;

// Cache2k implmentation
import org.cache2k.Cache2kBuilder;
import org.cache2k.Cache;

/**
 * Internal cache implementation of DataObjectMap
 * This is done via a cache2k implementation via internal data structures.
 *
 * Built ontop of the Core_DataObjectMap_struct implementation.
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
		
		// We perfome the following in a syncronized block, to avoid race conditions
		// in the systemSetup process
		synchronized (StructCache_DataObjectMap.class) {
			// Lets load from global cache map (again) with cache name if possible
			_valueMap = globalCacheMap.get(cacheName());
			if (_valueMap != null) {
				return;
			}
			
			// Build the cache
			_valueMap = StructCacheUtil.setupCache2kMap(new Cache2kBuilder<String, Map<String,Object>>(){}, cacheName(), configMap());
			
			// Add it back to the global cache
			globalCacheMap.put(cacheName(), _valueMap);
		}
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	@Override
	public void systemDestroy() {
		synchronized (StructCache_DataObjectMap.class) {
			_valueMap.clear();
			globalCacheMap.remove(cacheName());
			_valueMap = null;
		}
	}
	
}
