package picoded.dstack.struct.cache;

// Java imports
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Picoded imports
import picoded.dstack.KeyValueMap;
import picoded.dstack.core.Core_KeyValueMap;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.MutablePair;
import picoded.core.struct.GenericConvertHashMap;

// Cache2k implmentation
import org.cache2k.Cache2kBuilder;
import org.cache2k.Cache;

/**
 * Internal cache implementation of KeyValueMap
 * This is done via a cache2k implementation via internal data structures.
 *
 * Built ontop of the StructCache_KeyValueMap implementation.
 **/
public class StructCache_KeyValueMap extends Core_KeyValueMap {

	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Constructor, without name constructor (this is required)
	 */
	public StructCache_KeyValueMap() {
		super();
	}
	
	/**
	 * Constructor, with name constructor
	 */
	public StructCache_KeyValueMap(String name) {
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
	protected volatile static Map<String, Cache<String, String>> globalCacheMap = new ConcurrentHashMap<String, Cache<String, String>>();
	
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
	protected Cache<String, String> _valueMap = null;
	
	/**
	 * @return Get the cachemap from global namespace by name
	 */
	private Cache<String, String> valueMap() {
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
	protected Map<String, String> backendMap() {
		return valueMap().asMap();
	}
	
	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown / maintenance (DStackCommon)
	//
	//--------------------------------------------------------------------------
	
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
		synchronized (StructCache_KeyValueMap.class) {
			// Lets load from global cache map (again) with cache name if possible
			_valueMap = globalCacheMap.get(cacheName());
			if (_valueMap != null) {
				return;
			}
			
			// Build the cache
			_valueMap = StructCacheUtil.setupCache2kMap(new Cache2kBuilder<String, String>(){}, cacheName(), configMap());
			
			// Add it back to the global cache
			globalCacheMap.put(cacheName(), _valueMap);
		}

	}

	@Override
	public void systemDestroy() {
		synchronized (StructCache_KeyValueMap.class) {
			_valueMap.clear();
			globalCacheMap.remove(cacheName());
			_valueMap = null;
		}

	}

	@Override
	public void maintenance() {
		// Does nothing

	}

	//--------------------------------------------------------------------------
	//
	// Set and get values
	//
	//--------------------------------------------------------------------------
	
	@Override
	public String setValueRaw(String key, String value, long expire) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setExpiryRaw(String key, long time) {
		// TODO Auto-generated method stub

	}

	@Override
	public MutablePair<String, Long> getValueExpiryRaw(String key, long now) {
		// TODO Auto-generated method stub
		return null;
	}
	
	//--------------------------------------------------------------------------
	//
	// KeySet and value query
	//
	//--------------------------------------------------------------------------
	
	@Override
	public Set<String> keySet(String value) {
		// TODO Auto-generated method stub
		return null;
	}

}
