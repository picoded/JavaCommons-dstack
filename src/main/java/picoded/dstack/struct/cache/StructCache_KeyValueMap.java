package picoded.dstack.struct.cache;

import java.io.Serializable;
// Java imports
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// Picoded imports
import picoded.dstack.core.Core_KeyValueMap;
import picoded.core.struct.MutablePair;

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
	 * Serializable class for storing value, expire pairs
	 * We use this because cache2k requires Serializable values
	 */
	static public class CacheValue implements Serializable {
		private static final long serialVersionUID = 1L;
		public String value;
		public long expire;
	}

	static CacheValue cacheValueBuilder(String val, long exp) {
		CacheValue ret = new CacheValue();
		ret.value = val;
		ret.expire = exp;
		return ret;
	}

	/**
	 * Global static cache map,
	 * Used to persist all the various cache maps used.
	 */
	protected volatile static Map<String, Cache<String, CacheValue>> globalCacheMap = new ConcurrentHashMap<String, Cache<String, CacheValue>>();
	
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
	protected Cache<String, CacheValue> _valueMap = null;
	
	/**
	 * @return Get the cachemap from global namespace by name
	 */
	private Cache<String, CacheValue> valueMap() {
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
			Cache2kBuilder<String, CacheValue> builder = new Cache2kBuilder<String, CacheValue>(){};
			builder.storeByReference(true);
			_valueMap = StructCacheUtil.setupCache2kMap(builder, cacheName(), configMap());
			
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
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the value, with validation
	 *
	 * @param key
	 * @param value, null means removal
	 * @param expire TIMESTAMP, 0 means no timestamp
	 *
	 * @return null
	 **/
	@Override
	public String setValueRaw(String key, String value, long expire) {
		// Handles null removal
		if( value == null ) {
			valueMap().remove(key);
			return null;
		}

		// Store and configure expiry
		valueMap().put(key, cacheValueBuilder(value, expire));
		if( expire > 0 ) {
			// Note that cache expiry config, may take priority
			// so this should not be relied on in itself
			valueMap().expireAt(key, expire);
		}
		return null;
	}

	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the expire time stamp value, raw without validation
	 *
	 * @param key as String
	 * @param expire TIMESTAMP in milliseconds, 0 means NO expire
	 *
	 * @return 
	 **/
	@Override
	public void setExpiryRaw(String key, long time) {
		CacheValue cv = valueMap().get(key);
		if( cv == null ) {
			// Does nothing if cv == null
			return;
		}

		// Set the actual expire value
		cv.expire = time;

		// Configure cache2k expire values
		if( time > 0 ) {
			// Note that cache expiry config, may take priority
			// so this should not be relied on in itself
			valueMap().expireAt(key, time);
		}
	}

	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Returns the value and expiry, with validation against the current timestamp
	 *
	 * @param key as String
	 * @param now timestamp, 0 = no timestamp so skip timestamp checks
	 *
	 * @return String value, and expiry pair
	 **/
	@Override
	public MutablePair<String, Long> getValueExpiryRaw(String key, long now) {
		CacheValue cv = valueMap().get(key);
		if( cv == null ) {
			// Does nothing if cv == null
			return null;
		}
		if( cv.expire > 0 && cv.expire <= System.currentTimeMillis() ) {
			// Value should be expired, does nothing
			return null;
		}
		return new MutablePair<String,Long>(cv.value, cv.expire);
	}
	
	//--------------------------------------------------------------------------
	//
	// KeySet and value query
	//
	//--------------------------------------------------------------------------
	
	@Override
	public Set<String> keySet(String value) {
		// Optimized for all
		if( value == null ) {
			return valueMap().asMap().keySet();
		}

		// We have to do a search
		Set<String> full = valueMap().asMap().keySet();
		Set<String> ret = new HashSet<>();
		long now = System.currentTimeMillis();

		// Lets iterate each key
		for(String key : full) {
			CacheValue cv = valueMap().get(key);
			if( cv == null ) {
				// Does nothing if cv == null
				continue;
			}
			if( cv.expire > 0 && cv.expire <= now ) {
				// Value should be expired, does nothing
				continue;
			}
			if( cv.value == value ) {
				ret.add(key);
			}
		}

		// Return the filtered set
		return ret;
	}

}
