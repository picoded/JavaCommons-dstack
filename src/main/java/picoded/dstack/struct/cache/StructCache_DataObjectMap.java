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
 * Internal cache implementation of DataObjectMap
 * This is done via a minimal implementation via internal data structures.
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
		
		//
		// Alright, time to build a new cache
		// We are in the era of GB ram computing, 10k cache would
		// be a good sane default in server environment. Even if there are 
		// multiple sets of StructCache, as it would take ~60MB
		//
		// to consider : auto detect RAM size in KB - and use that?
		// a good rough guideline would be 1/4 of free ram space divided by 6kb
		// as a capcity size auto detction
		//
		// # DataObjectMap caching napkin math assumptions
		// - Assume a hashmap object with 30 parameters (including system keys)
		// - Because its hard to predict the capacity/size ratio it is assumed to be 1:1
		// - Keys and value storage are assumed to be a 22 character string
		//
		// > The above assumptions was designed to somewhat be the upper limit of 
		// > ram storage cost for a data object map. Rather then an average.
		//
		// # References
		// - http://java-performance.info/memory-consumption-of-java-data-types-2/
		// - https://www.javamex.com/tutorials/memory/string_memory_usage.shtml
		//
		// # The Math
		//
		//   36 bytes : 32+4 bytes - HashMap space on primary cache map
		//  108 bytes : 3 x overhead for cache mapping
		//   62 bytes : 40 overhead + 22 oid string key
		// 1080 bytes : 30 x (32+4)  HashMap overhead
		// 1860 bytes : 30 x (40+22) ObjectMap key strings
		// 1860 bytes : 30 x (40+22) ObjectMap value strings
		// ----------
		// 5006 bytes : Total bytes per object map
		// ~ 6 kilo bytes : Rounded up
		//
		// # RAM cost for 10k objects
		//
		// 100,000 * 6 KB = 60 MB
		//
		// > So yea, we are ok to assume a 10k objects for most parts
		//
		int capicity = configMap().getInt("capacity", 10000);
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
