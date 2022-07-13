package picoded.dstack.redis;

// Java imports
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// JavaCommons imports
import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GenericConvert;
import picoded.core.conv.NestedObjectFetch;
import picoded.core.conv.StringEscape;
import picoded.core.struct.query.Query;
import picoded.core.common.ObjectToken;
import picoded.dstack.*;
import picoded.dstack.core.*;

// Redis imports
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.RMap;
import org.redisson.api.RKeys;

// import org.redisson.api.RSet;

/**
 * Redis implementation of DataObjectMap data structure.
 *
 * Built ontop of the Core_DataObjectMap_struct implementation.
 * 
 * Developers of this class would need to reference the following
 * 
 * - 
 **/
public class Redis_DataObjectMap extends Core_DataObjectMap_struct {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/** Redis instance representing the backend connection */
	RedisStack redisStack = null;
	RedissonClient redisson = null;
	RMap<String, Object> redisMap = null;
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param  inStack   hazelcast stack to use
	 * @param  name      of data object map to use
	 */
	public Redis_DataObjectMap(RedisStack inStack, String name) {
		super();
		redisStack = inStack;
		redisson = inStack.getConnection();
		redisMap = redisson.getMap(name);
	}
	
	//--------------------------------------------------------------------------
	//
	// map naming support
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
	private RMap<String, Map<String, Object>> _backendRMap = null;
	
	/**
	 * @return Storage map used for the backend operations of one "DataObjectMap"
	 *         identical to valueMap, made to be compliant with Core_DataObjectMap_struct
	 */
	protected RMap<String, Map<String, Object>> backendMap() {
		if (_backendRMap != null) {
			return _backendRMap;
		}
		_backendRMap = redisson.getMap(name());
		return _backendRMap;
	}
	
	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown (DStackCommon)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Setup the backend storage table, etc. If needed
	 **/
	@Override
	public void systemSetup() {
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	public void systemDestroy() {
		redisMap.delete();
	}
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		//Delete all the keys of the currently selected database
		//redisson.getKeys().flushdb();
		
		//Delete all the keys of all the existing databases
		redisson.getKeys().flushall();
	}
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 **/
	public void DataObjectRemoteDataMap_update(String _oid, Map<String, Object> fullMap,
		Set<String> updateKeys) {
		
		Map<String, Object> clonedMap = new HashMap<String, Object>();
		
		// Lets iterate the keys, and decide accordingly
		for (String key : fullMap.keySet()) {
			// Get the full map value
			Object val = fullMap.get(key);
			
			// Check for Map / List like objects
			if (val instanceof Map || val instanceof List) {
				// Clone it - by JSON serializing back and forth
				clonedMap.put(key, ConvertJSON.toObject(ConvertJSON.fromObject(val)));
			} else {
				// Store it directly, this should be a primative, or byte[]
				clonedMap.put(key, val);
			}
		}
		
		// call the default implementation
		super.DataObjectRemoteDataMap_update(_oid, clonedMap, updateKeys);
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * @return null if not exists, else a map with the data
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String _oid) {
		// Set<String> keys = new HashSet<String>();
		// keys.add(_oid);
		// Map<String, Object> res = redisMap.getAll(keys);
		RMap<String, Object> res = redisMap;
		
		//Input value myself 
		// res.put("helloKey", "worldValue");
		System.out.println(res);
		System.out.println("RMap content:");
		// Where are you content ?
		// System.out.println(res.readAllKeySet());
		System.out.println(res.readAllMap());
		// System.out.println(res.readAllEntrySet());
		// System.out.println(res.readAllValues());
		
		Map<String, Object> ret = new HashMap<>();
		
		Set<String> fullKeys = res.keySet();
		for (String key : fullKeys) {
			
			// Get the value
			Object val = res.get(key);
			
			// Populate the ret map
			ret.put(key, val);
		}
		
		return ret;
	}
	
	/**
	 * @return set of keys
	 **/
	public Set<String> keySet(String value) {
		// The return hashset
		HashSet<String> ret = new HashSet<String>();

		//Fetch everything in current db
		RKeys keySet = redisson.getKeys();
		if (value != null) {
			// Return key where value is matched
			keySet.getKeysByPattern(value).forEach(k -> ret.add(k));
		}
		else {
			// Return the full keyset
			keySet.getKeys().forEach(k -> ret.add(k));
		}
		return ret;
	}
	
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
	public void DataObjectRemoteDataMap_remove(String _oid) {
		//redisson.getKeys().delete(_oid);
		redisMap.fastRemove(_oid);
	}
	
}
