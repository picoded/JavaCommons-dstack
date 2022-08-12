package picoded.dstack.redisson;

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
import java.util.Collection;
import java.util.Iterator;

// JavaCommons imports
import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GenericConvert;
import picoded.core.conv.NestedObjectFetch;
import picoded.core.conv.StringEscape;
import picoded.core.struct.query.Query;
import picoded.core.common.ObjectToken;
import picoded.dstack.*;
import picoded.dstack.core.*;
import picoded.core.struct.query.*;

// Redis imports
import org.redisson.Redisson;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.api.RedissonClient;
import org.redisson.api.RMap;

/**
 * Redis implementation of DataObjectMap data structure.
 *
 * Built ontop of the Core_DataObjectMap_struct implementation.
 * 
 * Developers of this class would need to reference the following
 * 
 * - 
 **/
public class Redisson_DataObjectMap extends Core_DataObjectMap_struct {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/** Redis instance representing the backend connection */
	RedissonStack redisStack = null;
	RedissonClient redisson = null;
	RMap<String, Object> redisMap = null;
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param  inStack   hazelcast stack to use
	 * @param  name      of data object map to use
	 */
	public Redisson_DataObjectMap(RedissonStack inStack, String name) {
		super();
		redisStack = inStack;
		redisson = inStack.getConnection();
		redisMap = redisson.getMap(name, JsonJacksonCodec.INSTANCE);
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
		_backendRMap = redisson.getMap(name(), JsonJacksonCodec.INSTANCE);
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
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	public void systemDestroy() {
		redisMap.delete();
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
		// //redisson.getKeys().delete(_oid);
		redisMap.fastRemove(_oid);
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

			if (updateKeys.contains(key)) {
				clonedMap.put(key, val);
			}
		}

		// call the default implementation, basically equal to redisMap.put(_oid,clonedMap)
		super.DataObjectRemoteDataMap_update(_oid, clonedMap, updateKeys);
	}

	public Map<String,Object> ObjToMap(Object obj) {
		if( obj instanceof Map ) { return (Map<String,Object>) obj; }
		else {return null;}
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * @return null if not exists, else a map with the data
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String _oid) {

		//Map Slicing 
		Set<String> keys = new HashSet<String>();
		Map<String, Object> mapSlice = new HashMap<String, Object>();
		Map.Entry<String,Object> res = null;
		Object tmpObj = null;

		keys.add(_oid);
		mapSlice = redisMap.getAll(keys);
		
		Iterator<Map.Entry<String,Object>> it = mapSlice.entrySet().iterator();
		if(it.hasNext()){
			res = it.next();
			tmpObj = res.getValue();
		}

		if (tmpObj == null) {
			return null;
		}

		Map<String, Object> resObj = null;
		resObj = ObjToMap(tmpObj);
		
		Map<String, Object> ret = new HashMap<>();
		
		// Lets iterate through the object
		Set<String> fullKeys = resObj.keySet();
		for (String key : fullKeys) {
	
			// Get the value
			Object val = resObj.get(key);
					
			// Populate the ret map
			ret.put(key, val);
		}
	
		return ret;
	}
	
	/**
	 * @return set of keys
	 **/
	@Override
	public Set<String> keySet() {
		// The return hashset
		HashSet<String> ret = new HashSet<String>();
		//Fetch everything in current db
		List<String> retList = null;
		retList = new ArrayList<String>(redisMap.readAllKeySet());
		// Return the full keyset
		retList.forEach(k -> ret.add(k));
		return ret;
	}
	
}
