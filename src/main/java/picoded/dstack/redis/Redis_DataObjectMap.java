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

// Redis imports
import org.redisson.Redisson;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.api.RedissonClient;
import org.redisson.api.RMap;
import org.redisson.api.RKeys;

// Jackson library used
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.databind.MapperFeature;

import org.hjson.*;


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
	//RSet<Object> set = null;
	
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
		redisMap = redisson.getMap(name, JsonJacksonCodec.INSTANCE);
		//set = redisson.getSet(name, StringCodec.INSTANCE);
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
		redisson.getKeys().flushdb();
		
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

			// // Check for Map / List like objects
			// if (val instanceof Map || val instanceof List) {
			// 	// Clone it - by JSON serializing back and forth
			// 	clonedMap.put(key, ConvertJSON.toObject(ConvertJSON.fromObject(val)));
			// } 

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
	
	// /**
	//  * @return set of keys
	//  **/
	// @Override
	// public Set<String> keySet() {
	// 	System.out.println("KEYSET");
	// 	// The return hashset
	// 	HashSet<String> ret = new HashSet<String>();
	
	// 	//Fetch everything in current db
	// 	RKeys keySet = redisson.getKeys();
	// 	if (value != null) {
	// 		// Return key where value is matched
	// 		keySet.getKeysByPattern(value).forEach(k -> ret.add(k));
	// 	} else {
	// 		// Return the full keyset
	// 		keySet.getKeys().forEach(k -> ret.add(k));
	// 	}
	// 	return ret;
	// }
	
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
	 * Performs a search query, and returns the respective DataObject keys.
	 *
	 * This is the GUID key varient of query, this is critical for stack lookup
	 *
	 * @param   queryClause, of where query statement and value
	 * @param   orderByStr string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array
	 **/
	public String[] query_id(Query queryClause, String orderByStr, int offset, int limit) {
	
		// The return list of DataObjects
		List<String> retList = null;
	
		RMap<String, Object> myRedisMap = redisMap;
	
		// Setup the query, if needed
		if (queryClause == null) {
			// Null gets all
			retList = new ArrayList<String>(myRedisMap.readAllKeySet());
		} else {
	
			// Get the list of _oid that passes the query
			//Set<String> idSet = backendIMap().keySet(queryPredicate);
			//String[] idArr = idSet.toArray(new String[0]);
	
			// DataObject[] from idArr
			//DataObject[] doArr = getArrayFromID(idArr, true);
	
			// Converts to a list
			//retList = new ArrayList(Arrays.asList(doArr));
			retList = new ArrayList<String>(myRedisMap.readAllKeySet());
		}
	
		// Sort, offset, convert to array, and return
		// ???
	
		// Prepare the actual return string array
		int retLength = retList.size();
		String[] ret = new String[retLength];
		for (int a = 0; a < retLength; ++a) {
			//._oid(); -> where is it coming from
			//ret[a] = retList.get(a)._oid();
			ret[a] = String.valueOf(retList.get(a));
		}
	
		System.out.println(Arrays.toString(ret));
		// Returns sorted array of strings
		return ret;
	}
	
}
