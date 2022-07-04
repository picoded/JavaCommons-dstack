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
public class Redis_DataObjectMap extends Core_DataObjectMap {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/** Redis instance representing the backend connection */
	RedisStack redisStack = null;
	RedissonClient redisson = null;
	
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
		configMap().put("name", name);
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
	}
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		//Delete all the keys of the currently selected database
		//redisson.getKeys().flushall();
		
		//Delete all the keys of all the existing databases
		redisson.getKeys().flushall();
	}
	
	public void DataObjectRemoteDataMap_update(String _oid, Map<String, Object> fullMap,
		Set<String> updateKeys) {
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * @return null if not exists, else a map with the data
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String _oid) {
		RMap<String, String> res = redisson.getMap("_oid");
		
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
	@Override
	public Set<String> keySet() {
		// The return hashset
		HashSet<String> ret = new HashSet<String>();
		
		//Fetch everything in current db
		RKeys keySet = redisson.getKeys();
		keySet.getKeysByPattern("_oid").forEach(k -> ret.add(k));
		
		// Return the full keyset
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
		redisson.getKeys().delete(_oid);
	}
}
