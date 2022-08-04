package picoded.dstack.redis;

// Java imports
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Picoded imports
import picoded.core.conv.ConvertJSON;
import picoded.core.common.ObjectToken;
import picoded.core.struct.MutablePair;
import picoded.dstack.*;
import picoded.dstack.core.*;

// Redis imports
import org.redisson.Redisson;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.api.RedissonClient;
import org.redisson.api.RMap;

public class Redis_KeyLongMap extends Core_KeyLongMap {

    //--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	RedisStack redisStack = null;
	RedissonClient redisson = null;
	RMap<String, Object> redisMap = null;
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param  inStack   hazelcast stack to use
	 * @param  name      of data object map to use
	 */
	public Redis_KeyLongMap(RedisStack inStack, String name) {
		super();
		redisStack = inStack;
		redisson = inStack.getConnection();
		redisMap = redisson.getMap(name, JsonJacksonCodec.INSTANCE);
	}

    //--------------------------------------------------------------------------
	//
	// Local cache
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
	private RMap<String, String> _backendRMap = null;
	
	/**
	 * @return Storage map used for the backend operations of one "DataObjectMap"
	 *         identical to valueMap, made to be compliant with Core_DataObjectMap_struct
	 */
	protected RMap<String, String> backendMap() {
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
	 * Setsup the backend storage table, etc. If needed
	 **/
	@Override
	public void systemSetup() {
		// does nothing
	}

	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	public void systemDestroy() {
		redisMap.delete();
	}

	/**
	 * Perform maintenance, mainly removing of expired data if applicable
	 **/
	@Override
	public void maintenance() {
		// does nothing
	}

    //--------------------------------------------------------------------------
	//
	// KeySet support implementation
	//
	//--------------------------------------------------------------------------
	/**
	 * Search using the value, all the relevent key mappings
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key, note that null matches ALL
	 *
	 * @return array of keys
	 **/
	@Override
	public Set<String> keySet(Long value) {
		// The return hashset
		HashSet<String> ret = new HashSet<String>();
		//Fetch everything in current db
		List<String> retList = null;
        if (value != null) {
            retList = new ArrayList<String>(redisMap.readAllKeySet());
            // Return the full keyset
            retList.forEach(k -> ret.add(k));
		}
		return ret;
	}

    //--------------------------------------------------------------------------
	//
	// Fundemental set/get value (core)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Stores (and overwrites if needed) key, value pair
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param expect as Long
	 * @param update as Long
	 *
	 * @return true if successful
	 **/
	@Override
	public boolean weakCompareAndSet(String key, Long expect, Long update) {
		// // Possibly a blank setup
		// if (expect == null) {
		// 	expect = new Long(0);
		// }
		// if (expect.longValue() == 0) {
		// 	backendMap().putIfAbsent(key, expect);
		// }
		
		// // Value update - doa atomically directly
		// return backendMap().replace(key, expect, update);
        return false; //TO REMOVE
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the expire time stamp value, raw without validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param expire TIMESTAMP in milliseconds, 0 means NO expire
	 *
	 * @return 
	 **/
	public void setExpiryRaw(String key, long time) {
		// if (time > 0) {
		// 	backendMap().setTtl(key, Math.max(time - System.currentTimeMillis(), 1),
		// 		TimeUnit.MILLISECONDS);
		// } else {
		// 	backendMap().setTtl(key, 0, TimeUnit.MILLISECONDS);
		// }
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the value, with validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key
	 * @param value, null means removal
	 * @param expire TIMESTAMP, 0 means not timestamp
	 *
	 * @return null
	 **/
	public Long setValueRaw(String key, Long value, long expire) {
		// // removal
		// if (value == null) {
		// 	backendMap().remove(key);
		// 	return null;
		// }
		
		// // Setup key, value - with expirary?
		// if (expire > 0) {
		// 	backendMap().set(key, value, Math.max(expire - System.currentTimeMillis(), 1),
		// 		TimeUnit.MILLISECONDS);
		// } else {
		// 	backendMap().set(key, value);
		// }
		return null;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Returns the value and expiry, with validation against the current timestamp
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param now timestamp, 0 = no timestamp so skip timestamp checks
	 *
	 * @return Long value, and expiry pair
	 **/
	public MutablePair<Long, Long> getValueExpiryRaw(String key, long now) {
		// // Get the entry view
		// EntryView<String, Long> entry = backendMap().getEntryView(key);
		// if (entry == null) {
		// 	return null;
		// }
		
		// // Get the value and expire object : milliseconds?
		// Long value = entry.getValue();
		// Long expireObj = entry.getExpirationTime();
		// if (expireObj == null) {
		// 	expireObj = 0L;
		// }
		
		// // Note: 0 = no timestamp, hence valid value
		// long expiry = expireObj.longValue();
		// if (expiry != 0 && expiry < now) {
		// 	return null;
		// }
		
		// // Return the expirary pair
		// return new MutablePair<Long, Long>(value, expiry);
        return null; // TO REMOVE
	}
    
}
