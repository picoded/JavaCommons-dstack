package picoded.dstack.redisson;

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
import java.util.Iterator;

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
import org.redisson.api.RMapCache;

/**
 * Hazelcast implementation of KeyValueMap data structure.
 *
 * Built ontop of the Core_KeyValueMap implementation.
 **/
public class Redisson_KeyValueMap extends Core_KeyValueMap {
	
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
	public Redisson_KeyValueMap(RedisStack inStack, String name) {
		super();
		redisStack = inStack;
		redisson = inStack.getConnection();
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
	// private RMap<String, Map<String, Object>> _backendRMap = null;
	private RMapCache<String, String>  _backendRMap = null;
	
	/**
	 * @return Storage map used for the backend operations of one "DataObjectMap"
	 *         identical to valueMap, made to be compliant with Core_DataObjectMap_struct
	 */
	// protected RMap<String, Map<String, Object>> backendMap() {
	protected RMapCache<String, String> backendMap() {
		if (_backendRMap != null) {
			return _backendRMap;
		}
		_backendRMap = redisson.getMapCache(name(), JsonJacksonCodec.INSTANCE);
		return _backendRMap;
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
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	public void systemDestroy() {
		backendMap().clear();
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
	public Set<String> keySet(String value) {
		// The return hashset
		HashSet<String> ret = new HashSet<String>();
		List<String> retList = null; 
        if (value == null) {
			// Lets fetch everything
            retList = new ArrayList<String>(backendMap().readAllKeySet());
			// Return the full keyset
			retList.forEach(k -> ret.add(k));
		} else {
			//TODO: not sure abt keyset() vs readAllKeySet()
			retList = new ArrayList<String>(backendMap().keySet());
			for (String key : retList) {
				String val = backendMap().get(key);
				if (value.equals(val)) {
					ret.add(key);
				}
			}
		}
		return ret;
	}
	
	//--------------------------------------------------------------------------
	//
	// Fundemental set/get value (core)
	//
	//--------------------------------------------------------------------------
	
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
		if (time > 0) {
			backendMap()
				.updateEntryExpiration(key, Math.max(time - System.currentTimeMillis(), 1),
					TimeUnit.MILLISECONDS,0,TimeUnit.MILLISECONDS);
		} else {
			backendMap()
				.updateEntryExpiration(key, 0, 
					TimeUnit.MILLISECONDS,0,TimeUnit.MILLISECONDS);
		}	
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
	public String setValueRaw(String key, String value, long expire) {
		// removal
		if (value == null) {
			backendMap().remove(key);
			return null;
		}
		
		// Setup key, value - with expirary?
		if (expire > 0) {
			backendMap().fastPut(key, value, Math.max(expire - System.currentTimeMillis(), 1),
				TimeUnit.MILLISECONDS);
		} else {
			backendMap().fastPut(key, value);
		}
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
	 * @return String value, and expiry pair
	 **/
	public MutablePair<String, Long> getValueExpiryRaw(String key, long now) {

		Set<String> keys = new HashSet<String>();
		Map<String, String> mapSlice = new HashMap<String, String>();
		Map.Entry<String,String> entry = null;

		keys.add(key);
		mapSlice = backendMap().getAll(keys);

		Iterator<Map.Entry<String,String>> it = mapSlice.entrySet().iterator();
		if(it.hasNext()){
			entry = it.next();
			if (entry == null) {
				return null;
			}
			String value = entry.getValue();

			Long expireObj = backendMap().remainTimeToLive(key) + System.currentTimeMillis();
			// Note: 0 = no timestamp, hence valid value
			long expiry = expireObj.longValue();
			return new MutablePair<String, Long>(value, expiry);
		}
		return null;
	}
}
