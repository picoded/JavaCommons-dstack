package picoded.dstack.struct.simple;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import picoded.dstack.KeyValueMap;
import picoded.dstack.core.Core_KeyValueMap;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.MutablePair;
import picoded.core.struct.GenericConvertHashMap;

/**
 * Reference implementation of KeyValueMap data structure.
 * This is done via a minimal implementation via internal data structures.
 *
 * Built ontop of the Core_KeyValueMap implementation.
 **/
public class StructSimple_KeyValueMap extends Core_KeyValueMap {
	
	//--------------------------------------------------------------------------
	//
	// Constructor vars
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Stores the key to value map
	 **/
	protected ConcurrentMap<String, String> valueMap = new ConcurrentHashMap<String, String>();
	
	/**
	 * Stores the expire timestamp
	 **/
	protected ConcurrentMap<String, Long> expireMap = new ConcurrentHashMap<String, Long>();
	
	/**
	 * Read write lock
	 **/
	protected ReentrantReadWriteLock accessLock = new ReentrantReadWriteLock();
	
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
		try {
			accessLock.readLock().lock();
			
			long now = System.currentTimeMillis();
			Set<String> ret = new HashSet<String>();
			
			// The keyset to check against
			Set<String> valuekeySet = valueMap.keySet();
			
			// Iterate and get
			for (String key : valuekeySet) {
				
				// Get the value pair, if it has a valid value
				MutablePair<String,Long> pair = getValueExpiryRaw_noLocking(key, now);
				if( pair == null ) {
					continue;
				}

				// Validate the rawValue
				String rawValue = pair.getLeft();
				if (rawValue != null && (value == null || rawValue.equals(value))) {
					ret.add(key);
				}
			}
			
			// Return the full keyset
			return ret;
		} finally {
			accessLock.readLock().unlock();
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// Fundemental set/get value (core)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the value, with validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key
	 * @param value, null means removal
	 * @param expire timestamp, 0 means not timestamp
	 *
	 * @return null
	 **/
	public String setValueRaw(String key, String value, long expire) {
		try {
			accessLock.writeLock().lock();
			if (value == null) {
				valueMap.remove(key);
				expireMap.remove(key);
			} else {
				valueMap.put(key, value);
			}
			setExpiryRaw(key, expire);
			return null;
		} finally {
			accessLock.writeLock().unlock();
		}
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
	protected MutablePair<String,Long> getValueExpiryRaw_noLocking(String key, long now) {
		String val = valueMap.get(key);
		if (val == null) {
			return null;
		}
		
		// Get the expire object
		Long expireObj = expireMap.get(key);
		if(expireObj == null) {
			expireObj = 0L;
		}

		// Note: 0 = no timestamp, hence valid value
		long expiry = expireObj.longValue();
		if (expiry != 0 && expiry < now) {
			return null;
		}

		// Return the expirary pair
		return new MutablePair<String,Long>(val,expiry);
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
	public MutablePair<String,Long> getValueExpiryRaw(String key, long now) {
		try {
			accessLock.readLock().lock();
			return getValueExpiryRaw_noLocking(key, now);
		} finally {
			accessLock.readLock().unlock();
		}
	}

	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the expire time stamp value, raw without validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param expire timestamp in seconds, 0 means NO expire
	 *
	 * @return 
	 **/
	public void setExpiryRaw(String key, long time) {
		try {
			accessLock.writeLock().lock();
			
			// Does nothing if empty
			if (time <= 0 || valueMap.get(key) == null) {
				expireMap.remove(key);
				return;
			}
			
			// Set expire value
			expireMap.put(key, time);
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown / maintenance (DStackCommon)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Sets up the backend storage. If needed.
	 * The SQL equivalent would be "CREATE TABLE {TABLENAME} IF NOT EXISTS"
	 **/
	@Override
	public void systemSetup() {
		//clear();
	}
	
	/**
	 * Destroy, Teardown and delete the backend storage. If needed
	 * The SQL equivalent would be "DROP TABLE {TABLENAME}"
	 **/
	@Override
	public void systemDestroy() {
		clear();
	}
	
	/**
	 * Perform maintenance, mainly removing of expired data if applicable
	 *
	 * Handles re-entrant lock where applicable
	 **/
	@Override
	public void maintenance() {
		try {
			accessLock.writeLock().lock();
			
			// The time to check against
			long now = System.currentTimeMillis();
			
			// not iterated directly due to remove()
			Set<String> expireKeySet = expireMap.keySet();
			
			// The keyset to check against
			String[] expireKeyArray = expireKeySet.toArray(new String[expireKeySet.size()]);
			
			// Iterate and evict
			for (String key : expireKeyArray) {
				Long timeObj = expireMap.get(key);
				//				long time = (timeObj != null) ? timeObj.longValue() : 0;
				long time = timeObj.longValue();
				// expired? kick it
				if (time < now && time > 0) {
					valueMap.remove(key);
					expireMap.remove(key);
				}
			}
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
	/**
	 * Removes all data, without tearing down setup
	 *
	 * Handles re-entrant lock where applicable
	 **/
	@Override
	public void clear() {
		try {
			accessLock.writeLock().lock();
			valueMap.clear();
			expireMap.clear();
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
}
