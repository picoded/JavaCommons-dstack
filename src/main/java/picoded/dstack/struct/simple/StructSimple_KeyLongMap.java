package picoded.dstack.struct.simple;

import picoded.dstack.core.Core_KeyLongMap;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StructSimple_KeyLongMap extends Core_KeyLongMap {

	//--------------------------------------------------------------------------
	//
	// Constructor vars
	//
	//--------------------------------------------------------------------------

	/**
	 * Stores the key to long map
	 **/
	protected ConcurrentMap<String, Long> longMap = new ConcurrentHashMap<String, Long>();

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
	public Set<String> keySet(Long value) {
		try {
			accessLock.readLock().lock();

			long now = System.currentTimeMillis();
			Set<String> ret = new HashSet<String>();

			// The keyset to check against
			Set<String> valuekeySet = longMap.keySet();

			// Iterate and get
			for (String key : valuekeySet) {
				Long rawValue = getValueRaw(key, now);
				if (rawValue != null && (value == null || rawValue.longValue() == value.longValue())) {
					ret.add(key);
				}
			}

			return ret;
		} finally {
			accessLock.readLock().unlock();
		}
	}

	//--------------------------------------------------------------------------
	//
	// Fundamental set/get value (core)
	//
	//--------------------------------------------------------------------------

	/**
	 * [Internal use, to be extended in future implementation]
	 * Returns the value, with validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param now timestamp
	 *
	 * @return Long value
	 **/
	protected Long getValueRaw(String key, long now) {
		try {
			accessLock.readLock().lock();

			Long val = longMap.get(key);
			if (val == null) {
				return null;
			}

			// Note: 0 = no timestamp, hence valid value
			long expiry = getExpiryRaw(key);
			if (expiry != 0 && expiry < now) {
				return null;
			}

			return val;
		} finally {
			accessLock.readLock().unlock();
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
	 * @param expire timestamp, 0 means not timestamp
	 *
	 * @return null
	 **/
	protected Long setValueRaw(String key, Long value, long expire) {
		try {
			accessLock.writeLock().lock();
			if (value == null) {
				longMap.remove(key);
				expireMap.remove(key);
			} else {
				longMap.put(key, value);
			}
			setExpiryRaw(key, expire);
			return null;
		} finally {
			accessLock.writeLock().unlock();
		}
	}

	//--------------------------------------------------------------------------
	//
	// Expiration and lifespan handling (core)
	//
	//--------------------------------------------------------------------------

	/**
	 * [Internal use, to be extended in future implementation]
	 * Returns the expire time stamp value, raw without validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 *
	 * @return long
	 **/
	protected long getExpiryRaw(String key) {
		try {
			accessLock.readLock().lock();

			// no value fails
			if (longMap.get(key) == null) {
				return -1;
			}

			// Expire value?
			Long expireObj = expireMap.get(key);
			if (expireObj == null) {
				return 0;
			}
			return expireObj.longValue();
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
	 * @return long
	 **/
	public void setExpiryRaw(String key, long time) {
		try {
			accessLock.writeLock().lock();

			// Does nothing if empty
			if (time <= 0 || longMap.get(key) == null) {
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
					longMap.remove(key);
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
			longMap.clear();
			expireMap.clear();
		} finally {
			accessLock.writeLock().unlock();
		}
	}
}
