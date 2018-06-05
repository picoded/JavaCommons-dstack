package picoded.dstack.core;

import picoded.core.conv.GenericConvert;
import picoded.dstack.KeyLong;
import picoded.dstack.KeyLongMap;
import picoded.core.struct.MutablePair;

/**
 * Common base utility class of KeyLongMap.
 *
 * Does not actually implement its required feature,
 * but helps provide a common base line for all the various implementation.
 **/
public abstract class Core_KeyLongMap extends Core_DataStructure<String, KeyLong> implements
		KeyLongMap {

	//--------------------------------------------------------------------------
	//
	// Basic get operation
	//
	//--------------------------------------------------------------------------

	/**
	 * Returns the KeyValue object, given the key identifier, if found
	 *
	 * @param key identifier to lookup value
	 *
	 * @return  KeyValue object if found
	 **/
	@Override
	public KeyLong getKeyLong(Object key) {
		if( key == null ) {
			throw new RuntimeException("key parameter cannot be NULL");
		}
		return new Core_KeyLong(this, key.toString());
	}

	//--------------------------------------------------------------------------
	//
	// raw put & get, meant to be actually implemented.
	// [Internal use, to be extended in future implementation]
	//
	//--------------------------------------------------------------------------

	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Sets the value, with validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key
	 * @param value, null means removal
	 * @param expire timestamp, 0 means no timestamp
	 *
	 * @return null
	 **/
	abstract public Long setValueRaw(String key, Long value, long expire);

	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Sets the expire time stamp value, raw without validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param expire timestamp in seconds, 0 means NO expire
	 *
	 * @return long
	 **/
	abstract public void setExpiryRaw(String key, long expire);
	
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
	abstract public MutablePair<Long,Long> getValueExpiryRaw(String key, long now);

	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Returns the value, with validation against the current timestamp
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param now timestamp, 0 = no timestamp so skip timestamp checks
	 *
	 * @return Long value
	 **/
	public Long getValueRaw(String key, long now) {
		MutablePair<Long, Long> pair = getValueExpiryRaw(key, now);
		if( pair != null ) {
			return pair.getLeft().longValue();
		}
		return null;
	}

	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Returns the expire time stamp value, raw without validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param now timestamp, 0 = no timestamp so skip timestamp checks
	 *
	 * @return long
	 **/
	public long getExpiryRaw(String key, long now) {
		MutablePair<Long,Long> pair = getValueExpiryRaw(key, now);
		if( pair != null ) {
			return pair.getRight().longValue();
		}
		return -1;
	}


	//--------------------------------------------------------------------------
	//
	// Basic get and put
	//
	//--------------------------------------------------------------------------

	/**
	 * Stores (and overwrites if needed) key, value pair
	 *
	 * Important note: It does not return the previously stored value
	 * Its return String type is to maintain consistency with Map interfaces
	 *
	 * @param key as String
	 * @param value as Long
	 *
	 * @return null
	 **/
	@Override
	public Long putValue(String key, Long value) {
		setValueRaw(key, value, 0);
		return null;
	}

	/**
	 * Returns the value, given the key
	 *
	 * Null return can either represent no value or expired value.
	 *
	 * @param key param find the thae meta key
	 *
	 * @return  value of the given key
	 **/
	@Override
	public Long getValue(Object key) {
		return getValueRaw( (key != null)? key.toString() : null, System.currentTimeMillis());
	}

	//--------------------------------------------------------------------------
	//
	// Incremental operations
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
	abstract public boolean weakCompareAndSet(String key, Long expect, Long update);

	//--------------------------------------------------------------------------
	//
	// Expiration and lifespan handling
	//
	// Built using getExpiryRaw and setExpiryRaw
	//
	//--------------------------------------------------------------------------

	/**
	 * Returns the expire time stamp value, if still valid
	 *
	 * @param key as String
	 *
	 * @return long, 0 means no expiry, -1 no data / expire
	 **/
	@Override
	public long getExpiry(String key) {
		// Get the value / expiry value pair
		MutablePair<Long,Long> pair = getValueExpiryRaw(key, System.currentTimeMillis());

		// No data found
		if( pair == null ) {
			return -1;
		}

		// Return expirary
		return pair.getRight().longValue();
	}

	/**
	 * Returns the lifespan time stamp value
	 *
	 * @param key as String
	 *
	 * @return long, 0 means no expiry, -1 no data / expire
	 **/
	@Override
	public long getLifespan(String key) {
		// Time stamp to use now
		long now = System.currentTimeMillis();

		// Get the value / expiry value pair
		MutablePair<Long,Long> pair = getValueExpiryRaw(key, now);
		
		// No data found
		if( pair == null ) {
			return -1;
		}

		// Get expire timestamp
		long expire = pair.getRight();

		//0 = no timestamp, -1 = no data
		if (expire <= 0) { 
			return expire;
		}
		
		// Calculate the livespan
		long lifespan = expire - now;
		if (lifespan <= 0) {
			return -1; //expired
		}
		return lifespan;
	}

	/**
	 * Sets the expire time stamp value, if still valid
	 *
	 * @param key
	 * @param expire timestamp in seconds, 0 means NO expire
	 **/
	@Override
	public void setExpiry(String key, long expire) {
		setExpiryRaw(key, expire);
	}

	/**
	 * Sets the expire time stamp value, if still valid
	 *
	 * @param key
	 * @param lifespan
	 **/
	@Override
	public void setLifeSpan(String key, long lifespan) {
		setExpiryRaw(key, lifespan + System.currentTimeMillis());
	}

	/**
	 * Stores (and overwrites if needed) key, value pair
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param value as String
	 * @param lifespan time to expire in seconds
	 *
	 * @return null
	 **/
	@Override
	public Long putWithLifespan(String key, Long value, long lifespan) {
		return setValueRaw(key, value, (lifespan <= 0) ? -1 : System.currentTimeMillis() + lifespan);
	}

	/**
	 * Stores (and overwrites if needed) key, value pair
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param value as String
	 * @param expireTime expire time stamp value
	 *
	 * @return Long
	 **/
	@Override
	public Long putWithExpiry(String key, Long value, long expireTime) {
		return setValueRaw(key, value, expireTime);
	}

}
