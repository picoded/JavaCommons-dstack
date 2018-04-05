package picoded.dstack.core;

import picoded.dstack.KeyLong;
import picoded.dstack.KeyLongMap;

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
	 * Returns the value, with validation against the current timestamp
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param now timestamp, 0 = no timestamp so skip timestamp checks
	 *
	 * @return Long value
	 **/
	abstract protected Long getValueRaw(String key, long now);

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
	abstract protected Long setValueRaw(String key, Long value, long expire);

	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Returns the expire time stamp value, raw without validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 *
	 * @return long
	 **/
	abstract protected long getExpiryRaw(String key);

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
	 * Increment the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public long incrementAndGet(Object key) {
		Long value = getValueRaw( (key != null)? key.toString() : null, System.currentTimeMillis());
		value = new Long(value.longValue() + 1);
		setValueRaw((String) key, value, 0);
		return value.longValue();
	}

	/**
	 * Return the current value of the key and increment by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public long getAndIncrement(Object key){
		Long value = getValueRaw( (key != null)? key.toString() : null, System.currentTimeMillis());
		setValueRaw((String) key, new Long(value.longValue() + 1), 0);
		return value.longValue();
	}

	/**
	 * Decrement the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public long decrementAndGet(Object key){
		Long value = getValueRaw( (key != null)? key.toString() : null, System.currentTimeMillis());
		value = new Long(value.longValue() - 1);
		setValueRaw((String) key, value, 0);
		return value.longValue();
	}

	/**
	 * Return the current value of the key and decrement by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public long getAndDecrement(Object key){
		Long value = getValueRaw( (key != null)? key.toString() : null, System.currentTimeMillis());
		setValueRaw((String) key, new Long(value.longValue() - 1), 0);
		return value.longValue();
	}

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
	public boolean weakCompareAndSet(String key, Long expect, Long update) {
		Long curVal = getValueRaw(key, System.currentTimeMillis());

		//if current value is equal to expected value, set to new value
		if (curVal != null && curVal.longValue() == expect.longValue()) {
			setValueRaw(key, update, 0);
			return true;
		} else if (curVal == null || curVal.longValue() == 0l) {
			setValueRaw(key, update, 0);
			return true;
		} else {
			return false;
		}

	}

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
	 * @return long, 0 means no expirary, -1 no data / expire
	 **/
	@Override
	public long getExpiry(String key) {
		long expire = getExpiryRaw(key);
		if (expire <= 0) { //0 = no timestamp, -1 = no data
			return expire;
		}
		if (expire > System.currentTimeMillis()) {
			return expire;
		}
		return -1; //expired
	}

	/**
	 * Returns the lifespan time stamp value
	 *
	 * @param key as String
	 *
	 * @return long, 0 means no expirary, -1 no data / expire
	 **/
	@Override
	public long getLifespan(String key) {
		long expire = getExpiryRaw(key);
		if (expire <= 0) { //0 = no timestamp, -1 = no data
			return expire;
		}

		long lifespan = expire - System.currentTimeMillis();
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
