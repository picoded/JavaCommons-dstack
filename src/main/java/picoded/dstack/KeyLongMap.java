package picoded.dstack;

import java.util.Set;
import picoded.core.struct.GenericConvertMap;

public interface KeyLongMap extends GenericConvertMap<String, KeyLong>, CommonStructure {

	//--------------------------------------------------------------------------
	//
	// Basic KeyValue object put / get / remove operations (for map support)
	//
	//--------------------------------------------------------------------------

	/**
	 * Gets and return the KeyValue, regardless if any value is stored (or expired)
	 *
	 * @param key identifier to lookup value
	 *
	 * @return  KeyLong object (does not validate if it exists)
	 */
	KeyLong getKeyLong(Object key);

	/**
	 * Returns the KeyLong, given the key
	 *
	 * Null return can either represent no value or expired value.
	 * Note that unless needed, it is highly recommended to use getValue instead
	 *
	 * @param key identifier to lookup value
	 *
	 * @return  KeyLong object if found
	 **/
	@Override
	default KeyLong get(Object key) {
		KeyLong r = getKeyLong(key);
		if(r.getLifespan() >= 0) {
			return r;
		}
		return null;
	}

	/**
	 * Stores (and overwrites if needed) the value at the given key
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param value as Long, as such its "key" is ignored when used here
	 *
	 * @return null
	 **/
	@Override
	default KeyLong put(String key, KeyLong value) {
		if( value == null ) {
			putValue(key, null);
		} else {
			putValue(key, value.getValue());
		}
		return null;
	}

	/**
	 * Convinence varient of put, where string value is used instead
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param value as Long, as such its "key" is ignored when used here
	 *
	 * @return null
	 **/
	default KeyLong put(String key, Long value) {
		putValue(key, value);
		return null;
	}

	/**
	 * Remove the value, given the key
	 *
	 * @param key where the value is stored
	 *
	 * @return  null
	 **/
	@Override
	default KeyLong remove(Object key) {
		removeValue(key);
		return null;
	}

	//--------------------------------------------------------------------------
	//
	// Basic put / get / remove operations performed
	// directly on the stored value
	//
	//--------------------------------------------------------------------------

	/**
	 * Returns the value, given the key
	 *
	 * Null return can either represent no value or expired value.
	 *
	 * @param key param find the the meta key
	 *
	 * @return  value of the given key
	 **/
	Long getValue(Object key);

	/**
	 * Stores (and overwrites if needed) key, value pair
	 *
	 * Important note: It does not return the previously stored value
	 * Its return String type is to maintain consistency with Map interfaces
	 *
	 * @param key as String
	 * @param value as String
	 *
	 * @return null
	 **/
	Long putValue(String key, Long value);

	/**
	 * Remove the value, given the key
	 *
	 * Important note: It does not return the previously stored value
	 * Its return String type is to maintain consistency with Map interfaces
	 *
	 * @param key param find the thae meta key
	 *
	 * @return  null
	 **/
	default Long removeValue(Object key) {
		return putValue((String)key, null);
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
	long incrementAndGet(Object key);

	/**
	 * Return the current value of the key and increment by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	long getAndIncrement(Object key);

	/**
	 * Decrement the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	long decrementAndGet(Object key);

	/**
	 * Return the current value of the key and decrement by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	long getAndDecrement(Object key);

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
	boolean weakCompareAndSet(String key, Long expect, Long update);

	//--------------------------------------------------------------------------
	//
	// Other common map operations
	//
	//--------------------------------------------------------------------------

	/**
	 * Contains key operation.
	 *
	 * Boolean false can either represent no value or expired value
	 *
	 * @param key as String
	 * @return boolean true or false if the key exists
	 **/
	@Override
	default boolean containsKey(Object key) {
		return getLifespan(key.toString()) >= 0;
	}

	/**
	 * [warning] : avoid use in production, use a DataTable instead.
	 *
	 * Use only for debugging. Returns all the valid keys.
	 * Kept to ensure full map interface compatibility
	 *
	 * NOTE: You DO NOT want to use this, as most KeyValueMap,
	 * systems are NOT designed for this operation. And will do so
	 * the hard way (iterating the entire data structure).
	 *
	 * @return  the full keyset
	 **/
	@Override
	default Set<String> keySet() {
		return keySet(null);
	}

	/**
	 * [warning] : avoid use in production, use a DataTable instead.
	 *
	 * NOTE: You DO NOT want to use this, as most KeyValueMap,
	 * systems are NOT designed for this operation. And will do so
	 * the hard way (searching every data value)
	 *
	 * @param value to search, note that null matches ALL. This is used by keySet()
	 *
	 * @return array of keys
	 **/
	Set<String> keySet(Long value);

	//--------------------------------------------------------------------------
	//
	// Expiration and lifespan handling
	//
	//--------------------------------------------------------------------------

	/**
	 * Returns the expire time stamp value, if still valid
	 *
	 * @param key as String
	 *
	 * @return long, 0 means no expirary, -1 no data / expired
	 **/
	long getExpiry(String key);

	/**
	 * Returns the lifespan time stamp value
	 *
	 * @param key as String
	 *
	 * @return long, 0 means no expirary, -1 no data / expired
	 **/
	long getLifespan(String key);

	/**
	 * Sets the expire time stamp value, if still valid
	 *
	 * @param key as String
	 * @param expireTimestamp expire unix timestamp value in milliseconds
	 **/
	void setExpiry(String key, long expireTimestamp);

	/**
	 * Sets the expire time stamp value, if still valid
	 *
	 * @param key as String
	 * @param lifespan time to expire in milliseconds
	 **/
	void setLifeSpan(String key, long lifespan);

	//--------------------------------------------------------------------------
	//
	// Extended map operations
	//
	//--------------------------------------------------------------------------

	/**
	 * Stores (and overwrites if needed) key, value pair
	 * with lifespan value.
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param value as Long
	 * @param lifespan time to expire in milliseconds
	 *
	 * @return null
	 **/
	Long putWithLifespan(String key, Long value, long lifespan);

	/**
	 * Stores (and overwrites if needed) key, value pair
	 * with expirary value.
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param value as Long
	 * @param expireTimestamp expire unix timestamp value in milliseconds
	 *
	 * @return String
	 **/
	Long putWithExpiry(String key, Long value, long expireTimestamp);

	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown / maintenance
	//
	//--------------------------------------------------------------------------

	/**
	 * Removes all data, without tearing down setup
	 *
	 * This is equivalent of "TRUNCATE TABLE {TABLENAME}"
	 *
	 * Note: that this is here to help resolve the interface conflict
	 **/
	default void clear() {
		((GenericConvertMap<String, KeyLong>) this).clear();
	}

}
