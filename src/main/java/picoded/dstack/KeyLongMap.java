package picoded.dstack;

import java.util.Set;

import javax.management.RuntimeErrorException;

import picoded.core.conv.GenericConvert;
import picoded.core.struct.GenericConvertMap;

/**
 * Reference interface of KeyLongMap Map data structure
 *
 * This is intended to be an optimized key numeric map data storage
 * Used mainly in caching or performance critical scenerios, with atomic support
 *
 * As such its sacrifices much utility for performance (eg: lack of query support)
 *
 * Note : expire timestamps are measured in milliseconds.
 * 
 * Note : KeyLong class simply serves as a convinence means to pass the 
 *        value representation without passing the whole map. 
 **/
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
		if (r.getLifespan() >= 0) {
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
		if (value == null) {
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
	 * Convinence varient of put, where string value is used instead
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param value as Long, as such its "key" is ignored when used here
	 *
	 * @return null
	 **/
	default KeyLong putLong(String key, Long value) {
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
		return putValue((String) key, null);
	}
	
	//--------------------------------------------------------------------------
	//
	// Incremental operations
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Returns the value, given the key
	 *
	 * @param key param find the meta key
	 * @param delta value to add
	 *
	 * @return  value of the given key after adding
	 **/
	default Long addAndGet(Object key, Object delta) {
		//
		// We simply use get and add, with the delta,
		// this reduce the amount of permutation needed to support
		//
		return getAndAdd(key, delta)+GenericConvert.toLong(delta);
	}
	
	/**
	 * Returns the value, given the key. Then apply the delta change
	 *
	 * @param key param find the meta key
	 * @param delta value to add
	 *
	 * @return  value of the given key, note that it returns 0 if there wasnt a previous value set
	 **/
	default Long getAndAdd(Object key, Object delta) {
		//
		// NOTE : The default implmentation of addAndGet,
		//        or getAndAdd relies on repetaed tries using
		//        weakCompareAndSet, while functional.
		//        Is highly inefficent in most cases
		//
		
		// Validate and convert the key to String
		if (key == null) {
			throw new IllegalArgumentException("key cannot be null in");
		}
		String keyAsString = key.toString();
		
		// Attempt to update the key for 5 times before throwing exception
		for (int tries = 0; tries < 5; tries++) {
			// Retrieve value from key
			Long value = getValue(keyAsString);
			
			// Assume value as 0 if not exist
			if (value == null) {
				value = new Long(0);
			}
			
			// Calculate the updated value
			Long updatedValue = GenericConvert.toLong(delta) + value;
			
			// Update the value with weakCompareAndSet and return the original value
			if (weakCompareAndSet(keyAsString, value, updatedValue)) {
				return value;
			}
		}
		
		// Throw exception due to number of retries exceeded the limit
		throw new RuntimeException("Number of retries exceeded limit");
	}
	
	/**
	 * Increment the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	default Long incrementAndGet(Object key) {
		return addAndGet(key, 1);
	}
	
	/**
	 * Return the current value of the key and increment by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	default Long getAndIncrement(Object key) {
		return getAndAdd(key, 1);
	}
	
	/**
	 * Decrement the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	default Long decrementAndGet(Object key) {
		return addAndGet(key, -1);
	}
	
	/**
	 * Return the current value of the key and decrement by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	default Long getAndDecrement(Object key) {
		return getAndAdd(key, -1);
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
	 * @return long, 0 means no expiry, -1 no data / expired
	 **/
	long getExpiry(String key);
	
	/**
	 * Returns the lifespan time stamp value
	 *
	 * @param key as String
	 *
	 * @return long, 0 means no expiry, -1 no data / expired
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
	 * with expiry value.
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
