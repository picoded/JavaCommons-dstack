package picoded.dstack;

import java.util.Set;

import picoded.core.security.NxtCrypt;
import picoded.core.struct.GenericConvertMap;
import picoded.core.conv.GUID;

/**
 * Reference interface of KeyValueMap Map data structure
 *
 * This is intended to be an optimized key value map data storage
 * Used mainly in caching or performance critical scenerios.
 *
 * As such its sacrifices much utility for performance (eg: lack of query support)
 *
 * Its value type is also intentionally a String, to ensure compatibility
 * with a large number of String based caching systems. Additionally,
 * NULL is considered a delete value.
 *
 * Note : expire timestamps are measured in milliseconds.
 * 
 * Note : KeyValue class simply serves as a convinence means to pass the 
 *        value representation without passing the whole map
 **/
public interface KeyValueMap extends GenericConvertMap<String, KeyValue>, CommonStructure {
	
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
	 * @return  KeyValue object (does not validate if it exists)
	 */
	KeyValue getKeyValue(Object key);

	/**
	 * Returns the KeyValue, given the key
	 *
	 * Null return can either represent no value or expired value.
	 * Note that unless needed, it is highly recommended to use getValue instead
	 *
	 * @param key identifier to lookup value
	 *
	 * @return  KeyValue object if found
	 **/
	@Override
	default KeyValue get(Object key) {
		KeyValue r = getKeyValue(key);
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
	 * @param value as String, as such its "key" is ignored when used here
	 *
	 * @return null
	 **/
	@Override
	default KeyValue put(String key, KeyValue value) {
		if( value == null ) {
			putValue(key, null);
		} else {
			putValue(key, value.toString());
		}
		return null;
	}
	
	/**
	 * Convinence varient of put, where string value is used instead
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param value as String, as such its "key" is ignored when used here
	 *
	 * @return null
	 **/
	default KeyValue put(String key, String value) {
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
	default KeyValue remove(Object key) {
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
	 * @param key param find the thae meta key
	 *
	 * @return  value of the given key
	 **/
	String getValue(Object key);
	
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
	String putValue(String key, String value);
	
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
	default String removeValue(Object key) {
		return putValue((String)key, null);
	}
	
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
	Set<String> keySet(String value);
	
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
	 * @param value as String
	 * @param lifespan time to expire in milliseconds
	 *
	 * @return null
	 **/
	String putWithLifespan(String key, String value, long lifespan);
	
	/**
	 * Stores (and overwrites if needed) key, value pair
	 * with expirary value.
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param value as String
	 * @param expireTimestamp expire unix timestamp value in milliseconds
	 *
	 * @return String
	 **/
	String putWithExpiry(String key, String value, long expireTimestamp);
	
	//--------------------------------------------------------------------------
	//
	// Nonce operations suppport (public)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Generates a random nonce hash, and saves the value into it
	 *
	 * This can be reconfigured via the following config map value
	 * + NonceLifespan
	 * + NonceKeyLength
	 *
	 * When in doubt, use the default of 3600 seconds or about 1 hour.
	 * And 22 characters, which is consistent with Base58 GUID
	 *
	 * @param value to store as string
	 *
	 * @return String value of the random key generated
	 **/
	default String generateNonceKey(String val) {
		return generateNonceKey(val, configMap().getLong("NonceLifespan", 3600*1000));
	}
	
	/**
	 * Generates a random nonce hash, and saves the value to it
	 *
	 * This can be reconfigured via the following config map value
	 * + NonceKeyLength
	 *
	 * When in doubt, use the default of 3600 seconds or about 1 hour.
	 * And 22 characters, which is consistent with Base58 GUID
	 *
	 * @param value to store as string
	 * @param lifespan time to expire in seconds
	 *
	 * @return String value of the random key generated
	 **/
	default String generateNonceKey(String val, long lifespan) {
		return generateNonceKey(val, lifespan, configMap().getInt("NonceKeyLength", 22));
	}
	
	/**
	 * Generates a random nonce hash, and saves the value to it
	 *
	 * Note that the random nonce value returned, is based on picoded.util.security.NxtCrypt.randomString.
	 * Note that this relies on true random to avoid collisions, and if it occurs. Values are over-written
	 *
	 * @param keyLength random key length size
	 * @param value to store as string
	 * @param lifespan time to expire in seconds
	 *
	 * @return String value of the random key generated
	 **/
	default String generateNonceKey(String val, long lifespan, int keyLength) {
		String res = null;

		// Use base58 guid for keylength == 22
		if( keyLength == 22 ) {
			res = GUID.base58();
		} else {
			res = NxtCrypt.randomString(keyLength);
		}
		putWithLifespan(res, val, lifespan);
		return res;
	}
	
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
		((GenericConvertMap<String, KeyValue>) this).clear();
	}
	
}
