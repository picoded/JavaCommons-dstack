package picoded.dstack;

import picoded.core.struct.GenericConvertValue;

/**
 * Represents a single pair of key and value.
 * 
 * Implemented as part of KeyValueMap
 **/
public interface KeyValue extends GenericConvertValue<String> {

	/**
	 * Get and return the key used to store the value
	 * 
	 * @return  the key representing the value
	 */
	String getKey();

	//--------------------------------------------------------------------------
	//
	// Expiration and lifespan handling
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Returns the expire time stamp value, if still valid
	 *
	 * @return long, 0 means no expiry, -1 no data / expired
	 **/
	long getExpiry();
	
	/**
	 * Returns the lifespan time stamp value
	 *
	 * @return long, 0 means no expiry, -1 no data / expired
	 **/
	long getLifespan();
	
	/**
	 * Sets the expire time stamp value, if still valid
	 *
	 * @param expireTimestamp expire unix timestamp value in milliseconds
	 **/
	void setExpiry(long expireTimestamp);
	
	/**
	 * Sets the expire time stamp value, if still valid
	 *
	 * @param lifespan time to expire in milliseconds
	 **/
	void setLifeSpan(long lifespan);
	
	/**
	 * Stores (and overwrites if needed) key, value pair
	 * with lifespan value.
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param value as String
	 * @param lifespan time to expire in milliseconds
	 *
	 * @return null
	 **/
	String putWithLifespan(String value, long lifespan);
	
	/**
	 * Stores (and overwrites if needed) key, value pair
	 * with expiry value.
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param value as String
	 * @param expireTimestamp expire unix timestamp value in milliseconds
	 *
	 * @return String
	 **/
	String putWithExpiry(String value, long expireTimestamp);
	
}