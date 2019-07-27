package picoded.dstack.core;

import java.util.HashSet;
import java.util.Set;

import picoded.dstack.KeyValue;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.GenericConvertHashMap;

/**
 * Represents a single key value pair in the KeyValueMap collection.
 * 
 * This class is more of a convinence class, and proxies the actual
 * request over to Core_KeyValueMap
 **/
public class Core_KeyValue implements KeyValue {
	
	//--------------------------------------------------------------------------
	//
	// Core variables
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Core_KeyValueMap used for the object
	 * Used to provide the underlying backend implementation
	 **/
	protected Core_KeyValueMap main = null;
	
	/**
	 * Current keyname used to identify the stored value
	 */
	protected String key = null;
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Setup a Core_KeyValue against a Core_KeyValueMap backend.
	 *
	 * This allow the setup in the following modes
	 *
	 * @param  keyValueMap backend to use
	 * @param  key used to identify the stored value
	 */
	public Core_KeyValue(Core_KeyValueMap keyValueMap, String inKey) {
		main = keyValueMap;
		key = inKey;
	}
	
	//--------------------------------------------------------------------------
	//
	// Generic convert value implementation
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Update the currently stored value 
	 * 
	 * @param  value to update to
	 **/
	public void putValue(String value) {
		main.putValue(key, value);
	}
	
	/**
	 * Remove the currently stored value
	 **/
	public void removeValue() {
		main.remove(key);
	}
	
	/**
	 * [Needs to be overriden, currently throws UnsupportedOperationException]
	 * 
	 * Get and return its stored value.
	 *
	 * @return value used for generic convert where applicable
	 **/
	public String getValue() {
		return main.getValue(key);
	}
	
	/**
	 * Default to String conversion of generic value
	 *
	 * @return The converted string, always possible unless null
	 **/
	@Override
	public String toString() {
		return getValue();
	}
	
	//--------------------------------------------------------------------------
	//
	// Key name identifier handling
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Get and return the key used to store the value
	 * 
	 * @return  the key representing the value
	 */
	public String getKey() {
		return key;
	}
	
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
	public long getExpiry() {
		return main.getExpiry(key);
	}
	
	/**
	 * Returns the lifespan time stamp value
	 *
	 * @return long, 0 means no expiry, -1 no data / expired
	 **/
	public long getLifespan() {
		return main.getLifespan(key);
	}
	
	/**
	 * Sets the expire time stamp value, if still valid
	 *
	 * @param expireTimestamp expire unix timestamp value in milliseconds
	 **/
	public void setExpiry(long expireTimestamp) {
		main.setExpiry(key, expireTimestamp);
	}
	
	/**
	 * Sets the expire time stamp value, if still valid
	 *
	 * @param lifespan time to expire in milliseconds
	 **/
	public void setLifeSpan(long lifespan) {
		main.setLifeSpan(key, lifespan);
	}
	
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
	public String putWithLifespan(String value, long lifespan) {
		return main.putWithLifespan(key, value, lifespan);
	}
	
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
	public String putWithExpiry(String value, long expireTimestamp) {
		return main.putWithExpiry(key, value, expireTimestamp);
	}
	
}