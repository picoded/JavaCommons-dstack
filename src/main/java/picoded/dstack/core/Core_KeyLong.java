package picoded.dstack.core;

import picoded.dstack.KeyLong;

import java.util.concurrent.locks.ReentrantLock;

public class Core_KeyLong implements KeyLong {

	//--------------------------------------------------------------------------
	//
	// Core variables
	//
	//--------------------------------------------------------------------------

	/**
	 * Core_KeyValueMap used for the object
	 * Used to provide the underlying backend implementation
	 **/
	protected Core_KeyLongMap main = null;

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
	 * Setup a Core_KeyLong against a Core_KeyLongMap backend.
	 *
	 * This allow the setup in the following modes
	 *
	 * @param  keyLongMap backend to use
	 * @param  key used to identify the stored value
	 */
	public Core_KeyLong(Core_KeyLongMap keyLongMap, String inKey) {
		main = keyLongMap;
		key = inKey;
	}

	//--------------------------------------------------------------------------
	//
	// Incremental implementation
	//
	//--------------------------------------------------------------------------

	/**
	 * Increment the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public Long incrementAndGet(Object key) {
		Long value = main.incrementAndGet(key);
		return value;
	}

	/**
	 * Return the current value of the key and increment by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public Long getAndIncrement(Object key){
		Long value = main.getAndIncrement(key);
		return value;
	}

	/**
	 * Decrement the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public Long decrementAndGet(Object key){
		Long value = main.decrementAndGet(key);
		return value;
	}

	/**
	 * Return the current value of the key and decrement by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public Long getAndDecrement(Object key){
		Long value = main.getAndDecrement(key);
		return value;
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
	public void putValue(Long value) {
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
	public Long getValue() {
		return main.getValue(key);
	}

	/**
	 * Default to String conversion of generic value
	 *
	 * @return The converted string, always possible unless null
	 **/
	@Override
	public String toString() {
		return getValue().toString();
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
	 * @return long, 0 means no expirary, -1 no data / expired
	 **/
	public long getExpiry() {
		return main.getExpiry(key);
	}

	/**
	 * Returns the lifespan time stamp value
	 *
	 * @return long, 0 means no expirary, -1 no data / expired
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
	public Long putWithLifespan(Long value, long lifespan) {
		return main.putWithLifespan(key, value, lifespan);
	}

	/**
	 * Stores (and overwrites if needed) key, value pair
	 * with expirary value.
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param value as String
	 * @param expireTimestamp expire unix timestamp value in milliseconds
	 *
	 * @return String
	 **/
	public Long putWithExpiry(Long value, long expireTimestamp) {
		return main.putWithExpiry(key, value, expireTimestamp);
	}
}
