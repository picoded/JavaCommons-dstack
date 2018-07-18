package picoded.dstack;

import picoded.core.struct.GenericConvertValue;
import java.util.concurrent.atomic.AtomicLong;

public abstract class KeyLong extends AtomicLong {
	


	// /**
	//  * Get and return the key used to store the value
	//  *
	//  * @return  the key representing the value
	//  */
	// String getKey();
	
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
	abstract public long getExpiry();
	
	/**
	 * Returns the lifespan time stamp value
	 *
	 * @return long, 0 means no expiry, -1 no data / expired
	 **/
	abstract public long getLifespan();
	
	/**
	 * Sets the expire time stamp value, if still valid
	 *
	 * @param expireTimestamp expire unix timestamp value in milliseconds
	 **/
	abstract public void setExpiry(long expireTimestamp);
	
	/**
	 * Sets the expire time stamp value, if still valid
	 *
	 * @param lifespan time to expire in milliseconds
	 **/
	abstract public void setLifeSpan(long lifespan);
	
	/**
	 * Stores (and overwrites if needed) key, value pair
	 * with lifespan value.
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param value as Long
	 * @param lifespan time to expire in milliseconds
	 *
	 * @return null
	 **/
	abstract public long putWithLifespan(Long value, long lifespan);
	
	/**
	 * Stores (and overwrites if needed) key, value pair
	 * with expiry value.
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param value as String
	 * @param expireTimestamp expire unix timestamp value in milliseconds
	 *
	 * @return Long
	 **/
	abstract public long putWithExpiry(Long value, long expireTimestamp);
	
	/**
	 * Increment the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	abstract public long incrementAndGet(Object key);
	
	/**
	 * Return the current value of the key and increment by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	abstract public long getAndIncrement(Object key);
	
	/**
	 * Decrement the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	abstract public long decrementAndGet(Object key);
	
	/**
	 * Return the current value of the key and decrement by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	abstract public long getAndDecrement(Object key);
	
	// /**
	//  * Return the current value of the key
	//  *
	//  * @return Long
	//  */
	abstract public long getValue();
	
}
