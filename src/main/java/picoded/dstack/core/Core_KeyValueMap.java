package picoded.dstack.core;

import java.util.HashSet;
import java.util.Set;

import picoded.dstack.KeyValue;
import picoded.dstack.KeyValueMap;
import picoded.core.security.NxtCrypt;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.GenericConvertHashMap;

/**
 * Common base utility class of KeyValueMap.
 *
 * Does not actually implement its required feature,
 * but helps provide a common base line for all the various implementation.
 **/
public abstract class Core_KeyValueMap extends Core_DataStructure<String, KeyValue> implements
	KeyValueMap {
	
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
	public KeyValue getKeyValue(Object key) {
		if( key == null ) {
			throw new RuntimeException("key parameter cannot be NULL");
		}
		return new Core_KeyValue(this, key.toString());
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
	 * Returns the value, with validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param now timestamp, 0 = no timestamp so skip timestamp checks
	 *
	 * @return String value
	 **/
	abstract protected String getValueRaw(String key, long now);
	
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
	abstract protected String setValueRaw(String key, String value, long expire);
	
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
	abstract public void setExpiryRaw(String key, long time);
	
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
	public void setExpiry(String key, long time) {
		setExpiryRaw(key, time);
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
	public String putWithLifespan(String key, String value, long lifespan) {
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
	 * @return String
	 **/
	@Override
	public String putWithExpiry(String key, String value, long expireTime) {
		return setValueRaw(key, value, expireTime);
	}
	
}
