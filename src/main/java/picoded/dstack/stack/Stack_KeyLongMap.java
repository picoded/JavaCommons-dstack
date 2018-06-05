package picoded.dstack.stack;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import picoded.dstack.CommonStructure;
import picoded.dstack.KeyLongMap;
import picoded.dstack.core.Core_KeyLongMap;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.MutablePair;
import picoded.core.struct.GenericConvertHashMap;

/**
 * Stacked implementation of KeyValueMap data structure.
 *
 * Built ontop of the Core_KeyLongMap implementation.
 **/
public class Stack_KeyLongMap extends Core_KeyLongMap implements Stack_CommonStructure {
	
	//--------------------------------------------------------------------------
	//
	// Constructor vars
	//
	//--------------------------------------------------------------------------
	
	// Data layers to apply basic read/write against
	protected Core_KeyLongMap[] dataLayers = null;

	// Data layer to apply query against
	protected Core_KeyLongMap queryLayer = null;

	/**
	 * Setup the data object with the respective data, and query layers
	 * 
	 * @param  inDataLayers data layers to get / set data from, 0 index first
	 * @param  inQueryLayer query layer for queries. Defaults to last data layer
	 */
	public Stack_KeyLongMap(Core_KeyLongMap[] inDataLayers, Core_KeyLongMap inQueryLayer) {
		// Ensure that stack is configured with the respective datalayers
		if( inDataLayers == null || inDataLayers.length <= 0 ) {
			throw new IllegalArgumentException("Missing valid dataLayers configuration");
		}
		// Configure the query layer, to the last data layer if not set
		if( inQueryLayer == null ) {
			inQueryLayer = inDataLayers[ inDataLayers.length - 1 ];
		}
		dataLayers = inDataLayers;
		queryLayer = inQueryLayer;
	}

	/**
	 * Setup the data object with the respective data, and query layers
	 * 
	 * @param  inDataLayers data layers to get / set data from, 0 index first; 
	 *         query layer for queries. Defaults to last data layer
	 */
	public Stack_KeyLongMap(Core_KeyLongMap[] inDataLayers) {
		this(inDataLayers, null);
	}

	//--------------------------------------------------------------------------
	//
	// Interface to ovewrite for `Stack_CommonStructure` implmentation
	//
	//--------------------------------------------------------------------------
	
	/**
	 * @return  array of the internal common structure stack used by the Stack_ implementation
	 */
	public CommonStructure[] commonStructureStack() {
		return (CommonStructure[])dataLayers;
	}

	//--------------------------------------------------------------------------
	//
	// Fundemental set/get value (core)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Returns the value and expiry, with validation against the current timestamp
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param now timestamp
	 *
	 * @return String value
	 **/
	public MutablePair<Long,Long> getValueExpiryRaw(String key, long now) {
		for(int i = 0; i < dataLayers.length; ++i) {
			MutablePair<Long,Long>  res = dataLayers[i].getValueExpiryRaw(key, now);
			if(res != null) {
				for(i = i-1; i>=0; --i) {
					dataLayers[i].setValueRaw(key, res.getLeft(), res.getRight().longValue());
				}
				return res;
			}
		}
		return null;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the value, with validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key
	 * @param value, null means removal
	 * @param expire timestamp, 0 means not timestamp
	 *
	 * @return null
	 **/
	public Long setValueRaw(String key, Long value, long expire) {
		// Write data from the lowest layer upwards
		for(int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].setValueRaw(key, value, expire);
		}
		return null;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the expire time stamp value, raw without validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param expire timestamp in seconds, 0 means NO expire
	 *
	 * @return long
	 **/
	public void setExpiryRaw(String key, long time) {
		// Write data from the lowest layer upwards
		for(int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].setExpiryRaw(key, time);
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// KeySet support implementation
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Search using the value, all the relevent key mappings
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key, note that null matches ALL
	 *
	 * @return array of keys
	 **/
	@Override
	public Set<String> keySet(Long value) {
		return queryLayer.keySet(value);
	}
	
	//--------------------------------------------------------------------------
	//
	// Atomic operation implementation
	//
	//--------------------------------------------------------------------------

	protected void assertAtomicImplementation(){
		if(dataLayers.length > 1) {
			throw new RuntimeException("Atomic operations are not supported for multi layered data structure");
		}
	}

	/**
	 * Returns the value, given the key
	 *
	 * @param key param find the meta key
	 * @param delta value to add
	 *
	 * @return  value of the given key after adding
	 **/
	public Long addAndGet(Object key, Object delta) {
		assertAtomicImplementation();
		return queryLayer.addAndGet(key, delta);
	}

	/**
	 * Returns the value, given the key. Then apply the delta change
	 *
	 * @param key param find the meta key
	 * @param delta value to add
	 *
	 * @return  value of the given key, note that it returns 0 if there wasnt a previous value set
	 **/
	public Long getAndAdd(Object key, Object delta) {
		assertAtomicImplementation();
		return queryLayer.getAndAdd(key, delta);
	}

	/**
	 * Increment the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public Long incrementAndGet(Object key) { 
		assertAtomicImplementation();
		return queryLayer.incrementAndGet(key);
	}

	/**
	 * Return the current value of the key and increment by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public Long getAndIncrement(Object key) { 
		assertAtomicImplementation();
		return queryLayer.getAndIncrement(key);
	}

	/**
	 * Decrement the value of the key and return the updated value.
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public Long decrementAndGet(Object key) { 
		assertAtomicImplementation();
		return queryLayer.decrementAndGet(key);
	}

	/**
	 * Return the current value of the key and decrement by 1
	 *
	 * @param key to retrieve
	 * @return Long
	 */
	public Long getAndDecrement(Object key) { 
		assertAtomicImplementation();
		return queryLayer.getAndDecrement(key);
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
	@Override
	public boolean weakCompareAndSet(String key, Long expect, Long update) {
		assertAtomicImplementation();
		return queryLayer.weakCompareAndSet(key, expect, update);
	}
	
	//--------------------------------------------------------------------------
	//
	// Copy pasta code, I wished could have worked in an interface
	//
	//--------------------------------------------------------------------------

	/**
	 * Removes all data, without tearing down setup
	 * 
	 * Sadly, due to a how Map interface prevents "default" implementation
	 * of clear from being valid, this seems to be a needed copy-pasta code
	 **/
	public void clear() {
		for(CommonStructure layer : commonStructureStack()) {
			layer.clear();
		}
	}
}