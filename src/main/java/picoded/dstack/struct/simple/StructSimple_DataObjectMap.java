package picoded.dstack.struct.simple;

// Java imports
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Picoded imports
import picoded.core.conv.ConvertJSON;
import picoded.core.common.ObjectToken;
import picoded.dstack.*;
import picoded.dstack.core.*;

/**
 * Reference implementation of DataObjectMap data structure.
 * This is done via a minimal implementation via internal data structures.
 *
 * Built ontop of the Core_DataObjectMap implementation.
 **/
public class StructSimple_DataObjectMap extends Core_DataObjectMap {
	
	//--------------------------------------------------------------------------
	//
	// Constructor vars
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Stores the key to value map
	 **/
	protected Map<String, Map<String, Object>> valueMap = new ConcurrentHashMap<String, Map<String, Object>>();
	
	/**
	 * Read write lock
	 **/
	protected ReentrantReadWriteLock accessLock = new ReentrantReadWriteLock();
	
	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown / maintenance (DStackCommon)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Setsup the backend storage table, etc. If needed
	 **/
	@Override
	public void systemSetup() {
		// does nothing
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	@Override
	public void systemDestroy() {
		clear();
	}
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		try {
			accessLock.writeLock().lock();
			valueMap.clear();
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// Internal functions, used by DataObject
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the complete remote data map, for DataObject.
	 * This is used to nuke an entire object
	 *
	 * @param  Object ID to remove
	 *
	 * @return  nothing
	 **/
	protected void DataObjectRemoteDataMap_remove(String oid) {
		try {
			accessLock.writeLock().lock();
			valueMap.remove(oid);
		} finally {
			accessLock.writeLock().unlock();
		}
		
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * Returns null if not exists
	 **/
	protected Map<String, Object> DataObjectRemoteDataMap_get(String oid) {
		try {
			accessLock.readLock().lock();
			Map<String, Object> storedValue = valueMap.get(oid);
			if (storedValue == null) {
				return null;
			}
			Map<String, Object> ret = new HashMap<String, Object>();
			for (Entry<String, Object> entry : storedValue.entrySet()) {
				ret.put(entry.getKey(), detachValue(storedValue.get(entry.getKey())));
			}
			return ret;
		} finally {
			accessLock.readLock().unlock();
		}
	}
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 **/
	protected void DataObjectRemoteDataMap_update(String oid, Map<String, Object> fullMap,
		Set<String> keys) {
		try {
			accessLock.writeLock().lock();
			
			// Get keys to store, null = all
			if (keys == null) {
				keys = fullMap.keySet();
			}
			
			// Makes a new map if needed
			Map<String, Object> storedValue = valueMap.get(oid);
			if (storedValue == null) {
				storedValue = new ConcurrentHashMap<String, Object>();
			}
			
			// Get and store the required values
			for (String key : keys) {
				Object val = fullMap.get(key);
				if (val == null) {
					storedValue.remove(key);
				} else {
					storedValue.put(key, val);
				}
			}
			
			// Ensure the value map is stored
			valueMap.put(oid, storedValue);
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// KeySet support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Get and returns all the GUID's, note that due to its
	 * potential of returning a large data set, production use
	 * should be avoided.
	 *
	 * @return set of keys
	 **/
	@Override
	public Set<String> keySet() {
		try {
			accessLock.readLock().lock();
			return valueMap.keySet();
		} finally {
			accessLock.readLock().unlock();
		}
	}
	
}
