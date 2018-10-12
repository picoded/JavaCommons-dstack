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
 * Built ontop of the Core_DataObjectMap_struct implementation.
 **/
public class StructSimple_DataObjectMap extends Core_DataObjectMap_struct {
	
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
	 * @return Storage map used centrally for all operations
	 */
	protected Map<String, Map<String, Object>> backendMap() {
		return valueMap;
	}
	
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
			super.clear();
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
	 * @param  ObjectID to remove
	 *
	 * @return  nothing
	 **/
	public void DataObjectRemoteDataMap_remove(String oid) {
		try {
			accessLock.writeLock().lock();
			super.DataObjectRemoteDataMap_remove(oid);
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * 
	 * @param  ObjectID to get
	 * 
	 * @return null if not exists
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String oid) {
		try {
			accessLock.readLock().lock();
			return super.DataObjectRemoteDataMap_get(oid);
		} finally {
			accessLock.readLock().unlock();
		}
	}
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 **/
	public void DataObjectRemoteDataMap_update(String oid, Map<String, Object> fullMap,
		Set<String> keys) {
		try {
			accessLock.writeLock().lock();
			super.DataObjectRemoteDataMap_update(oid, fullMap, keys);
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
			return super.keySet();
		} finally {
			accessLock.readLock().unlock();
		}
	}
	
}
