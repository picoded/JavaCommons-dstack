package picoded.dstack.core;

// Java imports
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Picoded imports
import picoded.core.conv.GenericConvert;
import picoded.core.conv.NestedObjectUtil;
import picoded.core.common.ObjectToken;
import picoded.core.struct.query.*;
import picoded.dstack.*;

/**
 * DataObjectMap extended to assume usage of a single 
 * underlying map interface for the backend storage
 **/
abstract public class Core_DataObjectMap_struct extends Core_DataObjectMap {
	
	/**
	 * @return Storage map used for the backend operations of one "DataObjectMap"
	 */
	protected abstract Map<String, Map<String, Object>> backendMap();
	
	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown / maintenance (DStackCommon)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		backendMap().clear();
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
	public void DataObjectRemoteDataMap_remove(String oid) {
		backendMap().remove(oid);
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * 
	 * @param  ObjectID to get
	 * 
	 * @return null if not exists
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String oid) {
		Map<String, Object> storedValue = backendMap().get(oid);
		if (storedValue == null) {
			return null;
		}
		return (Map<String, Object>) deepCopy(storedValue);
	}
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 * 
	 * @param  ObjectID to update
	 * @param  fullMap of values to apply update
	 * @param  keys of parameters to update (for partial update if supported)
	 **/
	public void DataObjectRemoteDataMap_update(String oid, Map<String, Object> fullMap,
		Set<String> keys) {
		
		// Get keys to store, null = all
		if (keys == null) {
			keys = fullMap.keySet();
		}
		
		// Makes a new map if needed
		Map<String, Object> storedValue = backendMap().get(oid);
		if (storedValue == null) {
			storedValue = new HashMap<String, Object>();
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
		backendMap().put(oid, storedValue);
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
		return backendMap().keySet();
	}
	
}