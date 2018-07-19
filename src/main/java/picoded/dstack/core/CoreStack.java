package picoded.dstack.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import picoded.core.common.SystemSetupInterface;
import picoded.core.struct.GenericConvertMap;
import picoded.dstack.*;

/**
 * [Internal use only]
 * 
 * Common configuration based stack provider
 * 
 * @return initialized data structure if type is supported
 **/
public abstract class CoreStack implements SystemSetupInterface {
	
	//-------------------------------------------------------------
	//
	//  Stack constructor and config
	//
	//-------------------------------------------------------------
	
	/**
	 * Configuration map of the stack
	 */
	public GenericConvertMap<String, Object> config = null;
	
	/**
	 * Constructor with configuration map
	 */
	public CoreStack(GenericConvertMap<String, Object> inConfig) {
		config = inConfig;
	}
	
	//-------------------------------------------------------------
	//
	//  Data structure caching
	//
	//-------------------------------------------------------------
	
	/**
	 * Cache of all initialized data structure, for use by maintainance calls
	 */
	public Map<String, Core_DataStructure> structureCache = new HashMap<>();
	
	/**
	 * @return keyValueMap of the given name, null if stack provider does not support the given object
	 */
	public Core_KeyValueMap keyValueMap(String name) {
		return (Core_KeyValueMap) cacheDataStructure(name, "KeyValueMap", Core_KeyValueMap.class);
	}
	
	/**
	 * @return keyLongMap of the given name, null if stack provider does not support the given object
	 */
	public Core_KeyLongMap keyLongMap(String name) {
		return (Core_KeyLongMap) cacheDataStructure(name, "KeyLongMap", Core_KeyLongMap.class);
	}
	
	/**
	 * @return dataObjectMap of the given name, null if stack provider does not support the given object
	 */
	public Core_DataObjectMap dataObjectMap(String name) {
		return (Core_DataObjectMap) cacheDataStructure(name, "DataObjectMap",
			Core_DataObjectMap.class);
	}
	
	/**
	 * @return fileWorkspaceMap of the given name, null if stack provider does not support the given object
	 */
	public Core_FileWorkspaceMap fileWorkspaceMap(String name) {
		return (Core_FileWorkspaceMap) cacheDataStructure(name, "FileWorkspaceMap",
			Core_FileWorkspaceMap.class);
	}
	
	/**
	 * Load and validate from the cache a requested data structure, or initialize it and cache it
	 * 
	 * @param  name  name of the datastructure to initialize
	 * @param  type  implmentation type (KeyValueMap / KeyLongMap / DataObjectMap / FileWorkspaceMap)
	 * @param  cObj  class type to validate for (optional)
	 * 
	 * @return  the cached data structure
	 */
	public Core_DataStructure cacheDataStructure(String name, String type, Class cObj) {
		// Get and validate cache if found
		Core_DataStructure cache = structureCache.get(name);
		if (cache != null) {
			// Class object to validate for (if validation class is provided)
			if (cObj == null || cObj.isInstance(cache)) {
				return cache;
			}
			// Validation failed, thros an exception
			throw new RuntimeException("Invalid data structure type found in cache for: " + name
				+ " / " + cObj.getSimpleName());
		}
		
		// Initialize the datastructure
		cache = initDataStructure(name, type);
		
		// Cache the structure if initialized
		if (cache != null) {
			structureCache.put(name, cache);
			return cache;
		}
		
		// Return null
		return null;
	}
	
	//-------------------------------------------------------------
	//
	//  Data structure implmentation provider
	//
	//-------------------------------------------------------------
	
	/**
	 * Initilize and return the requested data structure with the given name or type if its supported
	 * 
	 * @param  name  name of the datastructure to initialize
	 * @param  type  implmentation type (KeyValueMap / KeyLongMap / DataObjectMap / FileWorkspaceMap)
	 */
	protected abstract Core_DataStructure initDataStructure(String name, String type);

	//-------------------------------------------------------------
	//
	//  System Setup interface implementation
	//
	//-------------------------------------------------------------
	
	/**
	 * Calls systemSetup on each of the cached data structure
	 */
	public void systemSetup() {
		structureCache.forEach((k,v)->{
			v.systemSetup();
		});
	}

	/**
	 * Calls systemDestroy on each of the cached data structure
	 */
	public void systemDestroy() {
		structureCache.forEach((k,v)->{
			v.systemDestroy();
		});
	}

	/**
	 * Calls maintenance on each of the cached data structure
	 */
	public void maintenance() {
		structureCache.forEach((k,v)->{
			v.maintenance();
		});
	}

	/**
	 * Calls incrementalMaintenance on each of the cached data structure
	 */
	public void incrementalMaintenance() {
		structureCache.forEach((k,v)->{
			v.incrementalMaintenance();
		});
	}

	/**
	 * Calls clear on each of the cached data structure
	 */
	public void clear() {
		structureCache.forEach((k,v)->{
			v.clear();
		});
	}

	/**
	 * Calls close on each of the cached data structure
	 */
	public void close() {
		structureCache.forEach((k,v)->{
			v.close();
		});
	}

}