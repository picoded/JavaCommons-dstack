package picoded.dstack;

// Java imports
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Collections;

// External libraries
import org.apache.commons.lang3.RandomUtils;

// Picoded imports
import picoded.core.struct.template.UnsupportedDefaultMap;
import picoded.core.struct.query.Query;
import picoded.core.struct.query.utils.CollectionQueryForIDInterface;
import picoded.dstack.core.Core_DataObject;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.ProxyGenericConvertMap;

/**
 * DataObjectMap, serves as the core flexible backend storage implmentation for the whole
 * DStack setup. Its role can be viewed similarly to NoSql, or AWS SimpleDB
 * where almost everything is indexed and cached.
 *
 * On a performance basis, it is meant to "trade off" raw query performance of traditional optimized
 * SQL lookup, over flexibility in data model. 
 * 
 * Read / query performance can however be heavily mintigated by the inclusion
 * of a JCache layer for non-complex lookup cached reads. Which will in most cases be the main
 * read request load.
 * 
 * The following are the key considerations
 * 
 * + Query by SQL style queries
 * + Storage of arbitary data values
 * + Reasonable level of performance trade off
 * 
 * The following are things to note
 * 
 * + Internally it flatten nested objects as individual key value pairs
 * + Lists are treated as arrays internally
 * 
 * Gotcha's
 * 
 * + You must call the put command on changed values, for changes to be detected.
 * + While it is possible to store large binary data (its supported), it should be heavily avoided
 *   as objects are cached and used in its complete form. Resulting to large overheads for raw binary data.
 * + NULL values are treated as "deleted" values
 * 
 **/
public interface DataObjectMap extends UnsupportedDefaultMap<String, DataObject>, CommonStructure,
	CollectionQueryForIDInterface<String, DataObject> {
	
	// DataObject optimizations
	//----------------------------------------------
	
	/**
	 * Does a simple, get and check for null (taking advantage of the null is delete feature)
	 *
	 * Default implementation used keySet, which has extremly high performance
	 * penalties in deployments with large scale deployment
	 **/
	@Override
	default boolean containsKey(Object key) {
		return get(key) != null;
	}
	
	//
	// DataObject operations
	//--------------------------------------------------------------------------
	
	/**
	 * Generates a new blank object, with a GUID.
	 * Note that save does not trigger, unless its called.
	 *
	 * @return the DataObject
	 **/
	DataObject newEntry();
	
	/**
	 * Generates a new blank object, with a GUID.
	 * And append all the relevent data to it.
	 *
	 * Note that this does trigger a save all
	 *
	 * @param  data to save
	 *
	 * @return the DataObject
	 **/
	default DataObject newEntry(Map<String, Object> data) {
		DataObject ret = newEntry();
		if (data != null) {
			ret.putAll(data);
			ret.saveAll();
		}
		return ret;
	}
	
	/**
	 * Generates a new blank object, with a GUID.
	 * And append all the relevent data to it.
	 *
	 * Note that this does trigger a save all
	 *
	 * @param  data to save
	 *
	 * @return the DataObject
	 **/
	default <T extends ProxyGenericConvertMap> T newEntryWrap(Class<T> classObj,
		Map<String, Object> data) {
		return ProxyGenericConvertMap.ensure(classObj, newEntry(data));
	}
	
	/**
	 * Get a DataObject, and returns it.
	 *
	 * Existance checks is performed for such requests
	 *
	 * @param  object GUID to fetch
	 *
	 * @return the DataObject, null if not exists
	 **/
	DataObject get(Object oid);
	
	/**
	 * Get a DataObject, and returns it. Skips existance checks if required
	 *
	 * @param  object GUID to fetch
	 * @param  boolean used to indicate if an existance check is done for the request
	 *
	 * @return the DataObject
	 **/
	DataObject get(String oid, boolean isUnchecked);
	
	/**
	 * Get a DataObject, and returns it. Skips existance checks if required
	 * Wrapped in an ProxyGenericConvertMap compatible class
	 *
	 * @param  classObj for passing over class type
	 * @param  object GUID to fetch
	 * @param  boolean used to indicate if an existance check is done for the request
	 *
	 * @return  The ProxyGenericConvertMap[] array
	 **/
	default <T extends ProxyGenericConvertMap> T getWrap(Class<T> classObj, String oid,
		boolean isUnchecked) {
		return ProxyGenericConvertMap.ensure(classObj, get(oid, isUnchecked));
	}
	
	/**
	 * Removes a DataObject if it exists, from the DB
	 *
	 * @param  object GUID to fetch
	 *
	 * @return NULL
	 **/
	DataObject remove(Object key);
	
	// DataObject utility operations
	//--------------------------------------------------------------------------
	
	/**
	 * Get array of DataObjects
	 **/
	default DataObject[] getArrayFromID(String[] idArray, boolean isUnchecked) {
		DataObject[] retArr = new DataObject[idArray.length];
		for (int i = 0; i < idArray.length; ++i) {
			retArr[i] = get(idArray[i], isUnchecked);
		}
		return retArr;
	}
	
	// Query and aggregation operations (to optimize on specific implementation)
	// NOTE: Interface is inherited from CollectionQueryForIDInterface
	//--------------------------------------------------------------------------
	
	/**
	 * Performs a search query, and returns the respective DataObjects
	 *
	 * @param   where query statement
	 * @param   where clause values array
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max, use -1 to ignore
	 *
	 * @return  The DataObject[] array
	 **/
	default DataObject[] query(String whereClause, Object[] whereValues, String orderByStr,
		int offset, int limit) {
		return getArrayFromID(query_id(whereClause, whereValues, orderByStr, offset, limit), true);
	}
	
	/**
	 * Performs a search query, and returns the respective DataObject keys.
	 *
	 * This is the GUID key varient of query, this is critical for stack lookup
	 *
	 * @param   where query statement
	 * @param   where clause values array
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array
	 **/
	String[] query_id(String whereClause, Object[] whereValues, String orderByStr, int offset,
		int limit);
	
	/**
	 * Performs a search query, and returns the respective DataObjects
	 *
	 * @param   where query statement
	 * @param   where clause values array
	 *
	 * @return  The total count for the query
	 **/
	default long queryCount(String whereClause, Object[] whereValues) {
		// Query and count
		return query_id(whereClause, whereValues, null).length;
	}
	
	// Get from key names operations (to optimize on specific implementation)
	//
	// @TODO : REMOVAL
	//--------------------------------------------------------------------------
	
	/**
	 * Performs a custom search by configured keyname
	 *
	 * @param   keyName to lookup for
	 *
	 * @return  The DataObject[] array
	 **/
	default DataObject[] getFromKeyName(String keyName) {
		return getFromKeyName(keyName, null, -1, -1);
	}
	
	/**
	 * Performs a custom search by configured keyname
	 *
	 * @param   keyName to lookup for
	 * @param   query string to sort the order by, use null to ignore
	 *
	 * @return  The DataObject[] array
	 **/
	default DataObject[] getFromKeyName(String keyName, String orderByStr) {
		return getFromKeyName(keyName, orderByStr, -1, -1);
	}
	
	/**
	 * Performs a custom search by configured keyname
	 *
	 * @param   keyName to lookup for
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max
	 *
	 * @return  The DataObject[] array
	 **/
	default DataObject[] getFromKeyName(String keyName, String orderByStr, int offset, int limit) {
		return getArrayFromID(getFromKeyName_id(keyName, orderByStr, offset, limit), true);
	}
	
	/**
	 * Performs a custom search by configured keyname, and returns its ID array
	 *
	 * @param   keyName to lookup for
	 *
	 * @return  The DataObject[] array
	 **/
	default String[] getFromKeyName_id(String keyName) {
		return getFromKeyName_id(keyName, null, -1, -1);
	}
	
	/**
	 * Performs a custom search by configured keyname, and returns its ID array
	 *
	 * @param   keyName to lookup for
	 * @param   query string to sort the order by, use null to ignore
	 *
	 * @return  The DataObject[] array
	 **/
	default String[] getFromKeyName_id(String keyName, String orderByStr) {
		return getFromKeyName_id(keyName, orderByStr, -1, -1);
	}
	
	/**
	 * Performs a custom search by configured keyname, and returns its ID array
	 *
	 * @param   keyName to lookup for
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max, use -1 to ignore
	 *
	 * @return  The DataObject[] array
	 **/
	default String[] getFromKeyName_id(String keyName, String orderByStr, int offset, int limit) {
		// The return list
		List<String> retList = new ArrayList<String>();
		
		// Iterate the list, add if containsKey
		for (DataObject obj : values()) {
			if (obj.containsKey(keyName)) {
				retList.add(obj._oid());
			}
		}
		
		// Return
		return retList.toArray(new String[retList.size()]);
	}
	
	//
	// Query count optimization handling
	//--------------------------------------------------------------------------
	
	/**
	 * Get the size of the current DataObjectMap
	 * 
	 * Note that if the dataset size is larger then Integer.MAX_VALUE
	 * it is clamped accordingly to the MAX_VALUE
	 **/
	@Override
	default int size() {
		long s = queryCount(null, null);
		if (s > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		return (int) s;
	}
	
	/**
	 * Get the size as a long for the DataObjectMap
	 * This should be used instead of size (to work around int limits)
	 */
	default long longSize() {
		return queryCount(null, null);
	}
	
	//
	// Get key names handling
	//--------------------------------------------------------------------------
	
	/**
	 * Scans the object and get the various keynames used.
	 * This is used mainly in adminstration interface, etc.
	 *
	 * Note however, that in non-JSql mode, this function is not
	 * optimized, and does an iterative search for the various object keys,
	 * which is ridiculously expensive, so avoid calling this unless needed.
	 *
	 * The seekDepth parameter is ignored in JSql mode, as its optimized.
	 *
	 * @param  seekDepth, which detirmines the upper limit for iterating
	 *         objects for the key names, use -1 to search all
	 *
	 * @return  The various key names used in the objects
	 **/
	default Set<String> getKeyNames(int seekDepth) {
		Set<String> res = new HashSet<String>();
		
		// Iterate the list, get key names
		DataObject obj = randomObject();
		res.addAll(obj.keySet());
		
		// Lets iterate through
		for (int i = 1; i < seekDepth; ++i) {
			obj = looselyIterateObject(obj);
			if (obj != null) {
				res.addAll(obj.keySet());
			}
		}
		
		// Return the result set
		return res;
	}
	
	/**
	 * getKeyNames varient with seekDepth defaulted to 25
	 *
	 * @return  The various key names used in the objects
	 **/
	default Set<String> getKeyNames() {
		return getKeyNames(25);
	}
	
	// Resolving class inheritence conflict
	//--------------------------------------------------------------------------
	
	/**
	 * Removes all data, without tearing down setup
	 *
	 * This is equivalent of "TRUNCATE TABLE {TABLENAME}"
	 **/
	void clear();
	
	// Special iteration support
	//--------------------------------------------------------------------------
	
	/**
	 * Gets and return a random object ID
	 *
	 * @return  Random object ID
	 **/
	default String randomObjectID() {
		// Gets list of possible ID's
		Set<String> idSet = keySet();
		
		// Randomly pick and ID, and fetch the object
		int size = idSet.size();
		if (size > 0) {
			int chosen = ThreadLocalRandom.current().nextInt(size);
			int idx = 0;
			for (String idString : idSet) {
				if (idx >= chosen) {
					return idString;
				}
				idx++;
			}
		}
		
		// Possibly a blank set here, return null
		return null;
	}
	
	/**
	 * Gets and returns a random object,
	 * Useful for random validation / checks
	 *
	 * @return  Random DataObject
	 **/
	default DataObject randomObject() {
		String oid = randomObjectID();
		if (oid != null) {
			return get(oid);
		}
		return null;
	}
	
	/**
	 * Gets and return the next object ID key for iteration given the current ID,
	 * null gets the first object in iteration.
	 *
	 * It is important to note actual iteration sequence is implementation dependent.
	 * And does not gurantee that newly added objects, after the iteration started,
	 * will be part of the chain of results.
	 *
	 * Similarly if the currentID was removed midway during iteration, the return
	 * result is not properly defined, and can either be null, or the closest object matched
	 * or even a random object.
	 *
	 * It is however guranteed, if no changes / writes occurs. A complete iteration
	 * will iterate all existing objects.
	 *
	 * The larger intention of this function, is to allow a background thread to slowly
	 * iterate across all objects, eventually. With an acceptable margin of loss on,
	 * recently created/edited object. As these objects will eventually be iterated in
	 * repeated rounds on subsequent calls.
	 *
	 * Due to its roughly random nature in production (with concurrent objects generated)
	 * and its iterative nature as an eventuality. The phrase looselyIterate was chosen,
	 * to properly reflect its nature.
	 *
	 * Another way to phrase it, in worse case scenerio, its completely random, 
	 * eventually iterating all objects. In best case scenerio, it does proper 
	 * iteration as per normal.
	 *
	 * @param   Current object ID, can be NULL
	 *
	 * @return  Next object ID, if found
	 **/
	default String looselyIterateObjectID(String currentID) {
		// By default this is an inefficent implementation
		// of sorting the keyset, and returning in the respective order
		ArrayList<String> idList = new ArrayList<String>(keySet());
		Collections.sort(idList);
		int size = idList.size();
		
		// Blank set, nothing to iterate
		if (size == 0) {
			return null;
		}
		
		// But it works
		if (currentID == null) {
			// return first object
			return idList.get(0);
		}
		
		// Time to iterate it all
		for (int idx = 0; idx < size; ++idx) {
			if (currentID.equals(idList.get(idx))) {
				// Current position found
				if (idx >= (size - 1)) {
					// This is last object, return null (end)
					return null;
				}
				// Else get the next object
				return idList.get(idx + 1);
			}
			// If position not found, continue iterating
		}
		
		// If this position is reached,
		// possibly object was deleted mid iteration
		//
		// Fallsback to a random object to iterate
		return randomObjectID();
	}
	
	/**
	 * DataObject varient of randomlyIterateObjectID
	 *
	 * @param   DataObject to iterate next from, can be null
	 *
	 * @return  Random DataObject
	 **/
	default DataObject looselyIterateObject(DataObject currentObj) {
		String currentID = (currentObj != null) ? currentObj._oid() : null;
		String retID = looselyIterateObjectID(currentID);
		return (retID != null) ? get(retID) : null;
	}
	
}
