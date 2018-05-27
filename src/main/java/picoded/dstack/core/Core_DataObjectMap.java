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
 * Common base utility class of DataObjectMap
 *
 * Does not actually implement its required feature,
 * but helps provide a common base line for all the various implementation.
 **/
abstract public class Core_DataObjectMap extends Core_DataStructure<String, DataObject> implements DataObjectMap {
	
	//--------------------------------------------------------------------------
	//
	// Generic Utility functions
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Ensures the returned value is not refrencing the input value, cloning if needed
	 * 
	 * This is understandably currently a CPU / ram inefficent,solution,
	 * for Map / List implementation.
	 * 
	 * A possible solution in the future is to create and return a Map / List
	 * proxy, which only "saves" the delta changes or performs a clone of its
	 * internal structure on a put request.
	 * 
	 * As such no "cloning" would occur on a normal basis
	 *
	 * @return  The cloned value, with no risk of modifying the original.
	 **/
	static public Object deepCopy(Object in) {
		return NestedObjectUtil.deepCopy(in);
	}
	
	//--------------------------------------------------------------------------
	//
	// Query Utility functions
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Utility funciton, used to sort and limit the result of a query
	 *
	 * @param   list of DataObject to sort and return
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max
	 *
	 * @return  The DataObject list to return
	 **/
	public static List<DataObject> sortAndOffsetList(List<DataObject> retList, String orderByStr,
		int offset, int limit) {
		
		// Sorting the order, if needed
		if (orderByStr != null && (orderByStr = orderByStr.trim()).length() > 0) {
			// Creates the order by sorting, with _oid
			OrderBy<DataObject> sorter = new OrderBy<DataObject>(orderByStr + " , _oid");
			
			// Sort it
			Collections.sort(retList, sorter);
		}
		
		// Get sublist if needed
		if (offset >= 1 || limit >= 1) {
			int size = retList.size();
			
			// Out of bound, return blank
			if (offset >= size) {
				return new ArrayList<DataObject>();
			}
			
			// Ensures the upper end does not go out of bound
			int end = size;
			if (limit > -1) {
				end = offset + limit;
			}
			if (end > size) {
				end = size;
			}
			
			// // Out of range
			// if (end <= offset) {
			// 	return new DataObject[0];
			// }
			
			// Get sublist
			retList = retList.subList(offset, end);
		}
		
		// Returns the list, you can easily convert to an array via "toArray(new DataObject[0])"
		return retList;
	}
	
	//--------------------------------------------------------------------------
	//
	// DataObject removal
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Removes a DataObject if it exists, from the DB
	 *
	 * @param  object GUID to fetch, OR the DataObject itself
	 *
	 * @return NULL
	 **/
	public DataObject remove(Object key) {
		if (key instanceof DataObject) {
			// Removal via DataObject itself
			DataObjectRemoteDataMap_remove(((DataObject) key)._oid());
		} else {
			// Remove using the ID
			DataObjectRemoteDataMap_remove(key.toString());
		}
		return null;
	}

	//--------------------------------------------------------------------------
	//
	// Functions, used by DataObject
	// [Internal use, to be extended in future implementation]
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the complete remote data map, for DataObject.
	 * This is used to nuke an entire object
	 *
	 * @param Object ID to remove
	 *
	 * @return nothing
	 **/
	abstract protected void DataObjectRemoteDataMap_remove(String oid);
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Gets the complete remote data map, for DataObject.
	 * This is used to get the raw map data from the backend.
	 *
	 * @param  Object ID to get
	 *
	 * @return  The raw Map object to build the DataObject, null if does not exists
	 **/
	abstract protected Map<String, Object> DataObjectRemoteDataMap_get(String oid);
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 *
	 * @param   Object ID to get
	 * @param   The full map of data. This is required as not all backend implementations allow partial update
	 * @param   Keys to update, this is used to optimize certain backends
	 **/
	abstract protected void DataObjectRemoteDataMap_update(String oid, Map<String, Object> fullMap,
		Set<String> keys);
	
	//--------------------------------------------------------------------------
	//
	// Query functions
	// [Should really be overwritten, as this does the inefficent lazy way]
	//
	//--------------------------------------------------------------------------
	
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
	public String[] query_id(String whereClause, Object[] whereValues, String orderByStr,
		int offset, int limit) {
		
		// The return list of DataObjects
		List<DataObject> retList = null;
		
		// Setup the query, if needed
		if (whereClause == null) {
			// Null gets all
			retList = new ArrayList<DataObject>(this.values());
		} else {
			// Performs a search query
			Query queryObj = Query.build(whereClause, whereValues);
			retList = queryObj.search(this);
		}
		
		// Sort, offset, convert to array, and return
		retList = sortAndOffsetList(retList, orderByStr, offset, limit);
		
		// Prepare the actual return string array
		int retLength = retList.size();
		String[] ret = new String[retLength];
		for (int a = 0; a < retLength; ++a) {
			ret[a] = retList.get(a)._oid();
		}
		
		// Returns
		return ret;
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
	public DataObject[] getFromKeyName(String keyName, String orderByStr, int offset, int limit) {
		
		// The return list
		List<DataObject> retList = new ArrayList<DataObject>();
		
		// Iterate the list, add if containsKey
		for (DataObject obj : values()) {
			if (obj.containsKey(keyName)) {
				retList.add(obj);
			}
		}
		
		// Sort, offset, convert to array, and return
		return sortAndOffsetList(retList, orderByStr, offset, limit).toArray(new DataObject[0]);
	}
	
	//--------------------------------------------------------------------------
	//
	// DataObject operations
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Generates a new blank object, with a GUID
	 *
	 * @return the DataObject
	 **/
	public DataObject newEntry() {
		// Generating a new object
		DataObject ret = new Core_DataObject(this, null, null, false);
		
		// Actual return
		return ret;
	}
	
	/**
	 * Get a DataObject, and returns it. Skips existance checks if required
	 *
	 * @param  object GUID to fetch
	 * @param  boolean used to indicate if an existance check is done for the request
	 *
	 * @return the DataObject
	 **/
	public DataObject get(String oid, boolean isUnchecked) {
		if (isUnchecked) {
			return new Core_DataObject(this, oid, null, false);
		} else {
			return get(oid);
		}
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
	public DataObject get(Object oid) {
		// String oid
		String soid = (oid != null) ? oid.toString() : null;
		
		// Return null, if OID is null
		if (soid == null || soid.isEmpty()) {
			return null;
		}
		
		// Get remote data map
		Map<String, Object> fullRemote = DataObjectRemoteDataMap_get(soid);
		
		// Return null, if there is no data
		if (fullRemote == null) {
			return null;
		}
		
		// Return a DataObject
		return new Core_DataObject(this, soid, fullRemote, true);
	}
	
	//--------------------------------------------------------------------------
	//
	// Constructor and maintenance
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Maintenance step call, however due to the nature of most implementation not
	 * having any form of time "expirary", this call does nothing in most implementation.
	 *
	 * As such im making that the default =)
	 **/
	@Override
	public void maintenance() {
		// Does nothing
	}
}
