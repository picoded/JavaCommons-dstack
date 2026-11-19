package picoded.dstack;

// Java imports
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

	/**
	 * Maximum allowed items in a queryIn / queryIn_id value collection.
	 **/
	public static final int MAX_QUERY_IN_SIZE = 100;
	
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
	 * Generates a new blank object, with a generated GUID.
	 * Note that save does not trigger, unless its called.
	 *
	 * @return the DataObject
	 **/
	DataObject newEntry();

	/**
	 * Generates a new blank object, with a given GUID.
	 * Note that save does not trigger, unless its called.
	 * 
	 * @param oid the object ID to use for the new entry, if null a GUID will be generated
	 * @return the DataObject
	 */
	DataObject newEntry(String oid);
	
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
	 * Maximum allowed batch size for a single removeBulk operation.
	 **/
	public static final int MAX_BULK_REMOVE_SIZE = 500;

	/**
	 * Removes a DataObject if it exists, from the DB
	 *
	 * @param  object GUID to fetch
	 *
	 * @return NULL
	 **/
	DataObject remove(Object key);

	/**
	 * Removes multiple DataObjects by their string identifiers.
	 * Throws IllegalArgumentException if the collection exceeds MAX_BULK_REMOVE_SIZE (500).
	 *
	 * @param  oids collection of object IDs to remove
	 * @return number of records successfully removed
	 **/
	default long removeBulk(Collection<String> oids) {
		if (oids == null || oids.isEmpty()) {
			return 0;
		}
		if (oids.size() > MAX_BULK_REMOVE_SIZE) {
			throw new IllegalArgumentException(
				"Cannot bulk remove more than " + MAX_BULK_REMOVE_SIZE + " items at once. Received: " + oids.size()
			);
		}
		long count = 0;
		for (String oid : oids) {
			if (oid != null) {
				remove(oid);
				count++;
			}
		}
		return count;
	}

	/**
	 * Removes multiple DataObjects by their string identifiers array.
	 * Throws IllegalArgumentException if the array exceeds MAX_BULK_REMOVE_SIZE (500).
	 *
	 * @param  oids array of object IDs to remove
	 * @return number of records successfully removed
	 **/
	default long removeBulk(String[] oids) {
		if (oids == null || oids.length == 0) {
			return 0;
		}
		if (oids.length > MAX_BULK_REMOVE_SIZE) {
			throw new IllegalArgumentException(
				"Cannot bulk remove more than " + MAX_BULK_REMOVE_SIZE + " items at once. Received: " + oids.length
			);
		}
		return removeBulk(Arrays.asList(oids));
	}
	
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
	
	//
	// Query IN operations (matching property against collection of values)
	//--------------------------------------------------------------------------

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * with optional additional WHERE clause, and returns the respective DataObjects.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   additionalWhereQuery optional additional WHERE query string (e.g. "status = ?"), or null
	 * @param   additionalWhereArgs arguments for additionalWhereQuery, or null
	 * @param   orderByStr query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   limit number of objects to return max, use -1 to ignore
	 *
	 * @return  The DataObject[] array
	 **/
	default DataObject[] queryIn(String propertyName, Collection<?> values, String additionalWhereQuery,
		Object[] additionalWhereArgs, String orderByStr, int offset, int limit) {
		return getArrayFromID(queryIn_id(propertyName, values, additionalWhereQuery, additionalWhereArgs, orderByStr, offset, limit), true);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * and returns the respective DataObjects.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   orderByStr query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   limit number of objects to return max, use -1 to ignore
	 *
	 * @return  The DataObject[] array
	 **/
	default DataObject[] queryIn(String propertyName, Collection<?> values, String orderByStr,
		int offset, int limit) {
		return queryIn(propertyName, values, null, null, orderByStr, offset, limit);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * and returns the respective DataObjects.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 *
	 * @return  The DataObject[] array
	 **/
	default DataObject[] queryIn(String propertyName, Collection<?> values) {
		return queryIn(propertyName, values, null, null, null, -1, -1);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * and returns the respective DataObjects.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   orderByStr query string to sort the order by, use null to ignore
	 *
	 * @return  The DataObject[] array
	 **/
	default DataObject[] queryIn(String propertyName, Collection<?> values, String orderByStr) {
		return queryIn(propertyName, values, null, null, orderByStr, -1, -1);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * with optional additional WHERE clause, and returns the respective DataObjects.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   additionalWhereQuery optional additional WHERE query string (e.g. "status = ?"), or null
	 * @param   additionalWhereArgs arguments for additionalWhereQuery, or null
	 *
	 * @return  The DataObject[] array
	 **/
	default DataObject[] queryIn(String propertyName, Collection<?> values, String additionalWhereQuery,
		Object[] additionalWhereArgs) {
		return queryIn(propertyName, values, additionalWhereQuery, additionalWhereArgs, null, -1, -1);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * with optional additional WHERE clause, and returns the respective DataObjects.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   additionalWhereQuery optional additional WHERE query string (e.g. "status = ?"), or null
	 * @param   additionalWhereArgs arguments for additionalWhereQuery, or null
	 * @param   orderByStr query string to sort the order by, use null to ignore
	 *
	 * @return  The DataObject[] array
	 **/
	default DataObject[] queryIn(String propertyName, Collection<?> values, String additionalWhereQuery,
		Object[] additionalWhereArgs, String orderByStr) {
		return queryIn(propertyName, values, additionalWhereQuery, additionalWhereArgs, orderByStr, -1, -1);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * with optional additional WHERE clause, and returns the respective DataObject keys.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   additionalWhereQuery optional additional WHERE query string (e.g. "status = ?"), or null
	 * @param   additionalWhereArgs arguments for additionalWhereQuery, or null
	 * @param   orderByStr query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   limit number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array of object keys
	 **/
	default String[] queryIn_id(String propertyName, Collection<?> values, String additionalWhereQuery,
		Object[] additionalWhereArgs, String orderByStr, int offset, int limit) {
		if (propertyName == null || propertyName.trim().length() == 0) {
			throw new IllegalArgumentException("propertyName cannot be null or empty");
		}
		if (values == null || values.isEmpty()) {
			return new String[0];
		}
		if (values.size() > MAX_QUERY_IN_SIZE) {
			throw new IllegalArgumentException("queryIn exceeds maximum allowed values: " + values.size() + " > " + MAX_QUERY_IN_SIZE);
		}

		StringBuilder whereClause = new StringBuilder("(");
		int idx = 0;
		for (Object val : values) {
			if (idx > 0) {
				whereClause.append(" OR ");
			}
			whereClause.append(propertyName).append(" = ?");
			idx++;
		}
		whereClause.append(")");

		boolean hasAdditional = false;
		if (additionalWhereQuery != null && additionalWhereQuery.trim().length() > 0) {
			hasAdditional = true;
		}

		int additionalArgsLen = 0;
		if (hasAdditional && additionalWhereArgs != null) {
			additionalArgsLen = additionalWhereArgs.length;
		}

		Object[] whereValues = new Object[values.size() + additionalArgsLen];
		idx = 0;
		for (Object val : values) {
			whereValues[idx] = val;
			idx++;
		}

		if (hasAdditional) {
			whereClause.append(" AND (").append(additionalWhereQuery.trim()).append(")");
			if (additionalWhereArgs != null) {
				for (int i = 0; i < additionalWhereArgs.length; ++i) {
					whereValues[idx] = additionalWhereArgs[i];
					idx++;
				}
			}
		}

		return query_id(whereClause.toString(), whereValues, orderByStr, offset, limit);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * and returns the respective DataObject keys.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   orderByStr query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   limit number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array of object keys
	 **/
	default String[] queryIn_id(String propertyName, Collection<?> values, String orderByStr,
		int offset, int limit) {
		return queryIn_id(propertyName, values, null, null, orderByStr, offset, limit);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * and returns the respective DataObject keys.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 *
	 * @return  The String[] array of object keys
	 **/
	default String[] queryIn_id(String propertyName, Collection<?> values) {
		return queryIn_id(propertyName, values, null, null, null, -1, -1);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * and returns the respective DataObject keys.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   orderByStr query string to sort the order by, use null to ignore
	 *
	 * @return  The String[] array of object keys
	 **/
	default String[] queryIn_id(String propertyName, Collection<?> values, String orderByStr) {
		return queryIn_id(propertyName, values, null, null, orderByStr, -1, -1);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * with optional additional WHERE clause, and returns the respective DataObject keys.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   additionalWhereQuery optional additional WHERE query string (e.g. "status = ?"), or null
	 * @param   additionalWhereArgs arguments for additionalWhereQuery, or null
	 *
	 * @return  The String[] array of object keys
	 **/
	default String[] queryIn_id(String propertyName, Collection<?> values, String additionalWhereQuery,
		Object[] additionalWhereArgs) {
		return queryIn_id(propertyName, values, additionalWhereQuery, additionalWhereArgs, null, -1, -1);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * with optional additional WHERE clause, and returns the respective DataObject keys.
	 *
	 * @param   propertyName property/column name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   additionalWhereQuery optional additional WHERE query string (e.g. "status = ?"), or null
	 * @param   additionalWhereArgs arguments for additionalWhereQuery, or null
	 * @param   orderByStr query string to sort the order by, use null to ignore
	 *
	 * @return  The String[] array of object keys
	 **/
	default String[] queryIn_id(String propertyName, Collection<?> values, String additionalWhereQuery,
		Object[] additionalWhereArgs, String orderByStr) {
		return queryIn_id(propertyName, values, additionalWhereQuery, additionalWhereArgs, orderByStr, -1, -1);
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

	//--------------------------------------------------------------------------
	//
	// Index Management Interface
	//
	//--------------------------------------------------------------------------

	/**
	 * Indicates whether this DataObjectMap backend supports index management operations
	 * (listing, inspecting, and programmatically creating indexes).
	 *
	 * @return true if index operations are supported, false otherwise.
	 */
	default boolean supportsIndexManagement() {
		return false;
	}

	/**
	 * Lists all existing indexes on this collection/table as normalized IndexDefinition instances.
	 *
	 * @return list of IndexDefinition objects.
	 */
	default List<IndexDefinition> listIndexes() {
		return Collections.emptyList();
	}

	/**
	 * Checks whether an index with the given name already exists on this collection/table.
	 *
	 * @param indexName name of the index to check
	 * @return true if an index with the given name exists, false otherwise.
	 */
	default boolean hasIndex(String indexName) {
		if (indexName == null) {
			return false;
		}
		// Iterate through existing indexes and check for name match
		for (IndexDefinition idx : listIndexes()) {
			if (indexName.equals(idx.getName())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Checks whether an index with the exact set and order of keys already exists on this collection/table.
	 *
	 * @param indexDef index definition to match keys against
	 * @return true if a matching index exists, false otherwise.
	 */
	default boolean hasIndexWithKeys(IndexDefinition indexDef) {
		if (indexDef == null || indexDef.getKeys() == null || indexDef.getKeys().isEmpty()) {
			return false;
		}
		// Compare key definition and compound order against each existing index
		for (IndexDefinition idx : listIndexes()) {
			if (idx.matchesKeys(indexDef)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Creates an index using a normalized IndexDefinition specification.
	 *
	 * @param indexDef normalized index definition
	 */
	default void createIndex(IndexDefinition indexDef) {
		// Default no-op for backends that do not support index management
	}
	
}
