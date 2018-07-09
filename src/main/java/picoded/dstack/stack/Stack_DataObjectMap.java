package picoded.dstack.stack;

// Java imports
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Picoded imports
import picoded.core.conv.ConvertJSON;
import picoded.core.struct.query.Query;
import picoded.core.common.EmptyArray;
import picoded.core.common.ObjectToken;
import picoded.dstack.*;
import picoded.dstack.core.*;

/**
 * Stacked implementation of DataObjectMap data structure.
 *
 * Built ontop of the Core_DataObjectMap implementation.
 **/
public class Stack_DataObjectMap extends Core_DataObjectMap implements Stack_CommonStructure {
	
	//--------------------------------------------------------------------------
	//
	// Constructor vars
	//
	//--------------------------------------------------------------------------
	
	// Data layers to apply basic read/write against
	protected Core_DataObjectMap[] dataLayers = null;
	
	// Data layer to apply query against
	protected Core_DataObjectMap queryLayer = null;
	
	/**
	 * Setup the data object with the respective data, and query layers
	 * 
	 * @param  inDataLayers data layers to get / set data from, 0 index first
	 * @param  inQueryLayer query layer for queries. Defaults to last data layer
	 */
	public Stack_DataObjectMap(Core_DataObjectMap[] inDataLayers, Core_DataObjectMap inQueryLayer) {
		// Ensure that stack is configured with the respective datalayers
		if (inDataLayers == null || inDataLayers.length <= 0) {
			throw new IllegalArgumentException("Missing valid dataLayers configuration");
		}
		// Configure the query layer, to the last data layer if not set
		if (inQueryLayer == null) {
			inQueryLayer = inDataLayers[inDataLayers.length - 1];
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
	public Stack_DataObjectMap(Core_DataObjectMap[] inDataLayers) {
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
		return (CommonStructure[]) dataLayers;
	}
	
	//--------------------------------------------------------------------------
	//
	// Internal functions, used by DataObject to implement
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
		// Remove data from the lowest layer upwards
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].DataObjectRemoteDataMap_remove(oid);
		}
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * Returns null if not exists
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String oid) {
		// Get the data from the first "source" layer
		for (int i = 0; i < dataLayers.length; ++i) {
			Map<String, Object> res = dataLayers[i].DataObjectRemoteDataMap_get(oid);
			if (res != null) {
				// Populate the data back upwards
				Set<String> resKeySet = res.keySet();
				for (i = i - 1; i >= 0; --i) {
					dataLayers[i].DataObjectRemoteDataMap_update(oid, res, resKeySet);
				}
				return res;
			}
		}
		return null;
	}
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 **/
	public void DataObjectRemoteDataMap_update(String oid, Map<String, Object> fullMap,
		Set<String> keys) {
		// Write data from the lowest layer upwards
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].DataObjectRemoteDataMap_update(oid, fullMap, keys);
		}
	}
	
	/**
	 * Get and returns all the GUID's, note that due to its
	 * potential of returning a large data set, production use
	 * should be avoided.
	 *
	 * @return set of keys
	 **/
	@Override
	public Set<String> keySet() {
		return queryLayer.keySet();
	}
	
	//--------------------------------------------------------------------------
	//
	// Query based optimization
	//
	//--------------------------------------------------------------------------
	
	/// Internal reuse DataObject array representing "no data found"
	protected static final DataObject[] BLANK_DATA_OBJECTS = new DataObject[] {};
	
	/**
	 * Single object query optimization, this is done to optimize Query calls with _oid = ?
	 * This is kinda micro-optimization, but developers are humans.
	 * 
	 * See: https://github.com/picoded/JavaCommons/issues/1
	 * 
	 * This is intentionally only done on stack layer, as its currently
	 * the only setup that would benefit from such a caching setup
	 * 
	 * @param  queryClause to check for single _oid validation
	 * 
	 * @return the single DataObject, if found and valid
	 */
	public DataObject[] singleObjectQuery(Query queryClause) {
		
		// No queryClause, no possible result
		if (queryClause == null) {
			return null;
		}
		
		// Get the where clause as SQL string
		String whereClause = queryClause.toSqlString();
		
		// Assert that it has an _oid clause, and no OR clause
		// if it doesnt, return null
		String oidQuery = "\"_oid\" = ?";
		int oidQueryPos = whereClause.indexOf(oidQuery);
		if (oidQueryPos < 0 || whereClause.contains("OR")) {
			return null;
		}
		
		// There is no OR clause, assume only AND clauses
		// now is to find the argument index position
		int argumentIndex = 0;
		int argumentPos = whereClause.indexOf("?");
		int targetPos = oidQueryPos + oidQuery.length() - 1;
		
		// Iterate possible ? positions, till its actual index is found
		while (argumentPos > 0 && argumentPos < targetPos) {
			++argumentIndex;
			argumentPos = whereClause.indexOf("?", argumentPos + 1);
		}
		
		// Get the query argument to use, and get directly
		DataObject direct = get(queryClause.queryArgumentsArray()[argumentIndex]);
		
		// No object found, terminates
		if (direct == null) {
			return BLANK_DATA_OBJECTS;
		}
		
		// Object found validate it
		if (queryClause.test(direct)) {
			return new DataObject[] { direct };
		}
		
		// Validation failed, return null
		return BLANK_DATA_OBJECTS;
	}
	
	/**
	 * Performs a search query, and returns the respective DataObject keys.
	 *
	 * This is the GUID key varient of query, this is critical for stack lookup
	 *
	 * @param   queryClause, of where query statement and value
	 * @param   orderByStr string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array
	 **/
	public String[] query_id(Query queryClause, String orderByStr, int offset, int limit) {
		
		// Optimize for _oid = ? without or clauses
		DataObject[] singleQueryCheck = singleObjectQuery(queryClause);
		if (singleQueryCheck != null) {
			if (singleQueryCheck == BLANK_DATA_OBJECTS) {
				return EmptyArray.STRING;
			}
			String _oid = singleQueryCheck[0]._oid();
			return new String[] { _oid };
		}
		
		// Standard call against query layer
		return queryLayer.query_id(queryClause, orderByStr, offset, limit);
	}
	
	/**
	 * Performs a search query, and returns the respective DataObjects
	 *
	 * @param   where query statement
	 * @param   where clause values array
	 *
	 * @returns  The total count for the query
	 */
	@Override
	public long queryCount(String whereClause, Object[] whereValues) {
		
		// Optimize for _oid = ? without or clauses
		if (whereClause != null) {
			DataObject[] singleQueryCheck = singleObjectQuery(Query.build(whereClause, whereValues));
			if (singleQueryCheck != null) {
				return singleQueryCheck.length;
			}
		}
		
		// Standard call against query layer
		return queryLayer.queryCount(whereClause, whereValues);
	}
	
	/**
	 * Scans the object and get the various keynames used.
	 * This is used mainly in adminstration interface, etc.
	 *
	 * The seekDepth parameter is ignored in JSql mode, as its optimized.
	 *
	 * @param  seekDepth, which detirmines the upper limit for iterating
	 *         objects for the key names, use -1 to search all
	 *
	 * @return  The various key names used in the objects
	 **/
	@Override
	public Set<String> getKeyNames(int seekDepth) {
		return queryLayer.getKeyNames(seekDepth);
	}
	
	/**
	 * Gets and return a random object ID
	 *
	 * @return  Random object ID
	 **/
	public String randomObjectID() {
		return queryLayer.randomObjectID();
	}
	
	/**
	 * Gets and return the next object ID key for iteration given the current ID,
	 * null gets the first object in iteration.
	 *
	 * @param   Current object ID, can be NULL
	 *
	 * @return  Next object ID, if found
	 **/
	public String looselyIterateObjectID(String currentID) {
		return queryLayer.looselyIterateObjectID(currentID);
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
		for (CommonStructure layer : commonStructureStack()) {
			layer.clear();
		}
	}
	
}
