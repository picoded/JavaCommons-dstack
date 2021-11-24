package picoded.dstack.jsql_json;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

import picoded.dstack.DataObjectMap;
import picoded.dstack.DataObject;
import picoded.dstack.core.Core_DataObjectMap;
import picoded.dstack.jsql.JSql_DataObjectMapUtil;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.query.OrderBy;
import picoded.core.struct.query.Query;
import picoded.core.struct.GenericConvertHashMap;
import picoded.dstack.connector.jsql.*;
import picoded.core.conv.ListValueConv;
import picoded.core.conv.ConvertJSON;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.MutablePair;

/**
 * Postgres Jsonb implementation of data object map,
 * optimized specifically for the in built JSONB with postgres GIN index.
 * 
 * Effectively obseleting the original Entity-Key-Value map design
 **/
public class PostgresJsonb_DataObjectMap extends Core_DataObjectMap {
	
	//--------------------------------------------------------------------------
	//
	// Constructor setup
	//
	//--------------------------------------------------------------------------
	
	/**
	 * The inner sql object
	 **/
	protected JSql sqlObj = null;
	
	/**
	 * The tablename for the key value pair map
	 **/
	protected String dataStorageTable = null;
	
	// /**
	//  * Internal query builder 
	//  */
	// protected JSql_DataObjectMap_QueryBuilder queryBuilder = null;
	
	/**
	 * Internal config map specific to this DataObjectMap.
	 * 
	 * This is used primarily to configured fixed tables to be used
	 * with the cored data object map
	 */
	protected GenericConvertMap<String, Object> configMap;
	
	/**
	 * JSql setup
	 *
	 * @param   JSQL connection
	 * @param   Table name to use
	 **/
	public PostgresJsonb_DataObjectMap(JSql inJSql, String tablename) {
		this(inJSql, tablename, null);
	}
	
	/**
	 * JSql setup
	 *
	 * @param   JSQL connection
	 * @param   Table name to use
	 **/
	public PostgresJsonb_DataObjectMap(JSql inJSql, String tablename,
		GenericConvertMap<String, Object> inConfig) {
		super();
		
		sqlObj = inJSql;
		dataStorageTable = "DJ_" + tablename;
		
		configMap = inConfig;
		if (configMap == null) {
			configMap = new GenericConvertHashMap<>();
		}
	}
	
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
		
		//
		// Database primary key designed is intentionally setup
		// to be optimized for cockroachDB setup, while being POSTGRES compliant
		//
		// See: https://www.cockroachlabs.com/docs/v20.2/performance-best-practices-overview.html#unique-id-best-practices
		//
		
		// DATA Table constructor
		//----------------------------
		sqlObj.update_raw( //
			"CREATE TABLE IF NOT EXISTS " + dataStorageTable + " (" + //
				// User based primary key
				"oID VARCHAR(64) UNIQUE, " + //
				// Time stamps
				"cTm BIGINT, " + //
				"uTm BIGINT, " + //
				"eTm BIGINT, " + //
				// JSON data
				"data JSONB, " + //
				// BINARY data
				"bData BYTEA, " + //
				// Real primary key for DB
				"PRIMARY KEY(oID, cTm) " + //
				")" //
			);
		
		// Baseline GIN index
		//----------------------------
		sqlObj.update_raw( //
			"CREATE INDEX IF NOT EXISTS data_idx " + //
				"ON " + dataStorageTable + " " + //
				"USING gin (data)" //
			);
		
		//------------------------------------------------------
		// @TODO: Additional collumn specific B-Tree index
		//        which is declarable by config options
		//------------------------------------------------------
		
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	public void systemDestroy() {
		sqlObj.dropTable(dataStorageTable);
	}
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		sqlObj.delete(dataStorageTable);
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
	public void DataObjectRemoteDataMap_remove(String _oid) {
		// Delete the data
		sqlObj.delete(dataStorageTable, "oID = ?", new Object[] { _oid });
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * @returns null if not exists, else a map with the data
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String _oid) {
		
		// Fetch the datamap
		JSqlResult res = sqlObj.select( //
			dataStorageTable, //
			"*", "oID = ?", //
			new Object[] { _oid } //
			);
		
		// Get thee value lists
		GenericConvertList<Object> data_list = res.get("data");
		
		// Return null, if list is empty
		if (data_list == null || data_list.size() <= 0) {
			return null;
		}
		
		// Convert into a map
		return JsonbUtils.deserializeDataMap(data_list.get(0).toString(),
			(byte[]) (res.get("bData").get(0)));
	}
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 **/
	public void DataObjectRemoteDataMap_update(String _oid, Map<String, Object> fullMap,
		Set<String> keys) {
		
		// Get the data pair
		MutablePair<String, byte[]> dataPair = JsonbUtils.serializeDataMap(fullMap);
		
		// Curent timestamp
		long now = JSql_DataObjectMapUtil.getCurrentTimestamp();
		
		// Ensure GUID is registered
		sqlObj.upsert( //
			dataStorageTable, //
			new String[] { "oID" }, //
			new Object[] { _oid }, //
			new String[] { "uTm", "data", "bData" }, //
			new Object[] { now, dataPair.getLeft(), dataPair.getRight() }, //
			new String[] { "cTm", "eTm" }, //
			new Object[] { now, 0 }, //
			null // The only misc col, is pKy, which is being handled by DB
			);
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
		
		// Fetch the oID kust
		JSqlResult res = sqlObj.select(dataStorageTable, "oID");
		
		// Convert it into a set
		if (res == null || res.get("oID") == null) {
			return new HashSet<String>();
		}
		return ListValueConv.toStringSet(res.getObjectList("oID"));
	}
	
	//--------------------------------------------------------------------------
	//
	// Query based optimization
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Performs a search query, and returns the respective DataObject keys.
	 *
	 * This is the GUID key varient of query, this is critical for stack lookup
	 *
	 * @param whereClause         where query statement
	 * @param whereValues         where clause values array
	 * @param orderByStr          orderBy clause string, to sort result
	 * @param offset              offset of the result to display, use -1 to ignore
	 * @param limit               number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array
	 **/
	public String[] query_id(String whereClause, Object[] whereValues, String orderByStr,
		int offset, int limit) {
		
		// Build the full query
		MutablePair<String, Object[]> fullRawQuery = JsonbUtils.fullQueryRawBuilder( //
			dataStorageTable, "oID", //
			whereClause, whereValues, //
			orderByStr, offset, limit //
		);

		// Execute and get the result
		// we do a raw query, to avoid any JSQL parsing, which was not designed for 
		// JSONB postgres use case (manual overwrite being done here)
		JSqlResult res = sqlObj.query_raw(fullRawQuery.left, fullRawQuery.right);

		// Get the oID list and return it
		List<Object> oID_list = res.getObjectList("oID");
		
		// Generate the object list
		if (oID_list != null) {
			return ListValueConv.objectListToStringArray(oID_list);
		}
		
		// Blank list as fallback (nothing found)
		return new String[0];
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
		return query_id(queryClause.toString(), queryClause.queryArgumentsArray(), orderByStr,
			offset, limit);
	}
	
	/**
	 * Performs a search query, and returns the respective count
	 *
	 * @param   where query statement
	 * @param   where clause values array
	 *
	 * @returns  The total count for the query
	 */
	@Override
	public long queryCount(String whereClause, Object[] whereValues) {
		// Build the full query
		MutablePair<String, Object[]> fullRawQuery = JsonbUtils.fullQueryRawBuilder( //
			dataStorageTable, "COUNT(*) AS rcount", //
			whereClause, whereValues, //
			null, -1, -1 //
		);

		// Execute and get the result
		// we do a raw query, to avoid any JSQL parsing, which was not designed for 
		// JSONB postgres use case (manual overwrite being done here)
		JSqlResult res = sqlObj.query_raw(fullRawQuery.left, fullRawQuery.right);

		// Get rcount result
		GenericConvertList<Object> rcountArr = res.get("rcount");
		
		// Get the rcount arr
		if (rcountArr != null && rcountArr.size() > 0) {
			return rcountArr.getLong(0);
		}
		// Blank as fallback
		return 0;
	}
	
	//--------------------------------------------------------------------------
	//
	// Special iteration support
	//
	// NOTE: Fixed table hybrid support is not implemented for this function
	//       this is partially intentional to limit the performance impact
	//       and maybe reviewed overtime
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [NOTE: Fixed table hybrid support is not implemented for this function]
	 * 
	 * Gets and return a random object ID
	 *
	 * @return  Random object ID
	 **/
	public String randomObjectID() {
		// Get a random ID
		JSqlResult r = sqlObj.randomSelect(dataStorageTable, "oID", null, null, 1);
		
		// No result : NULL
		if (r == null || r.get("oID") == null || r.rowCount() <= 0) {
			return null;
		}
		
		// Return the result
		return r.getStringArray("oID")[0];
	}
	
	/**
	 * [NOTE: Fixed table hybrid support is not implemented for this function]
	 * 
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
	 * Another way to phrase it, in worse case scenerio, its completely random, eventually iterating all objects
	 * In best case scenerio, it does proper iteration as per normal.
	 *
	 * @param   Current object ID, can be NULL
	 *
	 * @return  Next object ID, if found
	 **/
	public String looselyIterateObjectID(String currentID) {
		// Result set to fetch next ID
		JSqlResult r = null;
		if (currentID == null) {
			r = sqlObj.select(dataStorageTable, "oID", null, null, "oID ASC", 1, 0);
		} else {
			r = sqlObj.select(dataStorageTable, "oID", "oID > ?", new Object[] { currentID },
				"oID ASC", 1, 0);
		}
		
		// No result : NULL
		if (r == null || r.get("oID") == null || r.rowCount() <= 0) {
			return null;
		}
		
		// Return the result
		return r.getStringArray("oID")[0];
	}
	
}
