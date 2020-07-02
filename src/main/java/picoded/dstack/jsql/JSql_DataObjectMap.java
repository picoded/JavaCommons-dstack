package picoded.dstack.jsql;

import java.util.logging.*;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import picoded.core.security.NxtCrypt;
import picoded.dstack.DataObjectMap;
import picoded.dstack.DataObject;
import picoded.dstack.core.Core_DataObjectMap;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.query.Query;
import picoded.core.struct.GenericConvertHashMap;
import picoded.dstack.connector.jsql.*;
import picoded.core.conv.ListValueConv;

/**
 * JSql implmentation of DataObjectMap
 *
 * Due to how complex this class is, it has been split apart into multiple sub classes
 * With this core class, forming the glue code for all the various sub classes.
 * 
 * In addition the optional configMap, is used to provide fixed table functionality,
 * while being configured in the following format.
 * 
 * ```
 * {
 *    "fixedTableMap": {
 *       "SQL_TABLE_NAME": {
 *          // An _oid collumn is required for every fixed table
 *          // as this will glue the various tables together
 *          "_oid": {
 *              // Collumn name for oid is to be provided
 * 				"name": "oID",
 *              // Disable primary key join for this table.
 *              // 
 *              // This should only be enabeld if the child
 *              // table is guaranteed to have its oid key
 *              // in the primary key table.
 *              // 
 *              // This should only be used for 
 *              // performance optimization
 *              "skipPrimaryKeyJoin": true
 *          },
 *          // When object key name and collumn name is identical
 *          // you can simply pass in the collumn type string
 *          "OBJECT_KEY" : "COLLUMN_TYPE",
 *          ...
 *          // If there is a name mismatch, a map can be used instead
 *          // to provide more fine grain controls
 *          "OBJECT_KEY": {
 * 				"name": "COLLUMN_NAME",
 * 	            "type": "COLLUMN_TYPE"
 *          },
 *          ...
 *       },
 *       ...
 *    }
 * }
 * ```
 **/
public class JSql_DataObjectMap extends Core_DataObjectMap {
	
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
	
	/**
	 * The tablename the parent key
	 **/
	protected String primaryKeyTable = null;
	
	/**
	 * Internal query builder 
	 */
	protected JSql_DataObjectMap_QueryBuilder queryBuilder = null;
	
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
	public JSql_DataObjectMap(JSql inJSql, String tablename) {
		this(inJSql, tablename, null);
	}
	
	/**
	 * JSql setup
	 *
	 * @param   JSQL connection
	 * @param   Table name to use
	 **/
	public JSql_DataObjectMap(JSql inJSql, String tablename,
		GenericConvertMap<String, Object> inConfig) {
		super();
		
		sqlObj = inJSql;
		primaryKeyTable = "DP_" + tablename;
		dataStorageTable = "DD_" + tablename;
		
		configMap = inConfig;
		if (configMap == null) {
			configMap = new GenericConvertHashMap<>();
		}
		
		// Note as this depends on the config and the above to be
		// initialized first - this is done last
		queryBuilder = new JSql_DataObjectMap_QueryBuilder(this);
	}
	
	//--------------------------------------------------------------------------
	//
	// Internal config vars
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Object ID field type
	 **/
	protected String objColumnType = "VARCHAR(64)";
	
	/**
	 * Key name field type
	 **/
	protected String keyColumnType = "VARCHAR(64)";
	
	/**
	 * Type collumn type
	 **/
	protected String typeColumnType = "TINYINT";
	
	/**
	 * Index collumn type
	 **/
	protected String indexColumnType = "TINYINT";
	
	/**
	 * String value field type
	 * @TODO: Investigate performance issues for this approach
	 **/
	protected String numColumnType = "DECIMAL(36,12)";
	
	/**
	 * String value field type
	 **/
	protected String strColumnType = "VARCHAR(64)";
	
	/**
	 * Full text value field type
	 **/
	protected String fullTextColumnType = "VARCHAR(MAX)";
	
	/**
	 * Timestamp field type
	 **/
	protected String tStampColumnType = "BIGINT";
	
	/**
	 * Primary key type
	 **/
	protected String pKeyColumnType = "BIGINT PRIMARY KEY AUTOINCREMENT";
	
	/**
	 * Raw datastorage type
	 **/
	protected String rawDataColumnType = "BLOB";
	
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
		
		// BASE Table constructor
		//----------------------------
		sqlObj.createTable( //
			primaryKeyTable, //
			new String[] { //
			// Primary key, as classic int, this is used to lower SQL
			// fragmentation level, and index memory usage. And is not accessible.
			// Sharding and uniqueness of system is still maintained by GUID's
				"pKy", //
				// Time stamps
				"cTm", //object created time
				"uTm", //object updated time
				"eTm", //object expire time (for future use)
				// Object keys
				"oID" //_oid
			}, //
			new String[] { //
			pKeyColumnType, //Primary key
				// Time stamps
				tStampColumnType, //
				tStampColumnType, //
				tStampColumnType, //
				// Object keys
				objColumnType //
			} //
			);
		
		// DATA Table constructor
		//----------------------------
		sqlObj.createTable( //
			dataStorageTable, //
			new String[] { //
			// Primary key, as classic int, this is used to lower SQL
			// fragmentation level, and index memory usage. And is not accessible.
			// Sharding and uniqueness of system is still maintained by GUID's
				"pKy", //
				// Time stamps
				"cTm", //value created time
				"uTm", //value updated time
				"eTm", //value expire time (for future use)
				// Object keys
				"oID", //_oid
				"kID", //key storage
				"idx", //index collumn
				// Value storage (except text)
				"typ", //type collumn
				"nVl", //numeric value (if applicable)
				"sVl", //case insensitive string value (if applicable), or case sensitive hash
				// Text value storage
				"tVl", //Textual storage, placed last for row storage optimization
				"rVl" //Raw binary storage, placed last for row storage optimization
			}, //
			new String[] { //
			pKeyColumnType, //Primary key
				// Time stamps
				tStampColumnType, //
				tStampColumnType, //
				tStampColumnType, //
				// Object keys
				objColumnType, //
				keyColumnType, //
				indexColumnType, //
				// Value storage
				typeColumnType, //
				numColumnType, //
				strColumnType, //
				fullTextColumnType, //
				rawDataColumnType } //
			);
		
		// Unique index
		//------------------------------------------------
		
		// This optimizes query by object keys
		// + oID
		sqlObj.createIndex( //
			primaryKeyTable, "oID", "UNIQUE", "unq" //
		); //
		
		// This optimizes query by object keys,
		// with the following combinations
		// + oID
		// + oID, kID
		// + oID, kID, idx
		sqlObj.createIndex( //
			dataStorageTable, "oID, kID, idx", "UNIQUE", "unq" //
		); //
		
		// Foreign key constraint,
		// to migrate functionality over to JSQL class itself
		try {
			sqlObj.update_raw( //
				"ALTER TABLE " + dataStorageTable + //
					"ADD CONSTRAINT " + dataStorageTable + "_fk" + //
					"FOREIGN KEY (oID) REFERENCES " + primaryKeyTable + "(oID)" + //
					"ON DELETE CASCADE" // NOTE : This slows performance down
				);
		} catch (Exception e) {
			// Silence exception
			// @TODO : properly handle conflicts only
		}
		
		// Key Values search index
		//------------------------------------------------
		
		// This optimizes for numeric values
		// + kID
		// + kID, nVl
		sqlObj.createIndex( //
			dataStorageTable, "kID, nVl", null, "knIdx" //
		); //
		
		// This optimizes for string values
		// + kID
		// + kID, sVl
		sqlObj.createIndex( //
			dataStorageTable, "kID, sVl", null, "ksIdx" //
		); //
		
		// This optimizes for numeric, string value sorting
		// + kID, nVl
		// + kID, nVl, sVl
		sqlObj.createIndex( //
			dataStorageTable, "kID, nVl, sVl", null, "knsIdx" //
		); //
		
		//--------------------------------------------------------------------
		//
		// test / timestamp index, is this needed?
		//
		// Currently commented out till a usage is found for them
		// This can be easily recommented in.
		//
		// Note that the main reason this is commented out is because
		// updated time and created time does not work fully as intended
		// as its is more of a system point of view. Rather then adminstration
		// point of view.
		//
		// A good example is at times several fields in buisness logic is set
		// to NOT want to update the updated time stamp of the object.
		//
		//--------------------------------------------------------------------
		
		// Full text index, for textual data
		//------------------------------------------------
		
		//if (sqlObj.sqlType != JSqlType.sqlite) {
		//	sqlObj.createIndex( //
		//		tName, "tVl", "FULLTEXT", "tVlT" //
		//	);
		//} else {
		// sqlObj.createIndex( //
		// 	dataStorageTable, "tVl", null, "tVlI" // Sqlite uses normal index
		// ); //
		//}
		
		// Create and updated timestamp index
		//------------------------------------------------
		
		// // By created time
		// sqlObj.createIndex( //
		// 	dataStorageTable, "cTm, kID, nVl, sVl", null, "cTm_valMap" //
		// ); //
		
		// // By updated time
		// sqlObj.createIndex( //
		// 	dataStorageTable, "uTm, kID, nVl, sVl", null, "uTm_valMap" //
		// ); //
		
		//sqlObj.createIndex( //
		//	tName, "uTm", null, "uTm" //
		//);
		
		//sqlObj.createIndex( //
		//	tName, "cTm", null, "cTm" //
		//);
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	public void systemDestroy() {
		sqlObj.dropTable(dataStorageTable);
		sqlObj.dropTable(primaryKeyTable);
	}
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		sqlObj.delete(dataStorageTable);
		sqlObj.delete(primaryKeyTable);
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
		queryBuilder.jSqlObjectMapRemove(_oid);
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * @returns null if not exists, else a map with the data
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String _oid) {
		return queryBuilder.jSqlObjectMapFetch(_oid, null);
	}
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 **/
	public void DataObjectRemoteDataMap_update(String _oid, Map<String, Object> fullMap,
		Set<String> keys) {
		
		// Curent timestamp
		long now = JSql_DataObjectMapUtil.getCurrentTimestamp();
		
		// Ensure GUID is registered
		sqlObj.upsert( //
			primaryKeyTable, //
			new String[] { "oID" }, //
			new Object[] { _oid }, //
			new String[] { "uTm" }, //
			new Object[] { now }, //
			new String[] { "cTm", "eTm" }, //
			new Object[] { now, 0 }, //
			null // The only misc col, is pKy, which is being handled by DB
			);
		
		// Does the data append
		queryBuilder.jSqlObjectMapUpdate(_oid, fullMap, keys);
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
		return queryBuilder.getOidKeySet();
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
	 * @param   queryClause, of where query statement and value
	 * @param   orderByStr string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array
	 **/
	public String[] query_id(Query queryClause, String orderByStr, int offset, int limit) {
		if (queryClause == null) {
			return queryBuilder.dataObjectMapQuery_id( //
				null, null, //
				orderByStr, offset, limit //
				);
		}
		return queryBuilder.dataObjectMapQuery_id( //
			queryClause.toSqlString(), //
			queryClause.queryArgumentsArray(), //
			orderByStr, offset, limit //
			);
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
		return queryBuilder.dataObjectMapCount(whereClause, whereValues, null, -1, -1);
	}
	
	//--------------------------------------------------------------------------
	//
	// Get key names handling
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Scans the object and get the various keynames used.
	 * This is used mainly in adminstration interface, etc.
	 *
	 * The seekDepth parameter is ignored in JSql mode, as its optimized.
	 * 
	 * @TODO - Fixed table hybrid support for JSql
	 *
	 * @param  seekDepth, which detirmines the upper limit for iterating
	 *         objects for the key names, use -1 to search all
	 *
	 * @return  The various key names used in the objects
	 **/
	@Override
	public Set<String> getKeyNames(int seekDepth) {
		JSqlResult r = sqlObj.select(dataStorageTable, "DISTINCT kID");
		if (r == null || r.get("kID") == null) {
			return new HashSet<String>();
		}
		
		return ListValueConv.toStringSet(r.getObjectList("kID"));
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
		JSqlResult r = sqlObj.randomSelect(primaryKeyTable, "oID", null, null, 1);
		
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
			r = sqlObj.select(primaryKeyTable, "oID", null, null, "oID ASC", 1, 0);
		} else {
			r = sqlObj.select(primaryKeyTable, "oID", "oID > ?", new Object[] { currentID },
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
