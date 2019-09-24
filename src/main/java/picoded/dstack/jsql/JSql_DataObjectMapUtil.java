package picoded.dstack.jsql;

// Java imports
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.SQLException;

// Picoded imports
import picoded.core.conv.*;
import picoded.core.common.*;
import picoded.dstack.*;
import picoded.dstack.core.*;
import picoded.dstack.connector.jsql.*;
import picoded.core.struct.*;
import picoded.core.struct.query.*;
import picoded.core.struct.query.condition.*;
import picoded.core.struct.query.internal.*;

/**
 * Protected class, used to orgainze the various JSql based logic
 * used in DataObjectMap.
 *
 * The larger intention is to keep the DataObjectMap class more maintainable and unit testable
 **/
public class JSql_DataObjectMapUtil {
	
	/**
	 * Static local logger
	 **/
	protected static Logger LOGGER = Logger.getLogger(JSql_DataObjectMapUtil.class.getName());
	
	//--------------------------------------------------------------------------
	//
	// JSqlResult search
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Fetches the result array position using the filters
	 *
	 * This is done, as each row in the SQL query only represents
	 * an object, key, value, value index pair.
	 *
	 * This is done to search out the row position for the result matching the criteria
	 *
	 * @param  JSqlResult to search up the row
	 * @param  _oid to check against, ignored if null
	 * @param  the key to check against, ignored if null
	 * @param  the idx to check against, ignored if -10 or below
	 *
	 * @return   row of the JSqlResult, -1 if failed to find
	 **/
	protected static int fetchResultPosition(JSqlResult r, String _oid, String key, int idx) {
		GenericConvertList<Object> oID_array = r.get("oID");
		GenericConvertList<Object> kID_array = r.get("kID");
		GenericConvertList<Object> idx_array = r.get("idx");
		
		int lim = kID_array.size();
		for (int i = 0; i < lim; ++i) {
			
			if (_oid != null && !_oid.equals(oID_array.get(i))) {
				continue;
			}
			
			if (key != null && !key.equals(kID_array.getString(i))) {
				continue;
			}
			
			if (idx > -9 && idx != (idx_array.getInt(i))) {
				continue;
			}
			
			return i;
		}
		
		return -1;
	}
	
	/**
	 * Fetches the result array position using the filters
	 *
	 * This is done, as each row in the SQL query only represents
	 * an object, key, value, value index pair.
	 *
	 * This is done to search out the row position for the result matching the criteria
	 *
	 * @param  JSqlResult to search up the row
	 * @param  the key to check against, ignored if null
	 * @param  the idx to check against, ignored if -10 or below
	 *
	 * @return   row of the JSqlResult, -1 if failed to find
	 **/
	protected static int fetchResultPosition(JSqlResult r, String key, int idx) {
		return fetchResultPosition(r, null, key, idx);
	}
	
	//--------------------------------------------------------------------------
	//
	// Core_DataType and SQL handling
	//
	//------------------------------------------------------------------------------------------
	
	/**
	 * The shorten string value used to represent the object
	 *
	 * This is used to truncate the string value to its searchable string length.
	 * Currently this length is set to atmost 64, maybe extended in the future
	 *
	 * @param  The input object value
	 *
	 * @return the shorten return value
	 **/
	public static String shortenStringValue(Object value) {
		String shortenValue = value.toString().toLowerCase();
		if (shortenValue.length() > 64) {
			shortenValue = shortenValue.substring(0, 64);
		}
		return shortenValue;
	}
	
	/**
	 * An option set, represents the following storage collumn
	 *
	 * "typ", //type collumn
	 * "nVl", //numeric value (if applicable)
	 * "sVl", //case insensitive string value (if applicable), or case sensitive hash
	 * "tVl", //Textual storage, placed last for row storage optimization
	 * "rVl", //Raw binary storage, placed last for row storage optimization
	 *
	 * In the format of an Object[] array, this is built using the following function
	 *
	 * @param  Value type
	 * @param  Numeric value to store (if any)
	 * @param  Shorten string value
	 * @param  Full string value
	 * @param  raw binary data
	 *
	 * @return  valueTypeSet set
	 **/
	protected static Object[] valueTypeSet(int type, Number value, String shortStr, String fullStr,
		byte[] rawVal) {
		return new Object[] { new Integer(type), value, shortStr, fullStr, rawVal };
	}
	
	/**
	 * Values to option set conversion used by JSql
	 * Note collumn strong typing is now DEPRECATED (for now)
	 *
	 * @TODO: Support array sets
	 * @TODO: Support GUID hash
	 * @TODO: Support DataObjectMap/Object
	 * @TODO: Check against configured type
	 * @TODO: Convert to configured type if possible (like numeric)
	 *
	 * @param  Value to store
	 *
	 * @return  valueTypeSet
	 **/
	public static Object[] valueToValueTypeSet(Object value) {
		// Type flag to use
		int type = 0;
		
		// Null type support
		if (value == null) {
			return valueTypeSet(0, null, null, null, null);
		}
		
		// Numeric type support
		if (value instanceof Number) {
			if (value instanceof Integer) {
				type = Core_DataType.INTEGER.getValue();
			} else if (value instanceof Float) {
				type = Core_DataType.FLOAT.getValue();
			} else if (value instanceof Double) {
				type = Core_DataType.DOUBLE.getValue();
			} else if (value instanceof Long) {
				type = Core_DataType.LONG.getValue();
			} else {
				// When all else fail, use Double type
				type = Core_DataType.DOUBLE.getValue();
				value = new Double(((Number) value).doubleValue());
			}
			return valueTypeSet(type, (Number) value, shortenStringValue(value), value.toString(),
				EmptyArray.BYTE);
		}
		
		// String type support
		if (value instanceof String) {
			return valueTypeSet(Core_DataType.STRING.getValue(), null, shortenStringValue(value),
				value.toString(), EmptyArray.BYTE);
		}
		
		// Binary type support
		if (value instanceof byte[]) {
			return valueTypeSet(Core_DataType.BINARY.getValue(), null, null, null, (byte[]) value);
		}
		
		// Fallback JSON support
		String jsonString = ConvertJSON.fromObject(value);
		return valueTypeSet(Core_DataType.JSON.getValue(), null, null, jsonString.toString(),
			EmptyArray.BYTE);
	}
	
	/**
	 * Takes in the JSqlResult from a DataObjectMap internal table query
	 * And extract out the respective result value
	 *
	 * @TODO: Support GUID hash
	 * @TODO: Support DataObjectMap
	 *
	 * @param  The jsql result set from a select call
	 * @param  Row position to fetch values from result
	 *
	 * @return  The object value
	 **/
	protected static Object extractNonArrayValueFromPos(JSqlResult r, int pos) {
		//
		// Get the storage type setting
		//
		int baseType = ((Number) (r.get("typ").get(pos))).intValue();
		
		//
		// Null type support
		//
		if (baseType == Core_DataType.NULL.getValue()) {
			return null;
		}
		
		//
		// Int, Long, Double, Float
		//
		if (baseType == Core_DataType.INTEGER.getValue()) {
			return new Integer(r.get("nVl").getInt(pos));
		} else if (baseType == Core_DataType.LONG.getValue()) {
			return new Long(r.get("nVl").getLong(pos));
		} else if (baseType == Core_DataType.FLOAT.getValue()) {
			return new Float(r.get("nVl").getFloat(pos));
		} else if (baseType == Core_DataType.DOUBLE.getValue()) {
			return new Double(r.get("nVl").getDouble(pos));
		}
		
		//
		// String / Text value support
		//
		if (baseType == Core_DataType.STRING.getValue()) { // String
			return r.get("tVl").getString(pos);
		} else if (baseType == Core_DataType.TEXT.getValue()) { // Text
			return r.get("tVl").getString(pos);
		}
		
		//
		// Binary value
		//
		if (baseType == Core_DataType.BINARY.getValue()) {
			// Older base64 stroage format
			// return (Base64.getDecoder().decode((String) (r.get("tVl").get(pos))));
			
			Object rawValue = r.get("rVl").get(pos);
			if (rawValue instanceof java.sql.Blob) {
				java.sql.Blob blobData = (java.sql.Blob) rawValue;
				try {
					// GetBytes is 1 indexed??
					// See: https://docs.oracle.com/javase/7/docs/api/java/sql/Blob.html#length()
					return blobData.getBytes(1, (int) (blobData.length()));
				} catch (SQLException e) {
					throw new JSqlException(e);
				} finally {
					try {
						blobData.free();
					} catch (SQLException e) {
						throw new JSqlException(e);
					}
				}
			}
			return (byte[]) rawValue;
		}
		
		//
		// JSON value support
		//
		if (baseType == Core_DataType.JSON.getValue()) { // JSON
			return ConvertJSON.toObject(r.get("tVl").getString(pos));
		}
		
		throw new RuntimeException("Object type not yet supported: oID = " + r.get("oID").get(pos)
			+ ", kID = " + r.get("kID").get(pos) + ", BaseType = " + baseType);
		
		//throw new RuntimeException("Object type not yet supported: Pos = "+pos+", BaseType = "+ baseType);
	}
	
	/**
	 * Same as extractNonArrayValueFromPos, however returns oid, and row key names as well
	 *
	 * @param  The jsql result set from a select call
	 * @param  Row position to fetch values from result
	 *
	 * @return  The object[] array representing [ kID, and value ]
	 **/
	protected static Object[] extractKeyValueFromPos_nonArray(JSqlResult r, int pos) {
		Object value = extractNonArrayValueFromPos(r, pos);
		List<Object> idArr = r.get("kID");
		return new Object[] { idArr.get(pos), value };
	}
	
	/**
	 * Extract the key value
	 *
	 * @TODO: Support array sets
	 *
	 * @param  The jsql result set from a select call
	 * @param  Keyname to use for extraction
	 *
	 * @return  The object value
	 **/
	protected static Object extractKeyValue(JSqlResult r, String key) throws JSqlException {
		int pos = fetchResultPosition(r, key, 0); //get the 0 pos value
		if (pos <= -1) {
			return null;
		}
		return extractNonArrayValueFromPos(r, pos);
	}
	
	//------------------------------------------------------------------------------------------
	//
	// JSQL operations - with data opbject map
	//
	//------------------------------------------------------------------------------------------
	
	/**
	 * Get the current timestamp in seconds
	 **/
	public static long getCurrentTimestamp() {
		return (System.currentTimeMillis() / 1000L);
	}
	
	/**
	 * perform the requried JSQL insert for the given key set
	 * 
	 * @param {JSql} sql                  - sql connection to write into the table
	 * @param {String} tName              - table name to setup, this holds the actual meta table data
	 * @param {String} _oid               - object id to store the key value pairs into
	 * @param {Map<String,Object>} objMap - map to extract values to store from
	 * @param {Set<String>} keyList       - keylist to limit append load
	 * @param {long} now                  - current "now" timestamp
	 * @return
	 */
	protected static boolean jSql_insert(JSql sql, String tName, String _oid, //
		Map<String, Object> objMap, Set<String> keyList, //
		long now //
	) {
		// Nothing to update, nothing to do
		if (keyList == null) {
			return true;
		}
		
		try {
			//
			// Iterate and store ONLY values inside the keyList
			// This helps optimize writes to only changed data
			//---------------------------------------------------------
			
			// Prepare the large multiUpsert values
			List<Object[]> insertValuesList = new ArrayList<Object[]>();
			
			// Iterate the key list to apply updates
			for (String k : keyList) {
				// Skip reserved key, otm is not allowed to be saved
				// (to ensure blank object is saved)
				if (k.equalsIgnoreCase("_otm")) { //reserved
					continue;
				}
				
				// Key length size protection
				if (k.length() > 64) {
					throw new RuntimeException(
						"Attempted to insert a key value larger then 64 for (_oid = " + _oid + "): " + k);
				}
				
				// Get the value to insert
				Object v = objMap.get(k);
				
				// Delete support
				if (v == ObjectToken.NULL || v == null) {
					// Skip reserved key, oid key is allowed to be removed directly
					if (k.equalsIgnoreCase("oid") || k.equalsIgnoreCase("_oid")) {
						continue;
					}
					sql.delete(tName, "oID=? AND kID=?", new Object[] { _oid, k });
				} else {
					// Converts it into a type set, and store it
					Object[] typSet = valueToValueTypeSet(v);
					
					// Setup the multiUpsert
					insertValuesList.add(new Object[] { //
						// The unique columns
							_oid, k, 0, //
							// Type, Numeric / String / Text / Raw storage
							typSet[0], typSet[1], typSet[2], typSet[3], typSet[4], //
							// Updated / Expire / Created timestamp setup
							now, 0, now //
						});
				}
			}
			
			// Nothing to update, nothing to do
			if (insertValuesList.size() <= 0) {
				return false;
			}
			
			// Does the actual multi upsert
			return sql.multiInsert(tName, // Table name to upsert on
				// "pKy" is auto generated by SQL db
				new String[] { //
				// "pKy" is auto generated by SQL db
				// The unique columns
					"oID", "kID", "idx", //
					// Type, Numeric / String / Text / Raw storage
					"typ", "nVl", "sVl", "tVl", "rVl", //
					// Updated / Expire / Created timestamp setup
					"uTm", "eTm", "cTm" //
				}, //
				insertValuesList //
				);
			
		} catch (Exception e) {
			throw new JSqlException(e);
		}
	}
	
	/**
	 * Iterates the relevent keyList, and appends its value from the objMap, into the sql colTypes database
	 *
	 * @param {JSql} sql                  - sql connection to write into the table
	 * @param {String} tName              - table name to setup, this holds the actual meta table data
	 * @param {String} _oid               - object id to store the key value pairs into
	 * @param {Map<String,Object>} objMap - map to extract values to store from
	 * @param {Set<String>} keyList       - keylist to limit append load
	 * @param {long} now                  - current "now" timestamp
	 **/
	public static boolean jSql_upsert( //
		JSql sql, String tName, String _oid, //
		Map<String, Object> objMap, Set<String> keyList, //
		long now //
	) {
		
		// Nothing to update, nothing to do
		if (keyList == null) {
			return true;
		}
		
		try {
			//
			// Iterate and store ONLY values inside the keyList
			// This helps optimize writes to only changed data
			//---------------------------------------------------------
			
			// Prepare the large multiUpsert values
			List<Object[]> uniqueValuesList = new ArrayList<Object[]>();
			List<Object[]> insertValuesList = new ArrayList<Object[]>();
			List<Object[]> defaultValuesList = new ArrayList<Object[]>();
			
			// Iterate the key list to apply updates
			for (String k : keyList) {
				// Skip reserved key, otm is not allowed to be saved
				// (to ensure blank object is saved)
				if (k.equalsIgnoreCase("_otm")) { //reserved
					continue;
				}
				
				// Key length size protection
				if (k.length() > 64) {
					throw new RuntimeException(
						"Attempted to insert a key value larger then 64 for (_oid = " + _oid + "): " + k);
				}
				
				// Get the value to insert
				Object v = objMap.get(k);
				
				// Delete support
				if (v == ObjectToken.NULL || v == null) {
					// Skip reserved key, oid key is allowed to be removed directly
					if (k.equalsIgnoreCase("oid") || k.equalsIgnoreCase("_oid")) {
						continue;
					}
					sql.delete(tName, "oID=? AND kID=?", new Object[] { _oid, k });
				} else {
					// Converts it into a type set, and store it
					Object[] typSet = valueToValueTypeSet(v);
					
					// Setup the multiUpsert
					uniqueValuesList.add(new Object[] { _oid, k, 0 });
					insertValuesList.add(new Object[] { typSet[0], typSet[1], typSet[2], typSet[3],
						typSet[4], now, 0 });
					defaultValuesList.add(new Object[] { now });
				}
			}
			
			// Nothing to update, nothing to do
			if (insertValuesList.size() <= 0) {
				return true;
			}
			
			// Does the actual multi upsert
			return sql.multiUpsert(tName, // Table name to upsert on
				// "pKy" is auto generated by SQL db
				new String[] { "oID", "kID", "idx" }, // The unique column names
				uniqueValuesList, // The row unique identifier values
				// Value / Text / Raw storage + Updated / Expire time stamp
				new String[] { "typ", "nVl", "sVl", "tVl", "rVl", "uTm", "eTm" }, //
				insertValuesList, //
				// Created timestamp setup
				new String[] { "cTm" }, //
				defaultValuesList, //
				null // The only misc col, is pKy, which is being handled by DB
				);
			
		} catch (Exception e) {
			throw new JSqlException(e);
		}
	}
	
	//------------------------------------------------------------------------------------------
	//
	// jSqlObjectMapUpdate - core update logic
	//
	//------------------------------------------------------------------------------------------
	
	/**
	 * Internal batch size upper limit - for jsql object map updates
	 * this is to work around known parameter limitations for certin SQL implementations
	 */
	public static final int jSqlObjectBatchSize = 200;
	
	/**
	 * Iterates the relevent keyList, and appends its value from the objMap, into the sql colTypes database
	 *
	 * @param {JSql} sql                  - sql connection to write into the table
	 * @param {String} tName              - table name to setup, this holds the actual meta table data
	 * @param {String} _oid               - object id to store the key value pairs into
	 * @param {Map<String,Object>} objMap - map to extract values to store from
	 * @param {Set<String>} keyList       - keylist to limit insert load
	 **/
	public static boolean jSqlObjectMapUpdate( //
		JSql sql, String tName, String _oid, //
		Map<String, Object> objMap, Set<String> keyList //
	) {
		// Skip if keylist size is 0
		if (keyList == null || keyList.size() <= 0) {
			return true;
		}
		
		// Internal boolean flag to try optimized inserts
		boolean tryOptimizedInsert = false;
		long now = getCurrentTimestamp();
		
		// Get the obj map set, and evaluate the insert flag
		Set<String> objMapSet = objMap.keySet();
		if (objMapSet.size() <= keyList.size()) {
			tryOptimizedInsert = true;
		}
		
		// Sets to keep track of
		Set<String> toApplySet = new HashSet<>(keyList);
		Set<String> appliedSet = new HashSet<>();
		
		// Lets iterate everything in batches
		// until they are applied
		while (toApplySet.size() > 0) {
			// The working set for the current iteration
			Set<String> workingSet = new HashSet<>();
			int workingSetSize = 0;
			
			// Lets setup the workingSet for the current batch
			for (String key : toApplySet) {
				workingSet.add(key);
				++workingSetSize;
				
				if (workingSetSize >= jSqlObjectBatchSize) {
					break;
				}
			}
			
			// Flag for succesful update
			boolean updateCompleted = false;
			
			// Ok we got a working set - lets try to set it up
			// with optimized inserts if possible - disable optimized insert if it fails
			if (tryOptimizedInsert) {
				try {
					boolean insertResult = jSql_insert(sql, tName, _oid, objMap, keyList, now);
					if (insertResult == false) {
						// Insert request failed - perform fallback
						// Exit the optimized loop
						tryOptimizedInsert = false;
					} else {
						updateCompleted = true; // Insert suceeded, skip to start of loop
					}
				} catch (Exception e) {
					// Insert request failed - perform fallback
					tryOptimizedInsert = false;
				}
			}
			
			// Lets do the upsert operation
			// if an update didnt previously happen
			if (updateCompleted == false) {
				updateCompleted = jSql_upsert(sql, tName, _oid, objMap, keyList, now);
			}
			
			// Throw an error if update failed 
			if (updateCompleted == false) {
				throw new RuntimeException(
					"Failed to perform the required insert / upsert operations required");
			}
			
			// Ok lets - update the various sets
			appliedSet.addAll(workingSet);
			toApplySet.removeAll(workingSet);
		}
		
		// Return true for succeful update
		return true;
	}
	
	/**
	 * Extracts and build the map stored under an _oid
	 *
	 * @param {JSql} sql                  - sql connection to setup the table
	 * @param {String} sqlTableName       - table name to setup, this holds the actual meta table data
	 * @param {String} _oid               - object id to store the key value pairs into
	 * @param {Map<String,Object>} ret    - map to populate, and return, created if null if there is data
	 **/
	public static Map<String, Object> jSqlObjectMapFetch( //
		JSql sql, //
		String sqlTableName, String _oid, //
		Map<String, Object> ret //
	) {
		JSqlResult r = sql.select(sqlTableName, "*", "oID=?", new Object[] { _oid });
		return extractObjectMapFromJSqlResult(r, _oid, ret);
	}
	
	/**
	 * Extracts and build the map stored under an _oid, from the JSqlResult
	 *
	 * @param {JSqlResult} r              - sql result
	 * @param {String} _oid               - object id to store the key value pairs into
	 * @param {Map<String,Object>} ret    - map to populate, and return, created if null and there is data
	 **/
	public static Map<String, Object> extractObjectMapFromJSqlResult(//
		JSqlResult r, String _oid, Map<String, Object> ret //
	) {
		// No result means no data to extract
		if (r == null) {
			return ret;
		}
		
		// Get thee value lists
		GenericConvertList<Object> oID_list = r.get("oID");
		GenericConvertList<Object> kID_list = r.get("kID");
		GenericConvertList<Object> idx_list = r.get("idx");
		
		// This is a query call, hence no data to extract
		if (kID_list == null || kID_list.size() <= 0) {
			return ret;
		}
		
		// Iterate the keys
		int lim = kID_list.size();
		for (int i = 0; i < lim; ++i) {
			
			// oid provided, and does not match, terminated
			if (_oid != null && !_oid.equals(oID_list.get(i))) {
				continue;
			}
			
			// Ignore non 0-indexed value (array support not added yet)
			if (idx_list.getInt(i) != 0) {
				continue; //Now only accepts first value (not an array)
			}
			
			// Extract out key value pair
			Object[] rowData = extractKeyValueFromPos_nonArray(r, i);
			
			// Only check for ret, at this point,
			// so returning null when no data occurs
			if (ret == null) {
				ret = new HashMap<String, Object>();
			}
			
			// Add in vallue
			ret.put(rowData[0].toString(), rowData[1]);
		}
		
		return ret;
	}
	
	//========================================================================================
	//
	// Super complicated complex inner join query builder here
	//
	// DO-NOT attempt to make changes here without ATLEAST an indepth
	// understanding of meta-table structure, and how inner join queries work
	//
	// The complexity of this query generator SHOULD NOT be underextimated
	//
	//========================================================================================
	
	// Complex query static vars
	//-----------------------------------------------------------------------------
	
	/**
	 * Select column statements for DISTINCT oID
	 **/
	protected static String oid_distinct = "DISTINCT oID";
	
	/**
	 * Select column statements for counting DISTINCT oID
	 **/
	protected static String oid_distinctCount = "COUNT(DISTINCT oID) AS rcount";
	
	// Complex query utilities
	//-----------------------------------------------------------------------------
	
	/**
	 * Does the value to Core_DataType conversion used in query search
	 *
	 * @param   Value used
	 *
	 * @return  Core_DataType value
	 **/
	public static Core_DataType searchValueToCore_DataType(Object value) {
		if (value instanceof Integer) {
			return Core_DataType.INTEGER;
		} else if (value instanceof Float) {
			return Core_DataType.FLOAT;
		} else if (value instanceof Double) {
			return Core_DataType.DOUBLE;
		} else if (value instanceof Long) {
			return Core_DataType.LONG;
		} else if (value instanceof String) {
			return Core_DataType.STRING;
		}
		// else if (value instanceof byte[]) {
		// 	return Core_DataType.BINARY;
		// } else {
		//	return Core_DataType.JSON;
		//}
		return null;
	}
	
	/**
	 * Sanatizes a query key, for a somewhat SQL safer varient
	 *
	 * @param  Key to sanatize
	 *
	 * @return  sanatized key to return
	 **/
	public static String escapeQueryKey(String key) {
		return key.replaceAll("[\u0000-\u001f]", "") // replaces invisible characters
			.replaceAll("\\\\", "\\\\") // Replaces the "\" character
			.replaceAll("\\\"", "\\\"") // Replaces the "double quotes"
			.replaceAll("\\'", "\\'") // Replaces the 'single quotes'
			.replaceAll("\\`", "\\`") // Replaces the `oracle quotes`
			.replaceAll("\\[", "\\[") // Replaces the [ms-sql opening brackets]
			.replaceAll("\\]", "\\]") // Replaces the [ms-sql closing brackets]
			.replaceAll("\\#", "\\#"); // Replaces the reserved # key
	}
	
	/**
	 * Sanatize the order by string, and places the field name as query arguments
	 *
	 * @param  Raw order by string
	 *
	 * @return  Order by function obj
	 **/
	public static OrderBy<DataObject> getOrderByObject(String rawString) {
		// Clear out excess whtiespace
		rawString = rawString.trim().replaceAll("\\s+", " ");
		if (rawString.length() <= 0) {
			throw new RuntimeException("Unexpected blank found in OrderBy query : " + rawString);
		}
		
		return new OrderBy<DataObject>(rawString);
	}
	
	// Complex query actual implementation
	//-----------------------------------------------------------------------------
	
	/**
	 * Lowercase suffix string
	 **/
	protected static String lowerCaseSuffix = "#lc";
	
	/**
	 * The complex left inner join StringBuilder used for view / query requests
	 *
	 * This helps build a complex temporary table view, for use in a query.
	 *
	 * This somewhat represents the actual table,
	 * you would have quried against in traditional fixed SQL view
	 *
	 * @param  sql connection used, this is used to detect vendor specific logic =(
	 * @param  meta table name, used to pull the actual data the view is based on
	 * @param  type mapping to build the complex view from
	 * @param  additional arguments needed to build the query,
	 *          this serves as an additional return value and is hence required
	 *
	 * @return StringBuilder for the view building statement, this can be used for creating permenant view / queries
	 **/
	protected static StringBuilder complexQueryView(JSql sql, String tableName,
		Map<String, Core_DataType> mtm, List<Object> queryArgs) {
		//
		// Vendor specific customization
		//-----------------------------------------
		
		String joinType = "LEFT";
		
		String lBracket = "\"";
		String rBracket = "\"";
		
		// Reserved to overwrite, and to do more complex quotes
		if (sql.sqlType() == JSqlType.MSSQL) {
			lBracket = "[";
			rBracket = "]";
		}
		
		//
		// Select / From StringBuilder setup
		//
		// Also setup the distinct oID collumn
		//
		//-----------------------------------------
		
		// This statement section, represents the final view "columns" to use
		StringBuilder select = new StringBuilder("SELECT B.oID AS ");
		select.append(lBracket + "oID" + rBracket);
		
		// This statement section, reprsents the underlying actual "columns" to fetch from
		StringBuilder from = new StringBuilder(" FROM ");
		from.append("(SELECT DISTINCT oID");
		
		//
		// Distinct table reference
		//
		// This is used to reduce the result set
		// to a single row per oID
		//
		//-----------------------------------------
		
		from.append(" FROM " + tableName + ")");
		//from.append( tableName );
		from.append(" AS B");
		
		//
		// Select / From query building base on Core_DataType
		//
		//--------------------------------------------------
		
		// Join count, is required to ensure table labeling does not colide
		int joinCount = 0;
		
		//
		// Iterate every Core_DataType, and build their respective column SELECT/FROM statement
		//
		for (Map.Entry<String, Core_DataType> e : mtm.entrySet()) {
			
			// Get key, safekey, and type
			String rawKey = e.getKey();
			String safeKey = escapeQueryKey(rawKey);
			Core_DataType type = e.getValue();
			
			if ( //
			type == Core_DataType.INTEGER || //
				type == Core_DataType.FLOAT || //
				type == Core_DataType.DOUBLE || type == Core_DataType.LONG) { //
				//---------------------------
				// Numeric column processing
				//---------------------------
				
				// Get numeric column
				select.append(", N" + joinCount + ".nVl AS ");
				select.append(lBracket + safeKey + rBracket);
				// Get string column, and shorten lowercase
				select.append(", N" + joinCount + ".sVl AS ");
				select.append(lBracket + safeKey + lowerCaseSuffix + rBracket);
				
				// Joined from, with unique OID
				from.append(" " + joinType + " JOIN " + tableName + " AS N" + joinCount);
				from.append(" ON B.oID = N" + joinCount + ".oID");
				// and is not an array, while matching raw key
				from.append(" AND N" + joinCount + ".idx = 0 AND N" + joinCount + ".kID = ?");
				queryArgs.add(rawKey);
				
			} else if (type == Core_DataType.STRING) {
				//---------------------------
				// String column processing
				//---------------------------
				
				// Get string column, in full representation,
				select.append(", S" + joinCount + ".tVl AS ");
				select.append(lBracket + safeKey + rBracket);
				// Get string column, and shorten lowercase
				select.append(", S" + joinCount + ".sVl AS ");
				select.append(lBracket + safeKey + lowerCaseSuffix + rBracket);
				
				// Joined from, with unique OID
				from.append(" " + joinType + " JOIN " + tableName + " AS S" + joinCount);
				from.append(" ON B.oID = S" + joinCount + ".oID");
				// and is not an array, while matching raw key
				from.append(" AND S" + joinCount + ".idx = 0 AND S" + joinCount + ".kID = ?");
				queryArgs.add(rawKey);
				
			} else {
				// Unknown Core_DataType ignored
				LOGGER.log(Level.WARNING, "complexQueryView -> Unsupported Core_DataType (" + type
					+ ") - for meta key : " + rawKey);
			}
			
			++joinCount;
		}
		
		// The final return string builder
		StringBuilder ret = new StringBuilder();
		ret.append(select);
		ret.append(from);
		
		// Return StringBuilder
		return ret;
	}
	
	/**
	 * Performs a search query, and returns the respective DataObjects information
	 * This works by taking the query and its args, building its complex inner view, then querying that view.
	 *
	 * CURRENTLY: It is entirely dependent on the whereValues object type to perform the relevent search criteria
	 * @TODO: Performs the search pattern using the respective type map
	 *
	 * @param   DataObjectMap object to refrence from
	 * @param   JSql connection to use
	 * @param   JSql table name to use
	 * @param   The selected columns to query
	 * @param   where query statement
	 * @param   where clause values array
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max
	 *
	 * @return  The JSql query result
	 **/
	protected static JSqlResult runComplexQuery( //
		DataObjectMap dataObjectMapObj, JSql sql, String tablename, String selectedCols, //
		String whereClause, Object[] whereValues, String orderByStr, int offset, int limit //
	) { //
	
		//----------------------------------------------------------------------
		// Quick optimal lookup : Does not do any complext building
		// Runs the query and exit immediately
		//----------------------------------------------------------------------
		if (whereClause == null && orderByStr == null && offset <= 0 && limit <= 0
			&& (selectedCols.equals(oid_distinctCount) || selectedCols.equals(oid_distinct))) {
			// Blank search, quick and easy
			return sql.select(tablename, selectedCols);
		}
		
		//----------------------------------------------------------------------
		// Sadly looks like things must be done the hardway, initialize the vars
		//----------------------------------------------------------------------
		
		// Query string, for either a newly constructed view, or cached view
		StringBuilder queryBuilder = new StringBuilder();
		
		// The actual args to use
		List<Object> complexQueryArgs = new ArrayList<Object>();
		Object[] queryArgs = null;
		
		// Result ordering by
		OrderBy<DataObject> orderByObj = null;
		
		// Building the Core_DataTypeMap from where request
		Map<String, Core_DataType> queryTypeMap = new HashMap<String, Core_DataType>();
		
		// The where clause query object, that is built, and actually used
		Query queryObj = null;
		
		//----------------------------------------------------------------------
		// Validating the Where clause and using it to build the Core_DataTypeMap
		// AND rebuild the query to the required formatting (lowercase)
		//----------------------------------------------------------------------
		
		// Where clause exists, build it!
		if (whereClause != null && whereClause.length() >= 0) {
			
			// Conversion to raw query object, round 1 of sanatizing
			//
			// To avoid confusion, the original query is called "RAW QUERY",
			// While the converted actual JSql query is called "PROCESSED QUERY"
			//---------------------------------------------------------------------
			queryObj = Query.build(whereClause, whereValues);
			
			// Build the query type map from the "raw query"
			//---------------------------------------------------------------------
			Map<String, List<Object>> queryMap = queryObj.keyValuesMap();
			for (String key : queryMap.keySet()) {
				Core_DataType subType = searchValueToCore_DataType(queryMap.get(key).get(0));
				if (subType != null) {
					queryTypeMap.put(key, subType);
				}
			}
			
			// Gets the original field to "raw query" maps
			// of keys, to do subtitution on,
			// and their respective argument map.
			Map<String, List<Query>> fieldQueryMap = queryObj.fieldQueryMap();
			Map<String, Object> queryArgMap = queryObj.queryArgumentsMap();
			
			// Gets the new index position to add new arguments if needed
			int newQueryArgsPos = queryArgMap.size() + 1;
			
			// Iterate the queryTypeMap, and doing the
			// required query expension / substitution
			//
			// Parses the where clause with query type map
			for (String key : queryTypeMap.keySet()) {
				
				// Get the "raw query" type that may require processing
				Core_DataType subType = queryTypeMap.get(key);
				
				// The query list to do processing on
				List<Query> toReplaceQueries = fieldQueryMap.get(key);
				
				// Skip if no query was found needed processing
				if (toReplaceQueries == null || toReplaceQueries.size() <= 0) {
					continue;
				}
				
				// For each string based search,
				// enforce and additional lowercase matching
				// for increased indexing perfromance.
				if (subType == Core_DataType.STRING) {
					
					// Iterate the queries to replace them
					//-------------------------------------------
					for (Query toReplace : toReplaceQueries) {
						
						// Get the argument name/idx used in the query
						String argName = toReplace.argumentName();
						
						// Get the lower case representation of query arguments
						String argLowerCase = queryArgMap.get(argName).toString();
						if (argLowerCase != null) {
							argLowerCase = argLowerCase.toString().toLowerCase();
						}
						
						// Store the lower case varient of the query
						queryArgMap.put("" + newQueryArgsPos, argLowerCase);
						
						// Add the new query with the lower case argument
						Query replacement = QueryFilter.basicQueryFromTokens(queryArgMap,
							toReplace.fieldName() + lowerCaseSuffix, toReplace.operatorSymbol(), ":"
								+ newQueryArgsPos);
						
						// Case sensitive varient, to consider support with #cs ?
						// Or perhaps a meta table configuration?
						//
						// replacement = new And(
						// 	replacement,
						// 	toReplace,
						// 	queryArgMap
						// );
						
						// Replaces old query with new query
						queryObj = queryObj.replaceQuery(toReplace, replacement);
						
						// Increment the argument count
						++newQueryArgsPos;
					}
					// End of query iteration replacement
					//-------------------------------------------
				}
			}
		}
		
		//----------------------------------------------------------------------
		// Order by clause handling
		//----------------------------------------------------------------------
		
		// Order by object handling, of oid keys
		// Replaces reserved "_oid" key with actual key
		if (orderByStr != null) {
			orderByStr.replaceAll("_oid", "oID");
			orderByObj = getOrderByObject(orderByStr);
		}
		
		// Order by handling for Core_DataType map
		if (orderByObj != null) {
			// Iterate each order by key name, and normalize them
			Set<String> orderByKeys = orderByObj.getKeyNames();
			for (String keyName : orderByKeys) {
				
				// Ignores oID keynames
				if (keyName.equalsIgnoreCase("oID")) {
					continue;
				}
				
				// Check if keyname is found within metamap
				// adds it if it wasnt previously added, assumes a string
				if (!queryTypeMap.containsKey(keyName)) {
					queryTypeMap.put(keyName, Core_DataType.STRING);
				}
				
				// Get the query type
				Core_DataType type = queryTypeMap.get(keyName);
				
				// Force lowercase string sorting for string orderby
				//
				// Case sensitive varient, to consider support with #cs ?
				// Or perhaps a meta table configuration?
				if (type == Core_DataType.STRING) {
					orderByObj.replaceKeyName(keyName, keyName + "#lc");
				}
			}
		}
		
		//----------------------------------------------------------------------
		// Finally putting all the query pieces together
		//----------------------------------------------------------------------
		
		// Building the Inner join query, via a complex query view
		// to actually query thee data against. This somewhat represents the actual table,
		// you would have quried against in traditional fixed SQL view
		StringBuilder innerJoinQuery = complexQueryView(sql, tablename, queryTypeMap,
			complexQueryArgs);
		
		// Building the complex inner join query
		//
		// Filters out DISTINCT support as it will probably be there only for OID
		// In which it would already be handled by the inner-join, creates buggy
		// result otherwise (to confirm?)
		queryBuilder.append("SELECT " + selectedCols.replaceAll("DISTINCT", "") + " FROM (");
		queryBuilder.append(innerJoinQuery);
		queryBuilder.append(") AS R");
		
		// WHERE query is built from queryObj, this acts as a form of sql sanitization
		if (queryObj != null) {
			queryBuilder.append(" WHERE ");
			queryBuilder.append(queryObj.toSqlString());
			complexQueryArgs.addAll(queryObj.queryArgumentsList());
		}
		
		// Order by string mapping
		if (orderByObj != null) {
			queryBuilder.append(" ORDER BY ");
			queryBuilder.append(orderByObj.toString());
		}
		
		//logger.log( Level.WARNING, queryBuilder.toString() );
		//logger.log( Level.WARNING, Arrays.asList(queryArgs).toString() );
		
		// Finalize query args
		queryArgs = complexQueryArgs.toArray(new Object[0]);
		
		// Limit and offset clause handling
		if (limit > 0) {
			queryBuilder.append(" LIMIT " + limit);
			if (offset > 0) {
				queryBuilder.append(" OFFSET " + offset);
			}
		}
		
		// // In case you want to debug the query =(
		// System.out.println(">>> "+queryBuilder.toString());
		// System.out.println(">>> "+ConvertJSON.fromList(complexQueryArgs));
		
		// // Dump and debug the table
		// System.out.println(">>> TABLE DUMP");
		// System.out.println( ConvertJSON.fromMap( sql.select(tablename).readRow(0) ) );
		
		// Execute and get the result
		return sql.query(queryBuilder.toString(), queryArgs);
	}
	
	/**
	 * Performs a search query, and returns the respective DataObjects GUID keys
	 *
	 * CURRENTLY: It is entirely dependent on the whereValues object type to perform the relevent search criteria
	 * @TODO: Performs the search pattern using the respective type map
	 *
	 * @param   DataObjectMap object to refrence from
	 * @param   JSql connection to use
	 * @param   JSql table name to use
	 * @param   where query statement
	 * @param   where clause values array
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max
	 *
	 * @return  The String[] array
	 **/
	public static String[] dataObjectMapQuery_id( //
		// The meta table / sql configs
		DataObjectMap dataObjectMapObj, JSql sql, String tablename, //
		// The actual query
		String whereClause, Object[] whereValues, String orderByStr, int offset, int limit //
	) { //
		JSqlResult r = runComplexQuery(
			//
			dataObjectMapObj, sql, tablename, "DISTINCT \"oID\"", whereClause, whereValues,
			orderByStr, offset, limit);
		List<Object> oID_list = r.getObjectList("oID");
		// Generate the object list
		if (oID_list != null) {
			return ListValueConv.objectListToStringArray(oID_list);
		}
		// Blank list as fallback
		return new String[0];
	}
	
	// /**
	//  * Performs a search query, and returns the respective DataObjects
	//  *
	//  * CURRENTLY: It is entirely dependent on the whereValues object type to perform the relevent search criteria
	//  * @TODO: Performs the search pattern using the respective type map
	//  *
	//  * @param   DataObjectMap object to refrence from
	//  * @param   JSql connection to use
	//  * @param   JSql table name to use
	//  * @param   where query statement
	//  * @param   where clause values array
	//  * @param   query string to sort the order by, use null to ignore
	//  * @param   offset of the result to display, use -1 to ignore
	//  * @param   number of objects to return max
	//  *
	//  * @return  The DataObject[] array
	//  **/
	// public static DataObject[] DataObjectMapQuery( //
	// 	// The meta table / sql configs
	// 	DataObjectMap dataObjectMapObj, JSql sql, String tablename, //
	// 	// The actual query
	// 	String whereClause, Object[] whereValues, String orderByStr, int offset, int limit //
	// ) { //
	// 	return dataObjectMapObj.getArrayFromID(
	// 		dataObjectMapQuery_id(dataObjectMapObj, sql, tablename, whereClause, whereValues, orderByStr,
	// 			offset, limit), true);
	// }
	
	/**
	 * Performs a search query, and returns the respective DataObjects
	 *
	 * CURRENTLY: It is entirely dependent on the whereValues object type to perform the relevent search criteria
	 * @TODO: Performs the search pattern using the respective type map
	 *
	 * @param   DataObjectMap object to refrence from
	 * @param   JSql connection to use
	 * @param   JSql table name to use
	 * @param   where query statement
	 * @param   where clause values array
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max
	 *
	 * @return  The DataObject[] array
	 **/
	public static long dataObjectMapCount( //
		//
		DataObjectMap dataObjectMapObj, JSql sql, String tablename, //
		//
		String whereClause, Object[] whereValues, String orderByStr, int offset, int limit //
	) { //
		JSqlResult r = runComplexQuery(dataObjectMapObj, sql, tablename,
			"COUNT(DISTINCT \"oID\") AS rcount", whereClause, whereValues, orderByStr, offset, limit);
		
		GenericConvertList<Object> rcountArr = r.get("rcount");
		// Generate the object list
		if (rcountArr != null && rcountArr.size() > 0) {
			return rcountArr.getLong(0);
		}
		// Blank as fallback
		return 0;
	}
	
}
