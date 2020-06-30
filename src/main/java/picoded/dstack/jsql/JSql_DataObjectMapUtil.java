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
	 * 
	 * 2000 / 11 = 181
	 * Rounded down to 175 to have a healthy buffer
	 */
	public static final int jSqlObjectBatchSize = 175;
	
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
	
}
