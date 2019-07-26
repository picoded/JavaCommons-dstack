package picoded.dstack.jsql;

import java.util.HashSet;
import java.util.Set;

import picoded.dstack.KeyValue;
import picoded.dstack.core.Core_KeyValueMap;
import picoded.dstack.connector.jsql.*;
import picoded.core.conv.ListValueConv;
import picoded.core.struct.MutablePair;

/**
 * Reference implementation of KeyValueMap data structure.
 * This is done via a minimal implementation via internal data structures.
 *
 * Built ontop of the Core_KeyValueMap implementation.
 **/
public class JSql_KeyValueMap extends Core_KeyValueMap {
	
	//--------------------------------------------------------------------------
	//
	// Constructor vars
	//
	//--------------------------------------------------------------------------
	
	/**
	 * The inner sql object
	 **/
	protected JSql sqlObj = null;
	
	/**
	 * The tablename for the key value pair map
	 **/
	protected String sqlTableName = null;
	
	/**
	 * [internal use] JSql setup with a SQL connection and tablename
	 **/
	public JSql_KeyValueMap(JSql inJSql, String tablename) {
		super();
		sqlTableName = "KV_" + tablename;
		sqlObj = inJSql;
	}
	
	//--------------------------------------------------------------------------
	//
	// Fundemental set/get value (core)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the value, with validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key
	 * @param value, null means removal
	 * @param expire timestamp, 0 means not timestamp
	 *
	 * @return null
	 **/
	public String setValueRaw(String key, String value, long expire) {
		long now = System.currentTimeMillis();
		sqlObj.upsert( //
			sqlTableName, //
			new String[] { "kID" }, //unique cols
			new Object[] { key }, //unique value
			//
			new String[] { "cTm", "eTm", "kVl" }, //insert cols
			new Object[] { now, expire, value } //insert values
			);
		return null;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * 
	 * Returns the value and expiry, with validation against the current timestamp
	 * 
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param now timestamp
	 *
	 * @return String value
	 **/
	public MutablePair<String, Long> getValueExpiryRaw(String key, long now) {
		// Search for the key
		JSqlResult r = sqlObj.select(sqlTableName, "*", "kID=?", new Object[] { key });
		long expiry = fetchExpiryRaw(r);
		
		// No valid value found , return null
		if (expiry < 0) {
			return null;
		}
		
		// Expired value, return null
		if (expiry != 0 && expiry < now) {
			return null;
		}
		
		// Check for null objects
		Object strObj = r.get("kVl").get(0);
		if (strObj == null) {
			return null;
		}
		
		String val = strObj.toString();
		if (val.isEmpty()) {
			return null;
		}
		
		// Get the value, and return the pair
		return new MutablePair<String, Long>(val, expiry);
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Gets the expire time from the JSqlResult
	 * 
	 * @return -2 : represents no record found, -1 represents expired
	 **/
	public long fetchExpiryRaw(JSqlResult r) throws JSqlException {
		// Search for the key
		Object rawTime = null;
		
		// Get the rawTime object only if valid value is found
		if (r != null && r.rowCount() > 0) {
			rawTime = r.get("eTm").get(0);
		} else {
			return -2; //No value (-2)
		}
		
		// Return valid rawTime value
		if (rawTime != null) {
			if (rawTime instanceof Number) {
				return ((Number) rawTime).longValue();
			} else {
				return Long.parseLong(rawTime.toString());
			}
		}
		
		// No value found, return 0
		return 0;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the expire time stamp value, raw without validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param expire timestamp in seconds, 0 means NO expire
	 *
	 * @return long
	 **/
	public void setExpiryRaw(String key, long time) {
		sqlObj.update("UPDATE " + sqlTableName + " SET eTm=? WHERE kID=?", time, key);
	}
	
	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown / maintenance (DStackCommon)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Primary key type
	 **/
	protected String pKeyColumnType = "BIGINT PRIMARY KEY AUTOINCREMENT";
	
	/**
	 * Timestamp field type
	 **/
	protected String tStampColumnType = "BIGINT";
	
	/**
	 * Key name field type
	 **/
	protected String keyColumnType = "VARCHAR(64)";
	
	/**
	 * Value field type
	 **/
	protected String valueColumnType = "VARCHAR(MAX)";
	
	/**
	 * Setsup the backend storage table, etc. If needed
	 **/
	public void systemSetup() {
		// Table constructor
		//-------------------
		sqlObj.createTable( //
			sqlTableName, //
			new String[] { //
			// Primary key, as classic int, this is used to lower SQL
			// fragmentation level, and index memory usage. And is not accessible.
			// Sharding and uniqueness of system is still maintained by meta keys
				"pKy", //
				// Time stamps
				"cTm", //value created time
				"eTm", //value expire time
				// Storage keys
				"kID", //
				// Value storage
				"kVl" //
			}, //
			new String[] { //
			pKeyColumnType, //Primary key
				// Time stamps
				tStampColumnType, tStampColumnType,
				// Storage keys
				keyColumnType, //
				// Value storage
				valueColumnType //
			} //
			);
		
		// Unique index
		//------------------------------------------------
		sqlObj.createIndex( //
			sqlTableName, "kID", "UNIQUE", "unq" //
		);
		
		// Value search index
		//------------------------------------------------
		if (sqlObj.sqlType() == JSqlType.MYSQL) {
			sqlObj.createIndex( //
				// kVl(190) is chosen, as mysql "standard prefix limitation" is 767
				// as a result, with mb4 where 4 byte represents a character. 767/4 = 191
				sqlTableName, "kVl(191)", null, "valMap" //
			);
		} else {
			sqlObj.createIndex( //
				sqlTableName, "kVl", null, "valMap" //
			);
		}
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	public void systemDestroy() {
		sqlObj.dropTable(sqlTableName);
	}
	
	/**
	 * Perform maintenance, mainly removing of expired data if applicable
	 **/
	public void maintenance() {
		long currentTime = System.currentTimeMillis();
		sqlObj.delete( //
			sqlTableName, //
			"eTm <= ? AND eTm > ?", //
			new Object[] { currentTime, 0 });
	}
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		sqlObj.delete(sqlTableName);
	}
	
	//--------------------------------------------------------------------------
	//
	// SQL specific KeySet / remove optimization
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Search using the value, all the relevent key mappings
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key, note that null matches ALL
	 *
	 * @return array of keys
	 **/
	@Override
	public Set<String> keySet(String value) {
		long now = System.currentTimeMillis();
		JSqlResult r = null;
		if (value == null) {
			r = sqlObj.select(sqlTableName, "kID", "eTm <= ? OR eTm > ?", new Object[] { 0, now });
		} else {
			r = sqlObj.select(sqlTableName, "kID", "kVl = ? AND (eTm <= ? OR eTm > ?)", new Object[] {
				value, 0, now });
		}
		
		if (r == null || r.get("kID") == null) {
			return new HashSet<String>();
		}
		
		// Gets the various key names as a set
		return ListValueConv.toStringSet(r.getObjectList("kID", "[]"));
	}
	
	/**
	 * Remove the value, given the key
	 *
	 * @param key param find the thae meta key
	 *
	 * @return  null
	 **/
	@Override
	public KeyValue remove(Object key) {
		removeValue(key);
		return null;
	}
	
	/**
	 * Remove the value, given the key
	 *
	 * Important note: It does not return the previously stored value
	 * Its return String type is to maintain consistency with Map interfaces
	 *
	 * @param key param find the thae meta key
	 *
	 * @return  null
	 **/
	@Override
	public String removeValue(Object key) {
		if (key == null) {
			throw new IllegalArgumentException("delete 'key' cannot be null");
		}
		String keyStr = key.toString();
		sqlObj.delete(sqlTableName, "kID = ?", new Object[] { keyStr });
		return null;
	}
	
}
