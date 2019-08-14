package picoded.dstack.jsql;

import picoded.core.conv.GenericConvert;
import picoded.core.conv.ListValueConv;
import picoded.core.struct.MutablePair;
import picoded.dstack.KeyLong;
import picoded.dstack.core.Core_KeyLongMap;
import picoded.dstack.connector.jsql.JSql;
import picoded.dstack.connector.jsql.JSqlException;
import picoded.dstack.connector.jsql.JSqlResult;

import java.util.HashSet;
import java.util.Set;

public class JSql_KeyLongMap extends Core_KeyLongMap {
	
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
	protected String keyLongMapName = null;
	
	/**
	 * JSql setup
	 *
	 * @param   inJSql JSQL connection
	 * @param   tablename Table name to use
	 **/
	public JSql_KeyLongMap(JSql inJSql, String tablename) {
		super();
		sqlObj = inJSql;
		keyLongMapName = "KL_" + tablename;
	}
	
	//--------------------------------------------------------------------------
	//
	// Internal config vars
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
	protected String valueColumnType = "DECIMAL(36,12)";
	
	//--------------------------------------------------------------------------
	//
	// raw put & get, meant to be actually implemented.
	// [Internal use, to be extended in future implementation]
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Sets the value, with validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key
	 * @param value, null means removal
	 * @param expire timestamp, 0 means no timestamp
	 *
	 * @return null
	 **/
	public Long setValueRaw(String key, Long value, long expire) {
		long now = System.currentTimeMillis();
		
		// Null values are returned and not added to the database
		if (value == null) {
			return null;
		}
		
		try {
			sqlObj.upsert( //
				keyLongMapName, //
				new String[] { "kID" }, //unique cols
				new Object[] { key }, //unique value
				//
				new String[] { "uTm", "eTm", "kVl" }, //insert cols
				new Object[] { now, expire, value.longValue() }, //insert values
				// 
				new String[] { "cTm" }, //default cols
				new Object[] { now }, //default values
				null // misc values
				);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return null;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Sets the expire time stamp value, raw without validation
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param expire timestamp in seconds, 0 means NO expire
	 *
	 * @return long
	 **/
	public void setExpiryRaw(String key, long expire) {
		sqlObj.update("UPDATE " + keyLongMapName + " SET eTm=? WHERE kID=?", expire, key);
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Returns the value and expiry, with validation against the current timestamp
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key as String
	 * @param now timestamp, 0 = no timestamp so skip timestamp checks
	 *
	 * @return Long value, and expiry pair
	 **/
	public MutablePair<Long, Long> getValueExpiryRaw(String key, long now) {
		// Search for the key
		JSqlResult r = sqlObj.select(keyLongMapName, "*", "kID = ?", new Object[] { key });
		long expiry = getExpiryRaw(r);
		
		if (expiry != 0 && expiry < now) {
			return null;
		}
		
		// Check for null objects
		Object longObj = r.get("kVl").get(0);
		if (longObj == null) {
			return null;
		}
		
		Long longVal = GenericConvert.toLong(longObj);
		if (longVal == null) {
			return null;
		}
		
		// Return long value
		return new MutablePair<Long, Long>(longVal, new Long(expiry));
	}
	
	//--------------------------------------------------------------------------
	//
	// Incremental operations
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Stores (and overwrites if needed) key, value pair
	 *
	 * Important note: It does not return the previously stored value
	 *
	 * @param key as String
	 * @param expect as Long
	 * @param update as Long
	 *
	 * @return true if successful
	 **/
	public boolean weakCompareAndSet(String key, Long expect, Long update) {
		// now timestamp
		long now = System.currentTimeMillis();
		
		// Potentially a new upsert, ensure there is something to "update" atleast
		// initializing an empty row if it does not exist
		if (expect == null || expect == 0l) {
			// Expect is now atleast 0
			expect = 0l;

			//
			// Attempt to do an update first if record already exist
			//
			// this is intentionally done, to guard against deletion of expired records
			// between the "failed" insert command and the final update attempt, when 
			// an existing record existed.
			//
			if( sqlObj.update("UPDATE " + keyLongMapName
				+ " SET kVl=?, uTm=?, eTm=? WHERE kID = ? AND (kVl = ? OR (eTm > ? AND eTm < ?))", update, now, 0l, key, expect, 0l, now) > 0 ) {
				return true;
			}

			// Does an insert (if record does not exist)
			try {
				// UPSERT EQUIVALENT
				//--------------------------
				// sqlObj.upsert( //
				// 	keyLongMapName, // unique key
				// 	new String[] { "kID" }, //unique cols
				// 	new Object[] { key }, //unique value
				// 	// insert (ignore)
				// 	null, null,
				// 	// default value
				// 	new String[] { "uTm", "cTm", "eTm", "kVl" }, //insert cols
				// 	new Object[] { now, now, 0l, 0l }, //insert values
				// 	// misc (ignore)
				// 	null);

				// INSERT EQUIVALENT
				//--------------------------

				// Immediately does an insert, this will intentionally fail if a record exists.
				if (sqlObj.update("INSERT INTO "+keyLongMapName+" (kID, uTm, cTm, eTm, kVl) VALUES (?, ?, ?, ?, ?)", key, now, now, 0l, update) > 0) {
					return true;
				}
			} catch (JSqlException e) {
				// silenced exception, if value already exists,
				// the update call will work anyway
			}
		}
		
		// Does the update from an existing
		// or on an expired record
		return sqlObj.update("UPDATE " + keyLongMapName
			+ " SET kVl=?, uTm=?, eTm=? WHERE kID = ? AND (kVl = ? OR (eTm > ? AND eTm < ?))", update, now, 0l, key, expect, 0l, now) > 0;
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
		try {
			// Table constructor
			//-------------------
			sqlObj.createTable( //
				keyLongMapName, //
				new String[] { //
				// Primary key, as classic int, this is used to lower SQL
				// fragmentation level, and index memory usage. And is not accessible.
				// Sharding and uniqueness of system is still maintained by meta keys
					"pKy", // primary key
					// Time stamps
					"uTm", //Updated timestamp
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
					tStampColumnType, tStampColumnType, tStampColumnType,
					// Storage keys
					keyColumnType, //
					// Value storage
					valueColumnType } //
				);
			
			// Unique index
			//------------------------------------------------
			sqlObj.createIndex( //
				keyLongMapName, "kID", "UNIQUE", "unq" //
			);
			
			// Value search index
			//------------------------------------------------
			sqlObj.createIndex( //
				keyLongMapName, "kVl", null, "valMap" //
			);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	@Override
	public void systemDestroy() {
		sqlObj.dropTable(keyLongMapName);
	}
	
	/**
	 * Perform maintenance, this is meant for large maintenance jobs.
	 * Such as weekly or monthly compaction. It may or may not be a long
	 * running task, where its use case is backend specific
	 **/
	@Override
	public void maintenance() {
		long currentTime = System.currentTimeMillis();
		sqlObj.delete( //
			keyLongMapName, //
			"eTm <= ? AND eTm > ?", //
			new Object[] { currentTime, 0 });
	}
	
	@Override
	public Set<String> keySet(Long value) {
		long now = System.currentTimeMillis();
		JSqlResult r = null;
		if (value == null) {
			r = sqlObj.select(keyLongMapName, "kID", "eTm <= ? OR eTm > ?", new Object[] { 0, now });
		} else {
			r = sqlObj.select(keyLongMapName, "kID", "kVl = ? AND (eTm <= ? OR eTm > ?)",
				new Object[] { value.longValue(), 0, now });
		}
		
		if (r == null || r.get("kID") == null) {
			return new HashSet<String>();
		}
		
		// Gets the various key names as a set
		return ListValueConv.toStringSet(r.getObjectList("kID", "[]"));
	}
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		sqlObj.delete(keyLongMapName);
	}
	
	/**
	 * Remove the value, given the key
	 *
	 * @param key param find the thae meta key
	 *
	 * @return  null
	 **/
	@Override
	public KeyLong remove(Object key) {
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
	public Long removeValue(Object key) {
		if (key == null) {
			throw new IllegalArgumentException("delete 'key' cannot be null");
		}
		String keyStr = key.toString();
		sqlObj.delete(keyLongMapName, "kID = ?", new Object[] { keyStr });
		return null;
	}
	
	//--------------------------------------------------------------------------
	//
	// Expiration and lifespan handling (core)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Gets the expire time from the JSqlResult
	 **/
	protected long getExpiryRaw(JSqlResult r) throws JSqlException {
		// Search for the key
		Object rawTime = null;
		
		// Has value
		if (r != null && r.rowCount() > 0) {
			rawTime = r.get("eTm").get(0);
		} else {
			return -1; //No value (-1)
		}
		
		// 0 represents expired value
		long ret = 0;
		if (rawTime != null) {
			if (rawTime instanceof Number) {
				ret = ((Number) rawTime).longValue();
			} else {
				ret = Long.parseLong(rawTime.toString());
			}
		}
		
		if (ret <= 0) {
			return 0;
		} else {
			return ret;
		}
	}
}
