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
		
		//
		// In general there are the following compare and set scenerios to handle
		//
		// 1) expecting value is 0
		//   a) existing record is expired
		//   b) existing record does not exist
		//   c) existing record is NOT expired, and is 0
		// 2) expecting value is non-zero
		//   a) existing record is NOT expired, and is expected value.
		//
		
		// Potentially a new upsert, ensure there is something to "update" atleast
		// initializing an empty row if it does not exist
		if (expect == null || expect == 0l) {
			// Expect is now atleast 0
			expect = 0l;
			
			//
			// Attempt to do an update first if record an expired record exists.
			//
			// this is intentionally done, before insert, to guard against deletion of expired records
			// between the "failed" insert command and the final update attempt, when an existing record existed.
			//
			// This handles
			// 1.a) expecting value is 0, existing record is expired
			//
			if (sqlObj.update("UPDATE " + keyLongMapName
				+ " SET kVl=?, uTm=?, eTm=? WHERE kID = ? AND (eTm > ? AND eTm < ?)", update, now, 0l,
				key, 0l, now) > 0) {
				return true;
			}
			
			//
			// Insert command
			// 
			// This handles
			// 1.b) expecting value is 0, existing record does not exist
			// 
			
			// Immediately does an insert, this will intentionally fail if a record exists.
			try {
				if (sqlObj.update("INSERT INTO " + keyLongMapName
					+ " (kID, uTm, cTm, eTm, kVl) VALUES (?, ?, ?, ?, ?)", key, now, now, 0l, update) > 0) {
					return true;
				}
			} catch (Exception e) {
				// intentionally ignored
				// @TODO: be more specific of what exception is being thrown
				// Currently this occurs when there are duplicate keys in the database and it 
				// throws SQLIntegrityConstraintViolationException
			}
		}
		
		//
		// Does the update from an existing, that is not expired
		//
		// This handles
		// 1.c) expecting value is 0, existing record is NOT expired, and is 0
		// 2.a) expecting value is non-zero, existing record is NOT expired, and is expected value.
		//
		return sqlObj.update("UPDATE " + keyLongMapName
			+ " SET kVl=?, uTm=? WHERE kID = ? AND kVl = ? AND (eTm <= ? OR eTm > ?)", update, now,
			key, expect, 0l, now) > 0;
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
