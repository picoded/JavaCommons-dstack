package picoded.dstack.core;

// Java imports
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// Picoded imports
import picoded.dstack.DataObject;
import picoded.dstack.DataObjectMap;
import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GUID;
import picoded.core.common.ObjectToken;

/**
 * Represents a single object node in the DataObjectMap collection.
 *
 * This is intended, to handle local, delta, and remote data seperately.
 * Where its data layers can be visualized in the following order
 *
 * + deltaDataMap                 - keeping track of local changes and removals
 * + remoteDataMap (incomplete)   - partial data map, used when only query data is needed
 * + remoteDataMap (complete)     - full data map, used when the incomplete map is insufficent
 *
 * NOTE: This class should not be initialized directly, but through DataObjectMap class
 **/
public class Core_DataObject implements DataObject {
	
	// Core variables
	//----------------------------------------------
	
	/**
	 * Core_DataObjectMap used for the object
	 * Used to provide the underlying backend implementation
	 **/
	protected Core_DataObjectMap mainTable = null;
	
	/**
	 * GUID used for the object
	 **/
	protected String _oid = null;
	
	/**
	 * Written changes, note that picoded.core.common.ObjectToken.NULL is used
	 * as a pesudo null value (remove operation)
	 **/
	protected Map<String, Object> deltaDataMap = new HashMap<String, Object>();
	
	/**
	 * Boolean Flag : Used to indicate if the full remoteDataMap is given.
	 * This is used when incomplete query data is given first
	 **/
	protected boolean isCompleteRemoteDataMap = false;
	
	/**
	 * Local data cache of what exists on the server.
	 * Is set to null if no remote data is loaded
	 **/
	protected Map<String, Object> remoteDataMap = null;
	
	// Constructor
	//----------------------------------------------
	
	/**
	 * Setup a DataObject against a DataObjectMap backend.
	 *
	 * This allow the setup in the following modes
	 *
	 * + No (or invalid) GUID : Assume a new DataObject is made with NO DATA. Issues a new GUID for the object
	 * + GUID without remote data, will pull the required data when required
	 * + GUID with complete remote data
	 * + GUID with incomplete remote data, will pull the required data when required
	 *
	 * @param  Meta table to use
	 * @param  GUID to use, can be null
	 * @param  Remote mapping data used (this should be modifiable)
	 * @param  is complete remote map, use false if incomplete data
	 **/
	public Core_DataObject(DataObjectMap inTable, String inOID, Map<String, Object> inRemoteData,
		boolean isCompleteData) {
		// Main table to use
		mainTable = (Core_DataObjectMap) inTable;
		
		// Generates a GUID if not given
		if (inOID == null) {
			// Issue a GUID
			if (_oid == null) {
				_oid = GUID.base58();
			}

			if(_oid.length() < 4) {
				throw new RuntimeException("_oid should be atleast 4 character long");
			}
			
			// // D= GUID collision check for LOLZ
			// remoteDataMap = mainTable.DataObjectRemoteDataMap_get(_oid);
			// if (remoteDataMap.size() > 0) {
			// 	throw new SecurityException("GUID Collision =.= DO YOU HAVE REAL ENTROPY? : " + _oid);
			// }
			
			// Ensure oid is savable, needed to save blank objects
			deltaDataMap.put("_oid", _oid);
			deltaDataMap.put("_createTimestamp", System.currentTimeMillis());
			
			// Create remote data map (blank)
			remoteDataMap = new HashMap<String, Object>();
			
			// Indicated that the provided map is "complete"
			isCompleteRemoteDataMap = true;
			
		} else {
			// _oid setup
			_oid = inOID;
			
			// Loading remote data map, only valid if _oid is given
			remoteDataMap = inRemoteData;
			
			// Indicates that isComplete is flagged if given
			if (remoteDataMap != null) {
				isCompleteRemoteDataMap = isCompleteData;
			}
		}
	}
	
	/**
	 * Constructor, with DataObjectMap and GUID (auto generated if null)
	 * This is the shorten version of the larger constructor
	 *
	 * @param  Meta table to use
	 * @param  GUID to use, can be null
	 **/
	public Core_DataObject(DataObjectMap inTable, String inOID) {
		this(inTable, inOID, null, false);
	}
	
	// DataObject ID
	//----------------------------------------------
	
	/**
	 * The object ID
	 **/
	@Override
	public String _oid() {
		return _oid;
	}
	
	// Utiltiy functions
	//----------------------------------------------
	
	/**
	 * Ensures the complete remote data map is loaded.
	 *
	 * This triggers if either the remote data is not flagged
	 * as complete, or is null, and calls from the main DataObjectMap
	 * via "DataObjectRemoteDataMap_get"
	 *
	 * This is mainly used when a get fails opportunistially with delta
	 * or incomplete data, and requires the actual "true" data.
	 **/
	protected void ensureCompleteRemoteDataMap() {
		if (remoteDataMap == null || !isCompleteRemoteDataMap) {
			remoteDataMap = mainTable.DataObjectRemoteDataMap_get(_oid);
			if (remoteDataMap == null) {
				remoteDataMap = new HashMap<String, Object>();
			}
			isCompleteRemoteDataMap = true;
		}
	}
	
	/**
	 * The aggressive type conversion to int / double types, if possible
	 *
	 * This is done to help ensure numeric strings are converted into number
	 * values in which they perform better via lookups
	 *
	 * @param The input object to convert
	 *
	 * @return The converted Integer or Double object, else its original value
	 **/
	protected Object agressiveNumericConversion(Object value) {
		if (value == null) {
			return value;
		}
		
		// Ignore byte[] array type conversion
		if (value instanceof byte[]) {
			return value;
		}
		
		Object ret = null;
		String strValue = value.toString();
		
		// Tries to convert to INT if possible
		try {
			ret = Integer.valueOf(strValue);
		} catch (Exception e) {
			// Silent ignore
		}
		
		// Tries to convert to LONG if possible
		try {
			if (ret == null) {
				ret = Long.parseLong(strValue);
			}
		} catch (Exception e) {
			// Silent ignore
		}
		
		// Tries to convert to double if possible
		try {
			if (ret == null) {
				ret = Double.parseDouble(strValue);
			}
		} catch (Exception e) {
			// Silent ignore
		}
		
		// Drop if accuracy or content is lost in the conversion process
		if (ret != null && ret.toString().equals(strValue)) {
			return ret;
		}
		return value;
	}
	
	// DataObject save operations
	//----------------------------------------------
	
	/**
	 * Collapse delta map to remote map, but DOES NOT actually update
	 * the remote data map inside the storage backend, this is done by the
	 * save delta / save all functionality.
	 *
	 * Seriously do not use this except in those cases
	 **/
	protected void collapseDeltaToRemoteMap() {
		ensureCompleteRemoteDataMap();
		for (Entry<String, Object> entry : deltaDataMap.entrySet()) {
			Object val = deltaDataMap.get(entry.getKey());
			if (val == null || val.equals(ObjectToken.NULL)) {
				remoteDataMap.remove(entry.getKey());
			} else {
				remoteDataMap.put(entry.getKey(), val);
			}
		}
		deltaDataMap = new HashMap<String, Object>();
	}
	
	/**
	 * Save the delta changes to storage. This only push the changes made to the server
	 * as such on heavy usage, it may create write sequence errors
	 **/
	@Override
	public void saveDelta() {
		// Get the delta change set
		Set<String> deltaKeySet = deltaDataMap.keySet();

		// Lets perform the save ONLY if changes are detected
		if( deltaKeySet.size() > 0 ) {

			// Add update timestamp
			deltaDataMap.put("_updateTimestamp", System.currentTimeMillis());

			// Lets sync up all the data !
			ensureCompleteRemoteDataMap();
			mainTable.DataObjectRemoteDataMap_update(_oid, this, deltaKeySet);

			// Clear up the delta object, after sync
			collapseDeltaToRemoteMap();
		}
	}
	
	/**
	 * Save all the configured data, ignore delta handling. This helps ensure all data
	 * is written in a single session for consistency.
	 *
	 * In some implementation, this maybe the only way.
	 **/
	@Override
	public void saveAll() {

		// Add update timestamp
		deltaDataMap.put("_updateTimestamp", System.currentTimeMillis());

		// Lets sync up all the data !
		ensureCompleteRemoteDataMap();
		Set<String> keySet = new HashSet<String>(deltaDataMap.keySet());
		keySet.addAll(remoteDataMap.keySet());
		mainTable.DataObjectRemoteDataMap_update(_oid, this, keySet);
		
		// Clear up the delta object, after sync
		collapseDeltaToRemoteMap();
	}
	
	// Critical map functions
	//----------------------------------------------
	
	/**
	 * Gets and return the requested current value
	 *
	 * @param   key to use
	 *
	 * @return  Value if present, NULL if fail
	 **/
	@Override
	public Object get(Object key) {
		
		// Get key operation : Special case
		if ("_oid".equalsIgnoreCase(key.toString())) {
			return _oid;
		}
		
		// Get from delta changes, if exists
		Object ret = deltaDataMap.get(key);
		
		// Get from incomplete map, if it exists
		// This helps optimize calls done after a "query"
		if (ret == null && remoteDataMap != null && !isCompleteRemoteDataMap) {
			ret = remoteDataMap.get(key);
		}
		
		// Ensure the remote map is completed and fetch its result
		if (ret == null) {
			ensureCompleteRemoteDataMap();
			ret = remoteDataMap.get(key);
		}
		
		// Return null value
		if (ret == null || ret.equals(ObjectToken.NULL)) {
			return null;
		}
		
		// Returns valid value
		return Core_DataObjectMap.deepCopy( ret );
	}
	
	/**
	 * Put and set its delta value, set null is considered "remove"
	 *
	 * @param  key to use
	 * @param  Value to store, does conversion to numeric if possible
	 *
	 * @return The previous value
	 **/
	@Override
	public Object put(String key, Object value) {
		
		// // _oid overwriting handling
		// if ("_oid".equalsIgnoreCase(key.toString())) {
		// 	// Silently ignore changes, and return existing value
		// 	return _oid;
		// }
		
		// Silently ignore reserved _underscore namespace data
		if(key.toString().substring(0,1).equalsIgnoreCase("_")) {
			return get(key);
		}

		// Get the previous value
		Object ret = get(key);
		
		// Aggressive numeric conversion
		value = agressiveNumericConversion(value);
		
		// If value and ret is both null, return
		if (value == null && ret == null) {
			return null;
		}
		
		// If no values are changed, ignore and return
		if (value != null && value.equals(ret)) {
			return ret;
		}
		
		// Check for change
		if (value == null) {
			// Put a NULL ObjectToken, if its a "delete"
			deltaDataMap.put(key, ObjectToken.NULL);
		} else {
			// Put the new value
			deltaDataMap.put(key, value);
		}
		
		return ret;
	}
	
	/**
	 * Remove operation
	 *
	 * @return  The old value if it exists
	 **/
	@Override
	public Object remove(Object key) {
		return put(key.toString(), null);
	}
	
	/**
	 * Raw keyset, that is unfiltered
	 * Filtering is needed to ensure, no null values are used
	 *
	 * @return  Unfiltered set of key names
	 **/
	protected Set<String> unfilteredForNullKeySet() {
		Set<String> unfilteredForNull = new HashSet<String>();
		ensureCompleteRemoteDataMap();
		unfilteredForNull.addAll(deltaDataMap.keySet());
		unfilteredForNull.addAll(remoteDataMap.keySet());
		return unfilteredForNull;
	}
	
	/**
	 * Gets and return valid keySet()
	 **/
	@Override
	public Set<String> keySet() {
		// Get unfilttered key
		Set<String> unfilteredForNull = unfilteredForNullKeySet();
		// Prepare result
		Set<String> retSet = new HashSet<String>();
		
		// Iterate the set
		for (String key : unfilteredForNull) {
			if ("_oid".equalsIgnoreCase(key)) {
				continue;
			}
			
			// List final set ONLY, if it has value
			if (get(key) != null) {
				retSet.add(key);
			}
		}
		
		// Add _oid a special field
		retSet.add("_oid");
		
		// Return the value.
		return retSet;
	}
	
	// To string operation : aids debugging
	//----------------------------------------------
	
	/**
	 * Converts the map into a string, via JSON format. Which is to aid debugging.
	 **/
	@Override
	public String toString() {
		return ConvertJSON.fromMap(this);
	}
}
