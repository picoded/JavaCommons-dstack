package picoded.dstack.hazelcast;

import java.util.ArrayList;
import java.util.Arrays;
// Java imports
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Picoded imports
import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GenericConvert;
import picoded.core.struct.query.Query;
import picoded.core.common.ObjectToken;
import picoded.dstack.*;
import picoded.dstack.core.*;

// Hazelcast implementation
import com.hazelcast.core.*;
import com.hazelcast.config.*;
import com.hazelcast.map.eviction.LRUEvictionPolicy;
import com.hazelcast.query.SqlPredicate;

/**
 * Hazelcast implementation of DataObjectMap data structure.
 *
 * Built ontop of the Core_DataObjectMap_struct implementation.
 **/
public class Hazelcast_DataObjectMap extends Core_DataObjectMap_struct {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/** HazelcastInstance representing the backend connection */
	HazelcastInstance hazelcast = null;
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param  hazelcast instance to perform operations on
	 * @param  name      of data object map to use
	 */
	public Hazelcast_DataObjectMap(HazelcastInstance inHazelcast, String name) {
		super();
		hazelcast = inHazelcast;
		configMap().put("name", name);
	}
	
	//--------------------------------------------------------------------------
	//
	// map naming support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * @return name memoizer
	 */
	private String _name = null;
	
	/**
	 * @return Get the internal map name, required to be in configMap
	 */
	private String name() {
		// Return memorized name
		if (_name != null) {
			return _name;
		}
		
		// Attempt to load cachename from config
		_name = configMap().getString("name");
		if (_name == null || _name.equals("")) {
			throw new IllegalArgumentException("Missing name configuration");
		}
		
		// Return config cachename
		return _name;
	}
	
	//--------------------------------------------------------------------------
	//
	// custom hashmap class for hazelcast
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Custom Comparable hazelcast map used for data storage 
	 */
	static public class HazelcastStorageMap extends HashMap<String, Object> implements
		Comparable<HazelcastStorageMap> {
		// Disagree on why this is needed, but required for serialization support =|
		static final long serialVersionUID = 1L;
		
		/** 
		 * @param o HazelcastStorageMap to compare against
		 * @return comparable by _oid property 
		 **/
		public int compareTo(HazelcastStorageMap o) {
			String this_oid = GenericConvert.toString(this.get("_oid"), "");
			String obj_oid = GenericConvert.toString(o.get("_oid"), "");
			return this_oid.compareTo(obj_oid);
		}
	}
	
	@Override
	public Map<String, Object> _newBlankStorageMap() {
		return new HazelcastStorageMap();
	}
	
	//--------------------------------------------------------------------------
	//
	// backend map accessor
	//
	//--------------------------------------------------------------------------
	
	/**
	 * @return backendmap memoizer
	 */
	private IMap<String, Map<String, Object>> _backendIMap = null;
	
	/**
	 * @return Storage map used for the backend operations of one "DataObjectMap"
	 *         identical to valueMap, made to be compliant with Core_DataObjectMap_struct
	 */
	protected IMap<String, Map<String, Object>> backendIMap() {
		if (_backendIMap != null) {
			return _backendIMap;
		}
		_backendIMap = hazelcast.getMap(name());
		return _backendIMap;
	}
	
	/**
	 * @return Storage map used for the backend operations of one "DataObjectMap"
	 *         identical to valueMap, made to be compliant with Core_DataObjectMap_struct
	 */
	protected Map<String, Map<String, Object>> backendMap() {
		return (Map<String, Map<String, Object>>) backendIMap();
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
		// Setup the map config
		MapConfig mConfig = new MapConfig(name());
		
		// Setup name, backup and async backup count
		mConfig.setName(name());
		mConfig.setBackupCount(configMap().getInt("backupCount", 2));
		mConfig.setAsyncBackupCount(configMap().getInt("asyncBackupCount", 0));
		
		// Enable or disable readBackupData, default is true IF asyncBackupCount == 0
		mConfig.setReadBackupData( //
			configMap().getBoolean("readBackupData", //
				configMap().getInt("asyncBackupCount", 0) == 0 //
				) //
			); //
		
		// Configure max size policy percentage to JVM heap
		MaxSizeConfig maxSize = new MaxSizeConfig( //
			configMap.getInt("freeHeapPercentage", 10), //
			MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE //
		); //
		mConfig.setMaxSizeConfig(maxSize);
		
		// Set LRU eviction policy
		mConfig.setMapEvictionPolicy(new LRUEvictionPolicy());
		
		// Enable query index for specific fields
		String[] indexArray = configMap().getStringArray("index", "[]");
		for (String indexName : indexArray) {
			mConfig.addMapIndexConfig(new MapIndexConfig(indexName, true));
		}
		
		// and apply it to the instance
		// see : https://docs.hazelcast.org/docs/latest-development/manual/html/Understanding_Configuration/Dynamically_Adding_Configuration_on_a_Cluster.html
		hazelcast.getConfig().addMapConfig(mConfig);
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	@Override
	public void systemDestroy() {
		// Since we do not have a proper map remove command,
		// the closest equivalent is to "clear"
		backendMap().clear();
	}
	
	//--------------------------------------------------------------------------
	//
	// Query functions
	//
	// ON HOLD until : https://github.com/hazelcast/hazelcast/pull/12708
	// is fully implemented
	//
	//--------------------------------------------------------------------------
	
	// /**
	//  * Converts a conv.Query into a full SQL string
	//  **/
	// protected String queryStringify(Query queryClause) {
		
	// 	// Converts into SQL string with ? value clause, and its arguments value
	// 	String sqlString = queryClause.toSqlString();
	// 	Object[] sqlArgs = queryClause.queryArgumentsArray();
		
	// 	// Fix up sql string, to be hazelcast compatible instead
	// 	sqlString = sqlString.replaceAll("\"(.*)\" (.*) \\?", "this[\'$1\'] $2 ?");
		
	// 	// if (sqlString != null) {
	// 	// 	throw new RuntimeException(sqlString);
	// 	// }
		
	// 	// Iterate each sql argument
	// 	for (int i = 0; i < sqlArgs.length; ++i) {
	// 		// sql argument
	// 		Object arg = sqlArgs[i];
			
	// 		// Support ONLY either null, string, or number types as of now
	// 		if (arg == null) {
	// 			sqlString = sqlString.replaceFirst("\\?", "null");
	// 		} else if (arg instanceof Number) {
	// 			sqlString = sqlString.replaceFirst("\\?", arg.toString());
	// 		} else if (arg instanceof String) {
	// 			sqlString = sqlString.replaceFirst("\\?", "\''"
	// 				+ arg.toString().replaceAll("\'", "\\'") + "\''");
	// 		} else {
	// 			throw new IllegalArgumentException("Unsupported query argument type : "
	// 				+ arg.getClass().getName());
	// 		}
	// 	}
		
	// 	// The processed SQL string
	// 	return sqlString;
	// }
	
	// /**
	//  * Performs a search query, and returns the respective DataObject keys.
	//  *
	//  * This is the GUID key varient of query, this is critical for stack lookup
	//  *
	//  * @param   queryClause, of where query statement and value
	//  * @param   orderByStr string to sort the order by, use null to ignore
	//  * @param   offset of the result to display, use -1 to ignore
	//  * @param   number of objects to return max, use -1 to ignore
	//  *
	//  * @return  The String[] array
	//  **/
	// public String[] query_id(Query queryClause, String orderByStr, int offset, int limit) {
	// 	// The return list of DataObjects
	// 	List<DataObject> retList = null;
		
	// 	// Setup the query, if needed
	// 	if (queryClause == null) {
	// 		// Null gets all
	// 		retList = new ArrayList<DataObject>(this.values());
	// 	} else {
	// 		// Converts query to sqlPredicate query
	// 		SqlPredicate sqlQuery = new SqlPredicate(queryStringify(queryClause));
			
	// 		// Get the list of _oid that passes the query
	// 		Set<String> idSet = backendIMap().keySet(sqlQuery);
	// 		String[] idArr = idSet.toArray(new String[0]);
			
	// 		// DataObject[] from idArr
	// 		DataObject[] doArr = getArrayFromID(idArr, true);
			
	// 		// Converts to a list
	// 		retList = new ArrayList(Arrays.asList(doArr));
	// 	}
		
	// 	// Sort, offset, convert to array, and return
	// 	retList = sortAndOffsetList(retList, orderByStr, offset, limit);
		
	// 	// Prepare the actual return string array
	// 	int retLength = retList.size();
	// 	String[] ret = new String[retLength];
	// 	for (int a = 0; a < retLength; ++a) {
	// 		ret[a] = retList.get(a)._oid();
	// 	}
		
	// 	// Returns sorted array of strings
	// 	return ret;
	// }
	
}
