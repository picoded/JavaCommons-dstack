package picoded.dstack.hazelcast.core;

// Java imports
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// JavaCommons imports
import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GenericConvert;
import picoded.core.conv.NestedObjectFetch;
import picoded.core.conv.StringEscape;
import picoded.core.struct.query.Query;
import picoded.core.common.ObjectToken;
import picoded.dstack.*;
import picoded.dstack.core.*;

// Hazelcast implementation
import com.hazelcast.core.*;
import com.hazelcast.config.*;
import com.hazelcast.map.eviction.LRUEvictionPolicy;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;

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
	HazelcastStack hazelcastStack = null;
	
	/**
	 * (DO NOT USE) Legacy Constructor, with direct instance, without stack support 
	 * this is used for test cases coverage
	 * 
	 * @param  inStack   hazelcast stack to use
	 * @param  name      of data object map to use
	 */
	public Hazelcast_DataObjectMap(HazelcastInstance instance, String name) {
		super();
		hazelcast = instance;
		configMap().put("name", name);
	}
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param  inStack   hazelcast stack to use
	 * @param  name      of data object map to use
	 */
	public Hazelcast_DataObjectMap(HazelcastStack inStack, String name) {
		super();
		hazelcastStack = inStack;
		hazelcast = inStack.getConnection();
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
	// custom hashmap class for hazelcast storage
	//
	//--------------------------------------------------------------------------
	
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
	// Overwrite update behaviour, to do a "clean clone" of data to be stored
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 * 
	 * @param  ObjectID to update
	 * @param  fullMap of values to apply update
	 * @param  keys of parameters to update (for partial update if supported)
	 **/
	public void DataObjectRemoteDataMap_update(String oid, Map<String, Object> fullMap,
		Set<String> keys) {
		
		// Clone the fullMap, to ensure data is "cleaned" for hazelcast
		// this helps work around known execptions with custom maps
		//
		// @TODO - consider finding a more "optimal" or performant conversion
		Map<String, Object> clonedMap = new HashMap<String, Object>();
		
		// Get and store the required values
		for (String key : fullMap.keySet()) {
			// Get the full map value
			Object val = fullMap.get(key);
			
			// Check for Map / List like objects
			if (val instanceof Map || val instanceof List) {
				// Clone it - by JSON serializing back and forth
				clonedMap.put(key, ConvertJSON.toObject(ConvertJSON.fromObject(val)));
			} else {
				// Store it directly, this should be a primative, or byte[]
				clonedMap.put(key, val);
			}
		}
		
		// call the default implementation
		super.DataObjectRemoteDataMap_update(oid, clonedMap, keys);
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
		
		// Setup the config based on the shared stack settings
		if (hazelcastStack != null) {
			hazelcastStack.setupHazelcastMapConfig(mConfig, configMap());
		}
		
		// Add in the default _oid
		mConfig.addMapIndexConfig(new MapIndexConfig("self[_oid]", true));
		
		// Enable query index for specific fields
		String[] indexArray = configMap().getStringArray("index", "[]");
		for (String indexName : indexArray) {
			// Skip _oid index, as its always defined
			if (indexName.equals("_oid")) {
				continue;
			}
			// Various collumn specific indexes
			mConfig.addMapIndexConfig(new MapIndexConfig("self[" + StringEscape.encodeURI(indexName)
				+ "]", true));
		}
		
		// Setup value extractor for `self` attribute
		mConfig.addMapAttributeConfig(new MapAttributeConfig("self",
			"picoded.dstack.hazelcast.core.HazelcastStorageExtractor"));
		
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
	//--------------------------------------------------------------------------
	
	/**
	 * Converts a conv.Query into a full SQL string
	 **/
	protected String queryStringify(Query queryClause) {
		
		// Converts into SQL string with ? value clause, and its arguments value
		String sqlString = queryClause.toSqlString();
		Object[] sqlArgs = queryClause.queryArgumentsArray();
		
		// Get the query argument map, to perform search and replace
		Map<String, List<Query>> fieldQueryMap = queryClause.fieldQueryMap();
		Set<String> fieldKeySet = fieldQueryMap.keySet();
		
		// Lets iterate each field string, and remap the sqlString
		//sqlString = sqlString.replaceAll("\"(.+)\" (.+) \\?", "self[\\\'$1\\\'] $2 ?");
		for (String field : fieldKeySet) {
			// Fix up sql string, to be hazelcast compatible instead
			sqlString = sqlString.replace("\"" + field + "\" ",
				"self[" + StringEscape.encodeURI(field) + "] ");
		}
		
		// if (sqlString != null) {
		// 	throw new RuntimeException(sqlString);
		// }
		
		// Iterate each sql argument
		for (int i = 0; i < sqlArgs.length; ++i) {
			// sql argument
			Object arg = sqlArgs[i];
			
			// Support ONLY either null, string, or number types as of now
			if (arg == null) {
				sqlString = sqlString.replaceFirst("\\?", "null");
			} else if (arg instanceof Number) {
				sqlString = sqlString.replaceFirst("\\?", arg.toString());
			} else if (arg instanceof String) {
				sqlString = sqlString.replaceFirst("\\?", "'" + arg.toString().replaceAll("\'", "\\'")
					+ "'");
			} else {
				throw new IllegalArgumentException("Unsupported query argument type : "
					+ arg.getClass().getName());
			}
		}
		
		// Debugging log
		// System.out.println(sqlString);
		
		// The processed SQL string
		return sqlString;
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
		// The return list of DataObjects
		List<DataObject> retList = null;
		
		// Setup the query, if needed
		if (queryClause == null) {
			// Null gets all
			retList = new ArrayList<DataObject>(this.values());
		} else {
			// Converts query to sqlPredicate query
			SqlPredicate sqlQuery = new SqlPredicate(queryStringify(queryClause));
			
			// Get the list of _oid that passes the query
			Set<String> idSet = backendIMap().keySet(sqlQuery);
			String[] idArr = idSet.toArray(new String[0]);
			
			// DataObject[] from idArr
			DataObject[] doArr = getArrayFromID(idArr, true);
			
			// Converts to a list
			retList = new ArrayList(Arrays.asList(doArr));
		}
		
		// Sort, offset, convert to array, and return
		retList = sortAndOffsetList(retList, orderByStr, offset, limit);
		
		// Prepare the actual return string array
		int retLength = retList.size();
		String[] ret = new String[retLength];
		for (int a = 0; a < retLength; ++a) {
			ret[a] = retList.get(a)._oid();
		}
		
		// Returns sorted array of strings
		return ret;
	}
	
}
