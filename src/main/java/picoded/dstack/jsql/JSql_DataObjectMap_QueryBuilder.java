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
import picoded.dstack.jsql.JSql_DataObjectMapUtil;
import picoded.dstack.connector.jsql.*;
import picoded.core.struct.*;
import picoded.core.struct.query.*;
import picoded.core.struct.query.condition.*;
import picoded.core.struct.query.internal.*;
import picoded.core.struct.MutablePair;

/**
 * Protected class, used to orgainze the various DataObjectMap query builder logic.
 *
 * The larger intention is to keep the DataObjectMap class more maintainable and unit testable
 * 
 * For simplicity of the documentation here, the examples used for the query being built is loosely based on
 * 
 * ```
 * SELECT oID FROM TABLENAME WHERE softDelete = 0 AND sourceOfLead = "post office"
 * ```
 **/
public class JSql_DataObjectMap_QueryBuilder {
	
	//-----------------------------------------------------------------------------------------------
	//
	//  Constructor with config
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Main internal dataobject map to fetch config / etc
	 */
	protected JSql_DataObjectMap dataMap = null;
	
	/**
	 * Constructor with the config map
	 */
	public JSql_DataObjectMap_QueryBuilder(JSql_DataObjectMap inMap) {
		dataMap = inMap;

		// Preloading memoizers in constructor,
		// as its the only lock-free segment that is 
		// guranteed to be thread safe
		preloadMemoizers();
	}
	
	/**
	 * Preloading of memoizer functions, this is done to ensure race conditions
	 * in multi threaded setup is avoided
	 */
	private void preloadMemoizers() {
		getFixedTableNameList();
		getFixedTableNamePrimaryKeyJoinSet();
	}

	//-----------------------------------------------------------------------------------------------
	//
	//  Fixed table configuration utilities
	//
	//  Note that special care was made such that all "memoizer" commands are "thread safe"
	//  without the need for locks.
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * @return the fixedTable config map if it exists, else returns null
	 */
	private GenericConvertMap<String, Object> getFixedTableFullConfigMap() {
		return dataMap.configMap.getGenericConvertStringMap("fixedTableMap", null);
	}
	
	/// Internal memoizer for the getFixedTableNameList
	private List<String> _getFixedTableNameList = null;

	/**
	 * @return Set of fixed table names if avaliable
	 */
	private List<String> getFixedTableNameList() {
		// Fetch Cache result 
		if( _getFixedTableNameList != null ) {
			return _getFixedTableNameList;
		}

		// Result list to build
		List<String> resList = new ArrayList<String>();
		
		// Get the table map
		GenericConvertMap<String, Object> tableMap = getFixedTableFullConfigMap();
		if (tableMap != null) {
			// Get the key set
			resList.addAll(tableMap.keySet());

			// Register memoizer, and return
			_getFixedTableNameList = resList;
			return resList;
		} 

		// memoizer blank result and return it
		_getFixedTableNameList = resList;
		return resList;
	}

	/**
	 * @param table alias name
	 * @return the fixed table name
	 */
	private String getFixedTableNameFromAlias(String aliasName) {
		int idx = Integer.parseInt( aliasName.substring(1) );
		return getFixedTableNameList().get(idx);
	}
	
	/**
	 * @param Fixed table name
	 * @return the fixed table name config
	 */
	private GenericConvertMap<String, Object> getFixedTableConfig(String tableName) {
		return getFixedTableFullConfigMap().getGenericConvertStringMap(tableName, "{}");
	}
	
	/**
	 * @param Fixed table name
	 * 
	 * @return the object key set that the collumns support
	 */
	private Set<String> getFixedTableObjectKeySet(String tableName) {
		return getFixedTableConfig(tableName).keySet();
	}

	/**
	 * @param Fixed table name
	 * @param the object key name
	 * 
	 * @return the collumn config used, normalized as a map - throws exception if config does not exist
	 */
	private GenericConvertMap<String, Object> getFixedTableCollumnConfig(String tableName,
		String objectKey) {
		// Get table specific config
		GenericConvertMap<String, Object> tableConfig = getFixedTableConfig(tableName);
		
		// Check for collumn setting
		if (tableConfig.get(objectKey) == null) {
			throw new RuntimeException("Missing valid '" + objectKey
				+ "' config for fixed table setup with '" + tableName + "'");
		}
		
		// Lets try get it as a map first
		GenericConvertMap<String, Object> res = tableConfig.getGenericConvertStringMap(objectKey,
			null);
		
		// Return it if not null
		if (res != null) {
			return res;
		}
		
		// Not stored as a map, assume a string instead, and remap it
		res = new GenericConvertHashMap<>();
		res.put("name", objectKey);
		res.put("type", tableConfig.getString(objectKey));
		
		// Return remapped config
		return res;
	}
	
	/**
	 * @param Fixed table name
	 * @param the object key name
	 * 
	 * @return the collumn name used
	 */
	private String getFixedTableCollumnName(String tableName, String objectKey) {
		// Get collumn specific config - throws exception if config does not exist
		GenericConvertMap<String, Object> collumnConfig = getFixedTableCollumnConfig(tableName,
			objectKey);
		
		// Ge the collumn name
		String name = collumnConfig.getString("name");
		
		// Validate, and return
		if (name == null || name.length() <= 0) {
			throw new RuntimeException("Missing valid collumn name config for '" + objectKey
				+ "' within fixed table setup of '" + tableName + "'");
		}
		return name;
	}
	
	/**
	 * @param Fixed table name
	 * @param the object key name
	 * 
	 * @return the collumn type used
	 */
	private String getFixedTableCollumnType(String tableName, String objectKey) {
		// Get collumn specific config - throws exception if config does not exist
		GenericConvertMap<String, Object> collumnConfig = getFixedTableCollumnConfig(tableName,
			objectKey);
		
		// Ge the collumn name
		String type = collumnConfig.getString("type");
		
		// Validate, and return
		if (type == null || type.length() <= 0) {
			throw new RuntimeException("Missing valid collumn type config for '" + objectKey
				+ "' within fixed table setup of '" + tableName + "'");
		}
		return type;
	}
	
	/// Internal memoizer for `getFixedTableNamePrimaryKeyJoinSet`
	private Set<String> _getFixedTableNamePrimaryKeyJoinSet = null;
	
	/**
	 * @return Set of fixed table that requires primary key joins
	 */
	private Set<String> getFixedTableNamePrimaryKeyJoinSet() {
		// Get from memoizer
		if (_getFixedTableNamePrimaryKeyJoinSet != null) {
			return _getFixedTableNamePrimaryKeyJoinSet;
		}
		
		// Boolean result if primary key joins is needed
		Set<String> pkJoinSet = new HashSet<String>();
		
		// Get the table names
		List<String> tableNameSet = getFixedTableNameList();

		// And iterate it
		for (String tableName : tableNameSet) {
			
			// Get the "_oid" collumn config, this also function as a quick config check
			GenericConvertMap<String, Object> oidConfig = getFixedTableCollumnConfig(tableName,
				"_oid");
			
			// Skip if primary key join is configured to be skipped
			if (oidConfig.getBoolean("skipPrimaryKeyJoin", false)) {
				continue;
			}
			
			// Build the result set
			pkJoinSet.add(tableName);
		}
		
		// Return the result
		_getFixedTableNamePrimaryKeyJoinSet = pkJoinSet;
		return pkJoinSet;
	}
	
	//-----------------------------------------------------------------------------------------------
	//
	//  _oid key set
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Query builder used to build the oID query, without where clause.
	 * 
	 * Can be used either to return a collumn of oID, or a single row/collumn of "rcount",
	 * representing the number of rows.
	 * 
	 * This take advantage of UNION for the fixed table, without joins.@interface
	 * This is not to be used together with the much larger complex joins
	 */
	private StringBuilder primaryKeyQueryBuilder(boolean isRcount) {
		
		// The query string to build
		StringBuilder queryStr = new StringBuilder();
		
		// Get fixed table name set
		Set<String> fixedTableNames = getFixedTableNamePrimaryKeyJoinSet();
		
		//------------------------------------------------------------------
		// If no fixed tablenames, return the heavily simplified query
		// with only the primary table map
		//------------------------------------------------------------------

		// Perform simple primary key query if possible
		if (fixedTableNames.size() <= 0) {
			// Select clause
			queryStr.append("SELECT ");

			// Handle rcount mode
			if( isRcount ) {
				queryStr.append("COUNT(*) AS rcount FROM ");
			} else {
				queryStr.append("oID FROM ");
			}

			// Primary table to query
			queryStr.append(dataMap.primaryKeyTable);

			// Return query string
			return queryStr;
		}
		
		//------------------------------------------------------------------
		// Complex fixed and dynamic query required here
		//------------------------------------------------------------------

		// oID collumn first
		queryStr.append("SELECT oID FROM ").append(dataMap.primaryKeyTable).append("\n");
		
		// Join the oid collumn for the resepctive tables
		for (String tableName : fixedTableNames) {
			queryStr.append("UNION \n");
			queryStr.append("SELECT '").append(getFixedTableCollumnName(tableName, "_oid"));
			queryStr.append("' AS oID FROM ").append(tableName).append(" \n");
		}
		
		// Row count would require a nested query of the oID,
		// to be wrapped with the row count clause
		if( isRcount ) {
			// lets build the wrapped query
			StringBuilder queryWrap = new StringBuilder();
			queryWrap.append("SELECT COUNT(*) AS rcount FROM (\n");
			queryWrap.append( queryStr );
			queryWrap.append(")");

			// And return it wrapped
			return queryWrap;
		}

		// Return the query string with oID
		return queryStr;
	}

	/**
	 * Get and returns all the GUID's, note that due to its
	 * potential of returning a large data set, production use
	 * should be avoided.
	 *
	 * @return JSqlResult, with the oID collumn filled with result
	 */
	public JSqlResult getOidKeyJSqlResult() {
		return dataMap.sqlObj.query(primaryKeyQueryBuilder(false).toString(), EmptyArray.OBJECT);
	}

	/**
	 * Get and returns all the GUID's, note that due to its
	 * potential of returning a large data set, production use
	 * should be avoided.
	 *
	 * @return set of keys
	 */
	public Set<String> getOidKeySet() {
		// Get raw jsql result
		JSqlResult r = getOidKeyJSqlResult();

		// Convert it into a set
		if (r == null || r.get("oID") == null) {
			return new HashSet<String>();
		}
		return ListValueConv.toStringSet(r.getObjectList("oID"));
	}
	
	//-----------------------------------------------------------------------------------------------
	//
	//  OrderBy string processing
	//
	//-----------------------------------------------------------------------------------------------
	
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
	
	//-----------------------------------------------------------------------------------------------
	//
	//  Query Builder Utils
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Scan the given list of object key names and split the query plan between both
	 * 
	 * @param List of object keys to be queries
	 * 
	 * @return Split keyname set, with the first (left) used for dynamic keys, and right used for fixed tables
	 */
	private MutablePair<List<String>, List<String>> splitCollumnListForDynamicAndFixedQuery(Collection<String> queryKeyNames) {
		// List of object keys for fixed and dynamic tables respectively
		Set<String> fixedKeyNames = new HashSet<String>();
		Set<String> dynamicKeyNames = new HashSet<String>();
		
		// Get fixed table name set
		List<String> fixedTableNameSet = getFixedTableNameList();
		
		// Lets process all the fixed table key names
		//-----------------------------------------------
		for(String tableName : fixedTableNameSet) {
			// Get the keynames of the table
			Set<String> tableKeyNameSet = getFixedTableObjectKeySet(tableName);
			
			// Lets iterate each table key name
			for(String tableKeyName : tableKeyNameSet) {
				// if table key name is in the query, register it
				if( queryKeyNames.contains(tableKeyName) ) {
					fixedKeyNames.add( tableKeyName );
				}
			}
		}

		// Lets process all the dynamic table key names
		//-----------------------------------------------
		for(String queryKey : queryKeyNames) {
			// Check if its already handled in fixed tables
			if( fixedKeyNames.contains(queryKey) ) {
				continue;
			}

			// Set it up as a dynamic key
			dynamicKeyNames.add( queryKey );
		}

		// Coonvert set into list
		List<String> fixedKeyNamesList = new ArrayList<String>();
		List<String> dynamicKeyNamesList = new ArrayList<String>();
		fixedKeyNamesList.addAll(fixedKeyNames);
		dynamicKeyNamesList.addAll(dynamicKeyNames);

		// Return result as mutable pair
		return new MutablePair<>(dynamicKeyNamesList, fixedKeyNamesList);
	}

	//-----------------------------------------------------------------------------------------------
	//
	//  Internal query builder
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Scan the given query keys, to deduce which collumn should have "NULL" support.
	 * Generating this set is important to ensure proper query support with NULL values.
	 * 
	 * This works by scanning orderby clause, that does not have a corresponding equality check
	 * Or where clauses with inequality check / null equality check
	 * 
	 * @param fieldQueryMap used to get the object key to sub query condition mapping
	 * @param set of raw order keys to scan
	 * @param set of raw where keys to scan
	 * 
	 * @return key set where NULL support is needed
	 */
	private Set<String> extractCollumnsWhichMustSupportNullValues( //
		Map<String, List<Query>> fieldQueryMap, //
		Collection<String> rawOrderByClauseCollumns, //
		Collection<String> rawWhereClauseCollumns //
	) { //

		// Prepare the return result
		Set<String> keysWhichMustHandleNullValues = new HashSet<>();

		// Process the order by string
		if (rawOrderByClauseCollumns != null) {
			for (String collumn : rawOrderByClauseCollumns) {
				// Collumn names to skip setup (reseved keywords?)
				if (collumn.equalsIgnoreCase("_oid") || collumn.equalsIgnoreCase("oID")) {
					continue;
				}
				
				// There is no query / query map, so NULL must be supported
				if (fieldQueryMap == null) {
					keysWhichMustHandleNullValues.add(collumn);
					continue;
				}
				
				// Check if any query is used with order by clause
				List<Query> toReplaceQueries = fieldQueryMap.get(collumn);
				
				// No query filtering was done, therefor, NULL must be suported
				if (toReplaceQueries == null || toReplaceQueries.size() <= 0) {
					keysWhichMustHandleNullValues.add(collumn);
					continue;
				}
				
				for (Query subQuery : toReplaceQueries) {
					// Check for inequality condition, where NULL must be supported
					// @TODO consider optimizing != null handling
					if (subQuery.operatorSymbol().equalsIgnoreCase("!=")) {
						keysWhichMustHandleNullValues.add(collumn);
						break;
					}
					
					// Check for equality condition, with NULL values
					if (subQuery.operatorSymbol().equalsIgnoreCase("=")
						&& subQuery.defaultArgumentValue() == null) {
						keysWhichMustHandleNullValues.add(collumn);
						break;
					}
				}
				
				// There are equality checks, which would filter out NULL values
				// therefor order by collumn is not added to the NULL support list
			}
		}
		
		// For each collumnName in the collumnNameSet, scan for inequality check
		// or equality with null check - to map its use case for "keysWhichMustHandleNullValues"
		if (rawWhereClauseCollumns != null) {
			for (String collumn : rawWhereClauseCollumns) {
				// Collumn names to skip setup (reseved keywords?)
				if (collumn.equalsIgnoreCase("_oid") || collumn.equalsIgnoreCase("oID")) {
					continue;
				}
				
				// The query list to do processing on
				List<Query> toReplaceQueries = fieldQueryMap.get(collumn);
				
				// Skip if no query was found needed processing
				if (toReplaceQueries == null || toReplaceQueries.size() <= 0) {
					continue;
				}
				
				// Check for inequality condition, where NULL must be supported
				for (Query subQuery : toReplaceQueries) {
					// Check for inequality condition, where NULL must be supported
					// @TODO consider optimizing != null handling
					if (subQuery.operatorSymbol().equalsIgnoreCase("!=")) {
						keysWhichMustHandleNullValues.add(collumn);
						break;
					}
					
					// Check for equality condition, with NULL values
					if (subQuery.operatorSymbol().equalsIgnoreCase("=")
						&& subQuery.defaultArgumentValue() == null) {
						keysWhichMustHandleNullValues.add(collumn);
						break;
					}
				}
			}

		}
		
		// The keys to support
		return keysWhichMustHandleNullValues;
	}

	/**
	 * Given the dynamic/fixed object keys, and it sequence -
	 * generate out the table alias map.
	 */
	private Map<String,String> generateCollumnTableAliasMap( //
		List<String> dynamicTableKeys, //
		List<String> fixedTableKeys //
	) { //

		// alias mapping of the collumn names (the result)
		Map<String, String> objectKeyTableAliasMap = new HashMap<>();
		
		// Dynamic table keys handling
		//-------------------------------------------------------------------
		for (int i=0; i<dynamicTableKeys.size(); ++i) {
			// Get the keyname
			String keyName = dynamicTableKeys.get(i);

			// Collumn names to skip setup (reseved keywords?)
			if (keyName.equalsIgnoreCase("_oid") || keyName.equalsIgnoreCase("oID")) {
				continue;
			}
			
			// collumn names that requires setup
			objectKeyTableAliasMap.put(keyName, "D" + i);
		}
		
		// Fixed table keys handling
		//-------------------------------------------------------------------

		// Get fixed table name set
		List<String> fixedTableNameList = getFixedTableNameList();
		
		// And iterate all the fixed tables in sequence
		for (int i=0; i<fixedTableNameList.size(); ++i) {
			// Get the table name
			String tableName = fixedTableNameList.get(i);

			// Get the keynames of the table
			Set<String> tableKeyNameSet = getFixedTableObjectKeySet(tableName);
			
			// Lets iterate each table key name
			for(String tableKeyName : tableKeyNameSet) {
				// if table key name is in the query, register it
				if( fixedTableKeys.indexOf(tableKeyName) >= 0 ) {
					// collumn names that requires setup
					objectKeyTableAliasMap.put(tableKeyName, "F" + i);
				}
			}
		}

		// Return result
		return objectKeyTableAliasMap;
	}

	/**
	 * Lets build the core inner join query string, 
	 * given the required filtered collumn names.
	 * 
	 * This is appended to the "SELECT DP.oID FROM" statement
	 * 
	 * Its expected result without any collumns provided would be
	 * 
	 * ```
	 * ```
	 * 
	 * Alternatively, if collumn names are provided (as part of the WHERE / ORDER BY clause),
	 * it will generate an additional inner join line
	 * 
	 * ```
	 * INNER JOIN (SELECT oID, nVl, sVl, tVl FROM DD_TABLENAME WHERE kID="softDelete") AS D0 ON (DP.oID = D0.oID)
	 * INNER JOIN (SELECT oID, nVl, sVl, tVl FROM DD_TABLENAME WHERE kID="sourceOfLead") AS D1 ON (DP.oID = D1.oID)
	 * ```
	 * 
	 * @param  collumns that is needed, in the given order
	 * @param  collumnWhichMustHandleNullValues to perform left join, instead of inner join, to support NULL values
	 * 
	 * @return pair of query string, with query args
	 */
	private MutablePair<StringBuilder, List<Object>> dynamicTableJoinBuilder(List<String> collumns, Set<String> collumnWhichMustHandleNullValues) {

		// Settings needed from main DataObjectMap
		String primaryKeyTable  = dataMap.primaryKeyTable;
		String dataStorageTable = dataMap.dataStorageTable; 

		// The query string to build
		StringBuilder queryStr = new StringBuilder();
		List<Object> queryArg = new ArrayList<>();
		
		// Add table name to join from first
		// queryStr.append(primaryKeyTable).append(" AS DP \n");
		
		// No collumns required (fast ending)
		if (collumns == null || collumns.size() <= 0) {
			return new MutablePair<>(queryStr, queryArg);
		}
		
		// For each collumn that is required, perform an inner join
		// where applicable, skipping left join.
		//
		// This represents the more "optimized" joins
		for (int i = 0; i < collumns.size(); ++i) {
			// Get the collumn name
			String collumnName = collumns.get(i);
			
			// Skip the "LEFT JOIN" collumn
			if (collumnWhichMustHandleNullValues.contains(collumnName)) {
				continue;
			}
			
			// Single collumn "INNER JOIN"
			queryStr.append("INNER JOIN (SELECT oID, nVl, sVl, tVl FROM ").append(dataStorageTable) //
				.append(" WHERE kID=? AND idx=?) AS D" + i + " ON (") //
				.append("DP.oID = D" + i + ".oID) \n");
			// With arguments
			queryArg.add(collumnName);
			queryArg.add(0);
		}
		
		// Perform the much slower (expensive) inner join
		for (int i = 0; i < collumns.size(); ++i) {
			// Get the collumn name
			String collumnName = collumns.get(i);
			
			// Skip the "INNER JOIN" collumn
			if (!collumnWhichMustHandleNullValues.contains(collumnName)) {
				continue;
			}
			
			// Single collumn "LEFT JOIN"
			queryStr.append("LEFT JOIN (SELECT oID, nVl, sVl, tVl FROM ").append(dataStorageTable) //
				.append(" WHERE kID=? AND idx=?) AS D" + i + " ON (") //
				.append("DP.oID = D" + i + ".oID) \n");
			// With arguments
			queryArg.add(collumnName);
			queryArg.add(0);
		}
		
		// Return the full query
		return new MutablePair<>(queryStr, queryArg);
	}
	
	/**
	 * Lets build the fixed table outer join query string, 
	 * given the required filtered collumn names.
	 * 
	 * This is designed to be appended to the dynamic table query,
	 * and is not designed to be used alone.
	 * 
	 * Its expected result without any collumns provided would be blank
	 * 
	 * ```
	 * ```
	 * 
	 * Alternatively, if collumn names are provided (as part of the WHERE / ORDER BY clause),
	 * it will generate an additional outer join line
	 * 
	 * ```
	 * LEFT JOIN FIXED_TABLE_A AS F0 ON DP.oID = F0.oID
	 * LEFT JOIN FIXED_TABLE_B AS F1 ON DP.oID = F1.oID
	 * ```
	 * 
	 * @param  collumns that is needed, in the given order
	 * @param  collumnWhichMustHandleNullValues to perform left join, instead of inner join, to support NULL values
	 * 
	 * @return pair of query string, with query args
	 */
	private MutablePair<StringBuilder, List<Object>> fixedTableJoinBuilder(List<String> collumns, Set<String> collumnWhichMustHandleNullValues) {

		// The query string to build
		StringBuilder queryStr = new StringBuilder();
		List<Object> queryArg = new ArrayList<>();
		
		// Fixed table keys handling
		//-------------------------------------------------------------------

		// Get fixed table name set
		List<String> fixedTableNameList = getFixedTableNameList();
		
		// And iterate all the fixed tables in sequence
		for (int i=0; i<fixedTableNameList.size(); ++i) {
			// Get the table name
			String tableName = fixedTableNameList.get(i);

			// Get the keynames of the table
			Set<String> tableKeyNameSet = getFixedTableObjectKeySet(tableName);
			
			// Indicate if the fixed table is the be queried
			boolean includeFixedTable = false;

			// Lets iterate the collumn names
			for(String objKey : collumns) {
				if( tableKeyNameSet.contains(objKey) ) {
					includeFixedTable = true;
					break;
				}
			}

			// Skip current table if tis not needed
			if( !includeFixedTable ) {
				break;
			}

			// OK - assume the current table needs to be include, build the query
			queryStr.append("LEFT JOIN ").append(tableName); //
			queryStr.append(" AS F"+i+" ON DP.oID = F0."+getFixedTableCollumnName(tableName, "oID") ); //
			queryStr.append("\n");
		}

		// Return the full query
		return new MutablePair<>(queryStr, queryArg);
	}

	/**
	 * Given the where clause query object, rewrite it to query against the joint dynamic table used internally.
	 * 
	 * This replaces the respective "object key" with the "TABLE_ALIAS.s/n/tVl" respectively.
	 * 
	 * @param  query object to rewrite (and return)
	 * @param  field to query mapping
	 * @param  arg name to arg value mapping
	 * @param  object key to table alias name mapping
	 * @param  list of dynamic keys to handle
	 * 
	 * @return rewritten queryObj 
	 */
	private Query dynamicTableQueryRewrite( //
		Query queryObj, Map<String, List<Query>> fieldQueryMap, //
		Map<String, Object> queryArgMap, //
		Map<String, String> objectKeyTableAliasMap, //
		List<String> dynamicKeyNames //
	) {
		
		// Lets iterate the dynamic key names
		// and rewrite each ddynamic key
		for (String collumn : dynamicKeyNames) {

			// The query list to do processing on
			List<Query> toReplaceQueries = fieldQueryMap.get(collumn);
			
			// Skip if no query was found needed processing
			if (toReplaceQueries == null || toReplaceQueries.size() <= 0) {
				continue;
			}
			
			// Special handling for _oid
			if (collumn.equalsIgnoreCase("_oid") || collumn.equals("oID")) {
				// Scan for the query to remap to DP.oID
				for (Query toReplace : toReplaceQueries) {
					Query replacement = QueryFilter.basicQueryFromTokens(
						//
						queryArgMap, "DP.oID", toReplace.operatorSymbol(),
						":" + toReplace.argumentName() //
					);
					// Replaces old query with new query
					queryObj = queryObj.replaceQuery(toReplace, replacement);
				}
				continue;
			}
			
			// Get the replacment table alias
			String collumnTableAlias = objectKeyTableAliasMap.get(collumn);

			// Scan for the query to perform replacements
			for (Query toReplace : toReplaceQueries) {
				// Get the argument
				Object argObj = queryArgMap.get(toReplace.argumentName());
				
				// Setup the replacement query
				Query replacement = null;
				
				if (argObj == null) {
					// Does special NULL handling
					replacement = QueryFilter.basicQueryFromTokens(queryArgMap, collumnTableAlias
						+ ".sVl", toReplace.operatorSymbol(), ":" + toReplace.argumentName() //
					);
					
					// 
					// Lets do special SQL condition overwriting
					// to properly support null equality checks
					//
					// This works around known limitations of SQL
					// requiring NULL checks as the "IS NULL" or "IS NOT NULL"
					// varient
					//
					// https://www.tutorialspoint.com/sql/sql-null-values.htm#:~:text=The%20SQL%20NULL%20is%20the,a%20field%20that%20contains%20spaces.
					//
					if (toReplace.operatorSymbol().equalsIgnoreCase("!=")) {
						replacement = new JSql_QueryStringOverwrite( //
							replacement, // The replacement query, in case is still needed
							"(" + collumnTableAlias + ".sVl IS NOT NULL OR " + replacement.toString()
								+ ")");
					} else if (toReplace.operatorSymbol().equalsIgnoreCase("=")) {
						replacement = new JSql_QueryStringOverwrite( //
							replacement, // The replacement query, in case is still needed
							"(" + collumnTableAlias + ".sVl IS NULL OR " + replacement.toString() + ")");
					}
				} else if (argObj instanceof Number) {
					// Does special numeric handling
					replacement = QueryFilter.basicQueryFromTokens(queryArgMap, collumnTableAlias
						+ ".nVl", toReplace.operatorSymbol(), ":" + toReplace.argumentName() //
					);
				} else if (argObj instanceof String) {
					if (toReplace.operatorSymbol().equalsIgnoreCase("LIKE")) {
						// Like operator maps to tVl
						replacement = QueryFilter.basicQueryFromTokens(queryArgMap, collumnTableAlias
							+ ".tVl", toReplace.operatorSymbol(), ":" + toReplace.argumentName() //
						);
					} else {
						// Else it maps to sVl, with applied limits
						replacement = QueryFilter.basicQueryFromTokens(queryArgMap, collumnTableAlias
							+ ".sVl", toReplace.operatorSymbol(), ":" + toReplace.argumentName() //
						);
						// Update the argument with limits
						queryArgMap.put(toReplace.argumentName(),
							JSql_DataObjectMapUtil.shortenStringValue(argObj.toString()));
						
						// 
						// Special handling of != once again
						// due to SQL quirk with NULL values
						//
						// `col != "value"`, is remapped as
						// `col != "value" OR col IS NULL`
						//
						if (toReplace.operatorSymbol().equalsIgnoreCase("!=")) {
							replacement = new JSql_QueryStringOverwrite( //
								replacement, // The replacement query, in case is still needed
								"(" + collumnTableAlias + ".sVl IS NULL OR " + replacement.toString()
									+ ")");
						}
					}
				}
				
				// Unprocessed arg type
				if (replacement == null) {
					throw new RuntimeException("Unexpeced query argument (unknown type) : " + argObj);
				}
				
				// Replaces old query with new rewritten query
				queryObj = queryObj.replaceQuery(toReplace, replacement);
			}
		}

		return queryObj;
	}

	/**
	 * Given the where clause query object, rewrite it to query against the joint fixed table used internally.
	 * 
	 * This replaces the respective "object key" with the "TABLE_ALIAS.collumn_name" respectively.
	 * 
	 * @param  query object to rewrite (and return)
	 * @param  field to query mapping
	 * @param  arg name to arg value mapping
	 * @param  object key to table alias name mapping
	 * @param  list of dynamic keys to handle
	 * 
	 * @return rewritten queryObj 
	 */
	private Query fixedTableQueryRewrite( //
		Query queryObj, Map<String, List<Query>> fieldQueryMap, //
		Map<String, Object> queryArgMap, //
		Map<String, String> objectKeyTableAliasMap, //
		List<String> fixedKeyNames //
	) {
		
		// Lets iterate the fixed table key names
		// and rewrite each object key
		for (String collumn : fixedKeyNames) {

			// The query list to do processing on
			List<Query> toReplaceQueries = fieldQueryMap.get(collumn);
			
			// Skip if no query was found needed processing
			if (toReplaceQueries == null || toReplaceQueries.size() <= 0) {
				continue;
			}
			
			// Special handling for _oid
			if (collumn.equalsIgnoreCase("_oid") || collumn.equals("oID")) {
				// Scan for the query to remap to DP.oID
				for (Query toReplace : toReplaceQueries) {
					Query replacement = QueryFilter.basicQueryFromTokens(
						//
						queryArgMap, "DP.oID", toReplace.operatorSymbol(),
						":" + toReplace.argumentName() //
					);
					// Replaces old query with new query
					queryObj = queryObj.replaceQuery(toReplace, replacement);
				}
				continue;
			}
			
			// Get the replacment table alias
			String collumnTableAlias = objectKeyTableAliasMap.get(collumn);
			String fixedTableName = getFixedTableNameFromAlias(collumnTableAlias);
			String fixedTableCollumnName = getFixedTableCollumnName(fixedTableName, collumn);
			
			// Scan for the query to perform replacements
			for (Query toReplace : toReplaceQueries) {
				// Get the argument
				Object argObj = queryArgMap.get(toReplace.argumentName());
				
				// Setup the replacement query
				Query replacement = null;
				
				if (argObj == null) {
					// Does special NULL handling
					replacement = QueryFilter.basicQueryFromTokens(queryArgMap, collumnTableAlias
						+ "." + fixedTableCollumnName, toReplace.operatorSymbol(), ":" + toReplace.argumentName() //
					);
					
					// 
					// Lets do special SQL condition overwriting
					// to properly support null equality checks
					//
					// This works around known limitations of SQL
					// requiring NULL checks as the "IS NULL" or "IS NOT NULL"
					// varient
					//
					// https://www.tutorialspoint.com/sql/sql-null-values.htm#:~:text=The%20SQL%20NULL%20is%20the,a%20field%20that%20contains%20spaces.
					//
					if (toReplace.operatorSymbol().equalsIgnoreCase("!=")) {
						replacement = new JSql_QueryStringOverwrite( //
							replacement, // The replacement query, in case is still needed
							"(" + collumnTableAlias + "." + fixedTableCollumnName + " IS NOT NULL OR " + replacement.toString()
								+ ")");
					} else if (toReplace.operatorSymbol().equalsIgnoreCase("=")) {
						replacement = new JSql_QueryStringOverwrite( //
							replacement, // The replacement query, in case is still needed
							"(" + collumnTableAlias + "." + fixedTableCollumnName + " IS NULL OR " + replacement.toString() + ")");
					}
				} else if (argObj instanceof Number) {
					// Does special numeric handling
					replacement = QueryFilter.basicQueryFromTokens(queryArgMap, collumnTableAlias
						+ "." + fixedTableCollumnName, toReplace.operatorSymbol(), ":" + toReplace.argumentName() //
					);
				} else if (argObj instanceof String) {
					if (toReplace.operatorSymbol().equalsIgnoreCase("LIKE")) {
						// Like operator maps to tVl
						replacement = QueryFilter.basicQueryFromTokens(queryArgMap, collumnTableAlias
							+ "." + fixedTableCollumnName, toReplace.operatorSymbol(), ":" + toReplace.argumentName() //
						);
					} else {
						// Else it maps to sVl, with applied limits
						replacement = QueryFilter.basicQueryFromTokens(queryArgMap, collumnTableAlias
							+ "." + fixedTableCollumnName, toReplace.operatorSymbol(), ":" + toReplace.argumentName() //
						);
						// Update the argument with limits
						queryArgMap.put(toReplace.argumentName(),
							JSql_DataObjectMapUtil.shortenStringValue(argObj.toString()));
						
						// 
						// Special handling of != once again
						// due to SQL quirk with NULL values
						//
						// `col != "value"`, is remapped as
						// `col != "value" OR col IS NULL`
						//
						if (toReplace.operatorSymbol().equalsIgnoreCase("!=")) {
							replacement = new JSql_QueryStringOverwrite( //
								replacement, // The replacement query, in case is still needed
								"(" + collumnTableAlias + "." + fixedTableCollumnName + " IS NULL OR " + replacement.toString()
									+ ")");
						}
					}
				}
				
				// Unprocessed arg type
				if (replacement == null) {
					throw new RuntimeException("Unexpeced query argument (unknown type) : " + argObj);
				}
				
				// Replaces old query with new rewritten query
				queryObj = queryObj.replaceQuery(toReplace, replacement);
			}
		}

		return queryObj;
	}

	//-----------------------------------------------------------------------------------------------
	//
	//  Build and execute the full complex query
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Performs a search query, and returns the respective result containing the DataObjects information
	 * This works by taking the query and its args, building its complex inner view, then querying that view.
	 * 
	 * This function is arguably the heart of the JSQL DataObjectMap query builder
	 *
	 * @param   The selected oid columns to query
	 * @param   where query statement
	 * @param   where clause values array
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max
	 *
	 * @return  The JSql query result
	 **/
	protected JSqlResult runComplexQuery( //
		String oidCollumns, String whereClause, Object[] whereValues, //
		String orderByStr, int offset, int limit //
	) { //
	
		// Settings needed from main DataObjectMap
		JSql   sql              = dataMap.sqlObj;
		String primaryKeyTable  = dataMap.primaryKeyTable;
		String dataStorageTable = dataMap.dataStorageTable; 

		//==========================================================================
		//
		// Quick optimal lookup : to the parent oID table.
		// Does not do any complex building of clauses
		// Runs the query and exit immediately
		//
		//==========================================================================
		
		if (whereClause == null && orderByStr == null && offset <= 0 && limit <= 0
		// ---
		// Note the following checks has been commented out as in its
		// current use case, its always "true"
		// ---
		// && (
		// 	oidCollumns.equalsIgnoreCase("DISTINCT DP.oID") || 
		// 	oidCollumns.equalsIgnoreCase("COUNT(DISTINCT DP.oID) AS rcount")
		// 	oidCollumns.equalsIgnoreCase("oID") || 
		// 	oidCollumns.equalsIgnoreCase("DISTINCT oID") || 
		// 	oidCollumns.equalsIgnoreCase("COUNT(DISTINCT oID) AS rcount")
		// )
		) {
			// Blank where clause query search, quick and easy
			boolean containsRcount = (oidCollumns.indexOf("COUNT") >= 0);
			return sql.query(primaryKeyQueryBuilder(containsRcount).toString(), EmptyArray.OBJECT);
		}
		
		//==========================================================================
		//
		// Sadly looks like things must be done the hard way, 
		// lets build the following generic struct
		//
		// - Query
		// - OrderBy
		//
		// which is used to facilitate
		//
		// - SQL Injection protect
		// - Extraction of collumn names needed
		//
		// At this point the code is just analysing the query to help
		// build the larger much more complex query.
		//
		//==========================================================================
		
		// The where clause query object, that is built, and actually used
		Query queryObj = null;
		
		// Result ordering by
		OrderBy<DataObject> orderByObj = null;
		
		// Collumn name sets
		Set<String> rawWhereClauseCollumns = null;
		Set<String> rawOrderByClauseCollumns = null;
		
		// List of collumns that is needed for both where / order by
		Set<String> rawCollumnNameSet = new HashSet<>();
		
		// Gets the original field to "raw query" maps
		// of keys, to do subtitution on,
		// and their respective argument map.
		Map<String, List<Query>> fieldQueryMap = null;
		Map<String, Object> queryArgMap = null;
		
		// Where clause exists, build it!
		if (whereClause != null && whereClause.length() >= 0) {
			// The complex query object
			queryObj = Query.build(whereClause, whereValues);
			
			// Get the collumn keynames
			rawWhereClauseCollumns = queryObj.keyValuesMap().keySet();
			rawCollumnNameSet.addAll(rawWhereClauseCollumns);
			
			// Build the field, and arg map
			fieldQueryMap = queryObj.fieldQueryMap();
			queryArgMap = queryObj.queryArgumentsMap();
		}
		
		// OrderBy clause exist, build it
		if (orderByStr != null) {
			// Lets build the order by object
			orderByObj = getOrderByObject(orderByStr);
			
			// Get the collumn keynames
			rawOrderByClauseCollumns = orderByObj.getKeyNames();
			rawCollumnNameSet.addAll(rawOrderByClauseCollumns);
		}
		
		//--------------------------------------------------------------------------
		// Scan for collumns to use for "inner join"
		// either from the "orderBy" clause, without an equality check
		// OR an inequality check.
		//
		// This helps find all the collumns requried for 
		// keysWhichMustHandleNullValues
		//--------------------------------------------------------------------------
		
		// List of collumns which must take into account possible NULL
		// values, which has its own set of quirks in SQL
		Set<String> keysWhichMustHandleNullValues = extractCollumnsWhichMustSupportNullValues( //
			fieldQueryMap, rawOrderByClauseCollumns, rawWhereClauseCollumns //
		); //
		
		//==========================================================================
		//
		// Prepare the various collumn alias mapping which is used internally
		// and build the complex inner query.
		//
		// Alias mapping is required to map the respective object key to the
		// joint key value of the query
		// 
		//==========================================================================
		
		// Split the key set between dynamic and fixed table collumns
		MutablePair<List<String>,List<String>> dynamicAndFixedKeyPairs = splitCollumnListForDynamicAndFixedQuery(rawCollumnNameSet);
		List<String> dynamicKeyNames = dynamicAndFixedKeyPairs.left;
		List<String> fixedKeyNames   = dynamicAndFixedKeyPairs.right;

		// Generate alias mapping of the various object keys
		Map<String, String> objectKeyTableAliasMap = generateCollumnTableAliasMap(dynamicKeyNames, fixedKeyNames);
		
		//==========================================================================
		//
		// Build the complex inner join table
		//
		//==========================================================================
		
		// Final query string builder and arguments
		StringBuilder fullQuery = new StringBuilder();
		List<Object> fullQueryArgs = new ArrayList<>();
		
		// The select clause
		fullQuery.append("SELECT ").append(oidCollumns).append(" FROM \n");
		fullQuery.append("(").append(primaryKeyQueryBuilder(false)).append(") AS DP \n");

		// the inner join 
		MutablePair<StringBuilder, List<Object>> innerJoinPair = dynamicTableJoinBuilder(dynamicKeyNames, keysWhichMustHandleNullValues);
		
		// Merged together with full query, with the inner join clauses
		fullQuery.append(innerJoinPair.left);
		fullQueryArgs.addAll(innerJoinPair.right);

		// Merge together fixed table query if needed
		if( fixedKeyNames.size() > 0 ) {
			MutablePair<StringBuilder, List<Object>> outerJoinPair = fixedTableJoinBuilder(fixedKeyNames, keysWhichMustHandleNullValues);

			// Merged together with full query, with the outer join clauses
			fullQuery.append(outerJoinPair.left);
			fullQueryArgs.addAll(outerJoinPair.right);
		}
		
		//==========================================================================
		//
		// Rebuild the query where clause, to use the respective
		// joined views in place of the object key
		//
		//==========================================================================
		if (queryObj != null) {
			
			// Rewrite the query for dynamic table collumns
			queryObj = dynamicTableQueryRewrite( //
				queryObj, fieldQueryMap, queryArgMap, //
				objectKeyTableAliasMap, dynamicKeyNames //
			); //

			// Rewrite the query for fixed table collumns
			if( fixedKeyNames.size() > 0 ) {
				queryObj = fixedTableQueryRewrite( //
					queryObj, fieldQueryMap, queryArgMap, //
					objectKeyTableAliasMap, fixedKeyNames //
				); //
			}

			//--------------------------------------------------------------------------
			// Update the query clauses collumn linkage, and apply to fullQuery
			//--------------------------------------------------------------------------
			
			// WHERE query is built from queryObj, this acts as a form of sql sanitization
			fullQuery.append("WHERE ");
			fullQuery.append(queryObj.toSqlString().replaceAll("\"", ""));
			fullQueryArgs.addAll(queryObj.queryArgumentsList());
		}
		
		//==========================================================================
		//
		// Rebuild the order clause, to use the respective
		// joined views in place of the object key
		//
		//==========================================================================
		if (orderByObj != null) {
			// Rebuilt orderByStr
			List<String> rebuiltOrderByList = new ArrayList<>();
			
			// Lets split up the orderby str
			String[] orderSplitting = orderByStr.trim().replaceAll("\\s+", " ").split(",");
			
			// Lets iterate the orderby 
			for (int i = 0; i < orderSplitting.length; ++i) {
				// Get order segment
				String orderSegmentStr = orderSplitting[i].trim();
				
				// Blank string, skip
				if (orderSegmentStr.length() <= 0) {
					continue;
				}
				
				// Lets build the order string
				OrderBy<DataObject> numericOrderBy = new OrderBy<>(orderSegmentStr);
				OrderBy<DataObject> stringOrderBy = new OrderBy<>(orderSegmentStr);
				
				// Lets perform the collumn aliasing replacement
				for (String collumn : numericOrderBy.getKeyNames()) {
					
					// Special handling for _oid
					if (collumn.equalsIgnoreCase("_oid") || collumn.equals("oID")) {
						// Replace for DP.oID
						stringOrderBy.replaceKeyName(collumn, "DP.oID");
						rebuiltOrderByList.add(stringOrderBy.toString());
						continue;
					}

					// Get the replacment table alias
					String collumnTableAlias = objectKeyTableAliasMap.get(collumn);
					
					if ( fixedKeyNames.contains(collumn) ) {

						// Fixed table order by remapping and rewrite
						//------------------------------------------------

						// Get the fixed collumn name
						String fixedTableName = getFixedTableNameFromAlias(collumnTableAlias);
						String fixedTableCollumnName = getFixedTableCollumnName(fixedTableName, collumn);

						stringOrderBy.replaceKeyName(collumn, collumnTableAlias + "." + fixedTableCollumnName);
						rebuiltOrderByList.add(stringOrderBy.toString());

					} else {

						// Dynamic table order by remapping and rewrite
						//------------------------------------------------

						// Lets update the numeric, and string order by settings
						numericOrderBy.replaceKeyName(collumn, collumnTableAlias + ".nVl");
						stringOrderBy.replaceKeyName(collumn, collumnTableAlias + ".sVl");
						
						// Lets append the order by clauses
						rebuiltOrderByList.add(numericOrderBy.toString());
						rebuiltOrderByList.add(stringOrderBy.toString());
					}
				}
			}
			
			// Order by clause is fully rebuilt, lets append it together
			String rebuiltOrderByStr = String.join(",", rebuiltOrderByList.toArray(EmptyArray.STRING));
			
			// Final sanity check?
			orderByObj = getOrderByObject(rebuiltOrderByStr);
			
			//--------------------------------------------------------------------------
			// Update the order clause to fullQuery
			//--------------------------------------------------------------------------
			
			// ORDER BY query is built from orderByObj
			fullQuery.append(" ORDER BY ");
			fullQuery.append(orderByObj.toString().replaceAll("\"", "") + "\n");
		}
		
		//----------------------------------------------------------------------
		// Limit and offset clause handling
		//----------------------------------------------------------------------
		
		// Limit and offset clause handling
		if (limit > 0) {
			fullQuery.append(" LIMIT " + limit);
			if (offset > 0) {
				fullQuery.append(" OFFSET " + offset);
			}
		}
		
		//----------------------------------------------------------------------
		// And finally, the query
		//----------------------------------------------------------------------
		
		// // Original where calause and values
		// System.err.println(">>> " + whereClause);
		// System.err.println(">>> " + ConvertJSON.fromArray(whereValues));
		
		// // In case you want to debug the query =(
		// System.err.println(">>> " + fullQuery.toString());
		// System.err.println(">>> " + ConvertJSON.fromList(fullQueryArgs));
		
		// // Dump and debug the table
		// System.out.println(">>> TABLE DUMP");
		// System.out.println( ConvertJSON.fromMap( sql.select(tablename).readRow(0) ) );
		
		// Execute and get the result
		JSqlResult res = sql.query(fullQuery.toString(), fullQueryArgs.toArray(EmptyArray.OBJECT));
		
		// And return it
		return res;
	}
	
	//-----------------------------------------------------------------------------------------------
	//
	//  Query builder for ID and count
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Performs a search query, and returns the respective DataObjects GUID keys
	 *
	 * CURRENTLY: It is entirely dependent on the whereValues object type to perform the relevent search criteria
	 * @TODO: Performs the search pattern using the respective type map
	 *
	 * @param   JSql connection to use
	 * @param   primaryKeyTable to build the query using
	 * @param   dataStorageTable to build the query using
	 * @param   where query statement
	 * @param   where clause values array
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max
	 *
	 * @return  The String[] array
	 **/
	public String[] dataObjectMapQuery_id( //
		String whereClause, Object[] whereValues, String orderByStr, int offset, int limit //
	) { //
		// Get the complex query
		JSqlResult r = runComplexQuery("DP.oID", whereClause,
			whereValues, orderByStr, offset, limit);
		List<Object> oID_list = r.getObjectList("oID");
		// Generate the object list
		if (oID_list != null) {
			return ListValueConv.objectListToStringArray(oID_list);
		}
		// Blank list as fallback
		return new String[0];
	}
	
	/**
	 * Performs a search query, and returns the respective DataObjects
	 *
	 * CURRENTLY: It is entirely dependent on the whereValues object type to perform the relevent search criteria
	 * @TODO: Performs the search pattern using the respective type map
	 *
	 * @param   JSql connection to use
	 * @param   primaryKeyTable to build the query using
	 * @param   dataStorageTable to build the query using
	 * @param   where query statement
	 * @param   where clause values array
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max
	 *
	 * @return  The DataObject[] array
	 **/
	public long dataObjectMapCount( //
		String whereClause, Object[] whereValues, String orderByStr, int offset, int limit //
	) { //
		JSqlResult r = runComplexQuery("COUNT(*) AS rcount", whereClause, whereValues, orderByStr, offset, limit);
		// Get rcount result
		GenericConvertList<Object> rcountArr = r.get("rcount");
		// Generate the object list
		if (rcountArr != null && rcountArr.size() > 0) {
			return rcountArr.getLong(0);
		}
		// Blank as fallback
		return 0;
	}
	
	//-----------------------------------------------------------------------------------------------
	//
	//  Get command support
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Extracts and build the map stored under an _oid for fixed tables
	 *
	 * @param {String} _oid               - object id to store the key value pairs into
	 * @param {Map<String,Object>} ret    - map to populate, and return, created if null if there is data
	 * 
	 * @returns null/ret object if not exists, else a map (ret) with the data
	 **/
	private Map<String, Object> fixedTableFetch( //
		String _oid, //
		Map<String, Object> ret //
	) {
		// Settings needed from main DataObjectMap
		JSql   sql              = dataMap.sqlObj;
		String dataStorageTable = dataMap.dataStorageTable; 

		// Get fixed table name set
		List<String> fixedTableNameSet = getFixedTableNameList();
		
		// Lets process all the fixed table key names
		//
		// @TODO - optimize this down to a single SQL join query?
		for(String tableName : fixedTableNameSet) {
			// Get the oid collumn
			String oidCollumn = getFixedTableCollumnName(tableName, "_oid");

			// Query the fixed table
			JSqlResult r = sql.select(tableName, "*", oidCollumn+"=?", new Object[] { _oid });

			// No result means no data to extract
			if (r == null || r.get(oidCollumn) == null || r.get(oidCollumn).size() <= 0) {
				continue;
			}

			// OK - looks like there is a data, lets initialize the return map if its null
			if( ret == null ) {
				ret = new HashMap<>();
			}

			// Get the keynames of the table
			Set<String> tableKeyNameSet = getFixedTableObjectKeySet(tableName);
			
			// Lets iterate each table key name
			for(String tableKeyName : tableKeyNameSet) {
				// Get the collumn name
				String collumnName = getFixedTableCollumnName(tableName, tableKeyName);

				// and copy its value over
				ret.put( tableKeyName, r.get(collumnName).get(0) );
			}
		}

		// Return final map
		return ret;
	}

	/**
	 * Extracts and build the map stored under an _oid
	 *
	 * @param {String} _oid               - object id to store the key value pairs into
	 * @param {Map<String,Object>} ret    - map to populate, and return, created if null if there is data
	 * 
	 * @returns null/ret object if not exists, else a map (ret) with the data
	 **/
	public Map<String, Object> jSqlObjectMapFetch( //
		String _oid, //
		Map<String, Object> ret //
	) {
		// Settings needed from main DataObjectMap
		JSql   sql              = dataMap.sqlObj;
		String dataStorageTable = dataMap.dataStorageTable; 

		// Grab data from dynamic tables
		ret = JSql_DataObjectMapUtil.jSqlObjectMapFetch(sql, dataStorageTable, _oid, ret);

		// Grab data from fixed tables
		ret = fixedTableFetch(_oid, ret);

		// Return final map
		return ret;
	}

	//-----------------------------------------------------------------------------------------------
	//
	//  Update command support
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Update the various fixed tables
	 *
	 * @param {String} _oid               - object id to store the key value pairs into
	 * @param {Map<String,Object>} objMap - map to extract values to store from
	 * @param {Set<String>} keyList       - keylist to limit insert load
	 * 
	 * @returns null/ret object if not exists, else a map (ret) with the data
	 **/
	public void fixedTableUpdate( //
		String _oid, //
		Map<String, Object> objMap, //
		Collection<String> keyList //
	) {
		// Get fixed table name set
		List<String> fixedTableNameSet = getFixedTableNameList();
		
		// Lets process all the fixed table names
		for(String tableName : fixedTableNameSet) {
			// Get the oid collumn
			String oidCollumn = getFixedTableCollumnName(tableName, "_oid");

			// Insert key, and values
			List<String> upsertColumns = new ArrayList<>();
			List<Object> upsertValues = new ArrayList<>();

			// Misc collumns (to avoid writting into)
			List<String> miscColumns = new ArrayList<>();

			// Get the keynames of the table
			Set<String> tableKeyNameSet = getFixedTableObjectKeySet(tableName);
			
			// Lets iterate each table key name
			// and build the uniqueColumn/values
			for(String tableKeyName : tableKeyNameSet) {
				// Skip _oid or oID here
				if( tableKeyName.equalsIgnoreCase("_oid") || tableKeyName.equalsIgnoreCase("oid") ) {
					continue;
				}

				// Get the collumn name
				String collumnName = getFixedTableCollumnName(tableName, tableKeyName);

				// Check if the key is to be skipped (misc)
				if( !keyList.contains(tableKeyName) ) {
					miscColumns.add(collumnName);
					continue;
				}

				// Key name is inside key list, this value needs to be set
				upsertColumns.add(collumnName);
				upsertValues.add(objMap.get(tableKeyName));
			}

			// Lets skip the upsert if no data needs to be added
			if( upsertColumns.size() <= 0 ) {
				continue;
			}

			// Perform the upsert
			dataMap.sqlObj.upsert(
				// Table name to upsert on
				tableName, 
				// The unique column names and value
				new String[] { oidCollumn },
				new Object[] { _oid },
				// Upsert collumn and values
				upsertColumns.toArray(EmptyArray.STRING),
				upsertValues.toArray(EmptyArray.OBJECT),
				// Default collumn and values
				null, null,
				// Misc collumns
				miscColumns.toArray(EmptyArray.STRING)
			);
		}
	}

	/**
	 * Update the dynamic data storage table
	 *
	 * @param {String} _oid               - object id to store the key value pairs into
	 * @param {Map<String,Object>} objMap - map to extract values to store from
	 * @param {Set<String>} keyList       - keylist to limit insert load
	 * 
	 * @returns null/ret object if not exists, else a map (ret) with the data
	 **/
	public void jSqlObjectMapUpdate( //
		String _oid, //
		Map<String, Object> objMap, //
		Collection<String> keyList //
	) {
		// Settings needed from main DataObjectMap
		JSql   sql              = dataMap.sqlObj;
		String dataStorageTable = dataMap.dataStorageTable; 

		// Split the key set between dynamic and fixed table collumns
		MutablePair<List<String>,List<String>> dynamicAndFixedKeyPairs = splitCollumnListForDynamicAndFixedQuery(keyList);
		List<String> dynamicKeyNames = dynamicAndFixedKeyPairs.left;
		List<String> fixedKeyNames   = dynamicAndFixedKeyPairs.right;

		// Update data on the dynamic table
		JSql_DataObjectMapUtil.jSqlObjectMapUpdate(sql, dataStorageTable, _oid, objMap, dynamicKeyNames);

		// Update data on the fixed table
		fixedTableUpdate(_oid, objMap, fixedKeyNames);
	}
	
	//-----------------------------------------------------------------------------------------------
	//
	//  Remove/Delete command support
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Delete/Remove configured
	 *
	 * @param {String} _oid - object id to delete
	 **/
	public void jSqlObjectMapRemove( String _oid ) {
		// Settings needed from main DataObjectMap
		JSql   sql              = dataMap.sqlObj;
		String dataStorageTable = dataMap.dataStorageTable; 

		// Delete the data from dynamic table
		//--------------------------------------------------------------------
		sql.delete(dataMap.dataStorageTable, "oID = ?", new Object[] { _oid });
		
		// Delete the data from the fixed table
		//--------------------------------------------------------------------

		// Get fixed table name set
		List<String> fixedTableNameSet = getFixedTableNameList();
		
		// Lets process all the fixed table names
		for(String tableName : fixedTableNameSet) {
			// Get the oid collumn
			String oidCollumn = getFixedTableCollumnName(tableName, "_oid");

			// And call the delete
			sql.delete(tableName, oidCollumn+" = ?", new Object[] { _oid });
		}

		// Delete the parent key
		//--------------------------------------------------------------------
		sql.delete(dataMap.primaryKeyTable, "oID = ?", new Object[] { _oid });
	}
	
}
