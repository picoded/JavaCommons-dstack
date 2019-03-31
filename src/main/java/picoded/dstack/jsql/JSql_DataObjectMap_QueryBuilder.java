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
	//  Utility functions
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
	//  Internal query builder
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Lets build the core inner join query string, 
	 * given the required filtered collumn names.
	 * 
	 * Its expected result without any collumns provided would be
	 * 
	 * ```
	 * (SELECT oID FROM DP_TABLENAME) AS DP
	 * ```
	 * 
	 * Alternatively, if collumn names are provided (as part of the WHERE / ORDER BY clause),
	 * it will generate an additional inner join line
	 * 
	 * ```
	 * (SELECT oID FROM DP_TABLENAME) AS DP
	 * INNER JOIN (SELECT oID, nVl, sVl, tVl FROM DD_TABLENAME WHERE kID="softDelete") AS D0 ON (DP.oID = D0.oID)
	 * INNER JOIN (SELECT oID, nVl, sVl, tVl FROM DD_TABLENAME WHERE kID="sourceOfLead") AS D1 ON (DP.oID = D1.oID)
	 * ```
	 * 
	 * @param  primaryKeyTable to build the query using
	 * @param  dataStorageTable to build the query using
	 * @param  collumns that is needed, in the given order
	 * 
	 * @return pair of query string, with query args
	 */
	private static MutablePair<StringBuilder, List<Object>> innerJoinBuilder(String primaryKeyTable,
		String dataStorageTable, List<String> collumns) {
		// The query string to build
		StringBuilder queryStr = new StringBuilder();
		List<Object> queryArg = new ArrayList<>();
		
		// oID collumn first
		queryStr.append("(SELECT oID FROM ").append(primaryKeyTable).append(") AS DP \n");
		
		// No collumns required (fast ending)
		if (collumns == null || collumns.size() <= 0) {
			return new MutablePair<>(queryStr, queryArg);
		}
		
		// For each collumn that is required, perform an inner join
		for (int i = 0; i < collumns.size(); ++i) {
			// Single collumn inner join
			queryStr.append("INNER JOIN (SELECT oID, nVl, sVl, tVl FROM ").append(dataStorageTable) //
				.append(" WHERE kID=? AND idx=?) AS D" + i + " ON (") //
				.append("DP.oID = D" + i + ".oID) \n");
			// With arguments
			queryArg.add(collumns.get(i));
			queryArg.add(0);
		}
		
		// The inner join query
		return new MutablePair<>(queryStr, queryArg);
	}
	
	/**
	 * Performs a search query, and returns the respective result containing the DataObjects information
	 * This works by taking the query and its args, building its complex inner view, then querying that view.
	 *
	 * @param   JSql connection to use
	 * @param   primaryKeyTable to build the query using
	 * @param   dataStorageTable to build the query using
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
		JSql sql, String primaryKeyTable, String dataStorageTable, String selectedCols, //
		String whereClause, Object[] whereValues, String orderByStr, int offset, int limit //
	) { //
	
		//--------------------------------------------------------------------------
		// Quick optimal lookup : to the parent oID table.
		// Does not do any complex building of clauses
		// Runs the query and exit immediately
		//--------------------------------------------------------------------------
		
		if (whereClause == null && orderByStr == null && offset <= 0 && limit <= 0
		// ---
		// Note the following checks has been commented out as in its
		// current use case, its always "true"
		// ---
		// && (
		// 	selectedCols.equalsIgnoreCase("DISTINCT DP.oID") || 
		// 	selectedCols.equalsIgnoreCase("COUNT(DISTINCT DP.oID) AS rcount")
		// 	selectedCols.equalsIgnoreCase("oID") || 
		// 	selectedCols.equalsIgnoreCase("DISTINCT oID") || 
		// 	selectedCols.equalsIgnoreCase("COUNT(DISTINCT oID) AS rcount")
		// )
		) {
			// Blank query search, quick and easy
			return sql.select(primaryKeyTable, selectedCols.replaceAll("DP.oID", "oID"));
		}
		
		//--------------------------------------------------------------------------
		// Sadly looks like things must be done the hardway, 
		// lets build the query and orderby clause objects
		// and extract out the collumn names
		//--------------------------------------------------------------------------
		
		// The where clause query object, that is built, and actually used
		Query queryObj = null;
		
		// Result ordering by
		OrderBy<DataObject> orderByObj = null;
		
		// Collumn name sets
		Set<String> rawWhereClauseCollumns = null;
		Set<String> rawOrderByClauseCollumns = null;
		
		// List of collumns that is needed for both where / order by
		Set<String> rawCollumnNameSet = new HashSet<>();
		
		// Where clause exists, build it!
		if (whereClause != null && whereClause.length() >= 0) {
			// The complex query object
			queryObj = Query.build(whereClause, whereValues);
			
			// Get the collumn keynames
			rawWhereClauseCollumns = queryObj.keyValuesMap().keySet();
			rawCollumnNameSet.addAll(rawWhereClauseCollumns);
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
		// Sort out and filter the required collumnNameSet
		//--------------------------------------------------------------------------
		
		// List of collumn names for the inner query builder
		List<String> collumnNames = new ArrayList<>();
		
		// alias mapping of the collumn names
		Map<String, String> collumnAliasMap = new HashMap<>();
		
		// For each collumnName in the collumnNameSet, set it up if applicable
		for (String collumn : rawCollumnNameSet) {
			
			// Collumn names to skip setup (reseved keywords?)
			if (collumn.equalsIgnoreCase("_oid")) {
				continue;
			}
			
			// collumn nmaes that requires setup
			collumnAliasMap.put(collumn, "D" + collumnNames.size());
			// note: registering alias map, before adding to list is intentional
			collumnNames.add(collumn);
		}
		
		//--------------------------------------------------------------------------
		// Build the complex inner join table
		//--------------------------------------------------------------------------
		
		// Final query string builder and arguments
		StringBuilder fullQuery = new StringBuilder();
		List<Object> fullQueryArgs = new ArrayList<>();
		
		// The select clause
		fullQuery.append("SELECT ").append(selectedCols).append(" FROM \n");
		
		// the inner join 
		MutablePair<StringBuilder, List<Object>> innerJoinPair = innerJoinBuilder(primaryKeyTable,
			dataStorageTable, collumnNames);
		
		// Merged together with full query, with the inner join clauses
		fullQuery.append(innerJoinPair.left);
		fullQueryArgs.addAll(innerJoinPair.right);
		
		//--------------------------------------------------------------------------
		// Update the query clauses collumn linkage
		//--------------------------------------------------------------------------
		if (queryObj != null) {
			
			// Gets the original field to "raw query" maps
			// of keys, to do subtitution on,
			// and their respective argument map.
			Map<String, List<Query>> fieldQueryMap = queryObj.fieldQueryMap();
			Map<String, Object> queryArgMap = queryObj.queryArgumentsMap();
			
			// Gets the new index position to add new arguments if needed
			int newQueryArgsPos = queryArgMap.size() + 1;
			
			// Lets iterate through the collumn names
			for (String collumn : rawWhereClauseCollumns) {
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
				String collumnTableAlias = collumnAliasMap.get(collumn);
				
				// Scan for the query to perform replacements
				for (Query toReplace : toReplaceQueries) {
					// Get the argument
					Object argObj = queryArgMap.get(toReplace.argumentName());
					
					// Setup the replacement query
					Query replacement = null;
					
					// Does special numeric handling
					if (argObj == null) {
						replacement = QueryFilter.basicQueryFromTokens(queryArgMap, collumnTableAlias
							+ ".sVl", toReplace.operatorSymbol(), ":" + toReplace.argumentName() //
						);
					} else if (argObj instanceof Number) {
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
						}
					}
					
					// Unprocessed arg type
					if (replacement == null) {
						throw new RuntimeException("Unexpeced query argument (unkown type) : " + argObj);
					}
					
					// Replaces old query with new query
					queryObj = queryObj.replaceQuery(toReplace, replacement);
				}
			}
			
			//--------------------------------------------------------------------------
			// Update the query clauses collumn linkage, and apply to fullQuery
			//--------------------------------------------------------------------------
			
			// WHERE query is built from queryObj, this acts as a form of sql sanitization
			fullQuery.append("WHERE ");
			fullQuery.append(queryObj.toSqlString().replaceAll("\"", ""));
			fullQueryArgs.addAll(queryObj.queryArgumentsList());
		}
		
		//--------------------------------------------------------------------------
		// Update the order by clauses collumn linkage
		//--------------------------------------------------------------------------
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
				OrderBy<DataObject> numericOrderBy = new OrderBy<DataObject>(orderSegmentStr);
				OrderBy<DataObject> stringOrderBy = new OrderBy<DataObject>(orderSegmentStr);
				
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
					String collumnTableAlias = collumnAliasMap.get(collumn);
					
					// Lets update the numeric, and string order by settings
					numericOrderBy.replaceKeyName(collumn, collumnTableAlias + ".nVl");
					stringOrderBy.replaceKeyName(collumn, collumnTableAlias + ".sVl");
					
					// Lets append the order by clauses
					rebuiltOrderByList.add(numericOrderBy.toString());
					rebuiltOrderByList.add(stringOrderBy.toString());
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
		
		// // In case you want to debug the query =(
		// System.err.println(">>> " + fullQuery.toString());
		// System.err.println(">>> " + ConvertJSON.fromList(fullQueryArgs));
		
		// // Dump and debug the table
		// System.out.println(">>> TABLE DUMP");
		// System.out.println( ConvertJSON.fromMap( sql.select(tablename).readRow(0) ) );
		
		// Execute and get the result
		return sql.query(fullQuery.toString(), fullQueryArgs.toArray(EmptyArray.OBJECT));
	}
	
	//-----------------------------------------------------------------------------------------------
	//
	//  Query builder
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
	public static String[] dataObjectMapQuery_id( //
		// The meta table / sql configs
		JSql sql, String primaryKeyTable, String dataStorageTable, //
		// The actual query
		String whereClause, Object[] whereValues, String orderByStr, int offset, int limit //
	) { //
		JSqlResult r = runComplexQuery(sql, primaryKeyTable, dataStorageTable, "DP.oID", whereClause,
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
	public static long dataObjectMapCount( //
		// The meta table / sql configs
		JSql sql, String primaryKeyTable, String dataStorageTable, //
		// The actual query
		String whereClause, Object[] whereValues, String orderByStr, int offset, int limit //
	) { //
		JSqlResult r = runComplexQuery(sql, primaryKeyTable, dataStorageTable,
			"COUNT(DP.oID) AS rcount", whereClause, whereValues, orderByStr, offset, limit);
		// Get rcount result
		GenericConvertList<Object> rcountArr = r.get("rcount");
		// Generate the object list
		if (rcountArr != null && rcountArr.size() > 0) {
			return rcountArr.getLong(0);
		}
		// Blank as fallback
		return 0;
	}
	
}