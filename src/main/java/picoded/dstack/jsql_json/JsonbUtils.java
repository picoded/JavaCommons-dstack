package picoded.dstack.jsql_json;

import java.util.logging.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import picoded.core.struct.query.OrderBy;
import picoded.core.struct.query.Query;
import picoded.core.struct.query.QueryType;
import picoded.core.struct.query.condition.Or;
import picoded.core.struct.query.condition.Not;
import picoded.core.struct.query.condition.ConditionBase;
import picoded.core.struct.query.internal.QueryFilter;
import picoded.dstack.DataObject;
import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GenericConvert;
import picoded.core.struct.MutablePair;
import picoded.core.common.EmptyArray;
import picoded.core.common.ObjectToken;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Various JSONB specific utilities, used internally to handle data processing
 **/
public class JsonbUtils {
	
	/**
	 * ThreadLocal copy of kryo - implemented as refrenced from
	 * https://hazelcast.com/blog/kryo-serializer/
	 */
	private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			kryo.register(HashMap.class);
			kryo.register(byte[].class);
			// kryo.register(byte.class);
			return kryo;
		}
	};
	
	/**
	 * Serializing the data map into two object pairs consisting of - a JSON string
	 * - serialized data binary (eg. byte[])
	 *
	 * @param {Map<String,Object>} objMap - map to extract values to store from
	 * @param {Set<String>} keySet - used to map the serialized data that needs to be updated
	 *
	 * @return Converted pairs of 2 objects
	 */
	public static MutablePair<String, byte[]> serializeDataMap(Map<String, Object> inMap,
		Set<String> keySet) {
		// The json and bin map, to encode seprately
		Map<String, Object> jsonMap = new HashMap<String, Object>();
		Map<String, Object> binMap = new HashMap<String, Object>();
		
		// Get the full keySet map
		Set<String> fullSet = inMap.keySet();
		
		// Strictly speaking, the implmentation support of other
		// JSQL (non JSON) backend does not support the use of byte[] data
		// in nested dataset.
		//
		// While this is not a limitation of the current backend design
		// we make the same presumption, while avoiding the need
		// for recursive scans / conversion
		if (keySet == null) {
			keySet = fullSet;
		}
		
		// Iterate the key list to apply updates
		for (String k : keySet) {
			// Get the value
			Object v = inMap.get(k);
			
			// Skip reserved key, otm is not allowed to be saved
			// (to ensure blank object is saved)
			if (k.equalsIgnoreCase("_otm")) { // reserved
				continue;
			}
			
			// Key length size protection
			if (k.length() > 64) {
				throw new RuntimeException(
					"Attempted to insert a key value larger then 64 for (_oid = " + inMap.get("_oid")
						+ "): " + k);
			}
			
			// Delete support, ignore NULL values
			if (v == ObjectToken.NULL || v == null) {
				// Skip reserved key, oid key is NOT allowed to be removed directly
				if (k.equalsIgnoreCase("oid") || k.equalsIgnoreCase("_oid")) {
					continue;
				}
			} else if (v instanceof byte[]) {
				// Handling of binary data
				binMap.put(k, v);
			} else {
				// In all other cases, treat it as JSON data
				// we add it to the jsonMap, if its within the keyset
				if (keySet.contains(k)) {
					jsonMap.put(k, v);
				}
			}
		}
		
		// Lets do the required conversions
		String json = ConvertJSON.fromMap(jsonMap);
		byte[] bin = null;
		
		// Count the keyset
		if (binMap.keySet().size() > 0) {
			// Lets encode the binMap
			
			// Get the kyro instance
			Kryo kryo = kryoThreadLocal.get();
			
			// Setup the default byte array stream
			// @CONSIDER: Should we initialize with (16kb buffer?) `new
			// ByteArrayOutputStream(16384)`
			ByteArrayOutputStream BA_OutputStream = new ByteArrayOutputStream();
			DeflaterOutputStream D_OutputStream = new DeflaterOutputStream(BA_OutputStream);
			Output kyroOutput = new Output(D_OutputStream);
			
			// Write the object, into the output stream
			kryo.writeObject(kyroOutput, binMap);
			kyroOutput.close();
			
			// Output into a bin byte[]
			bin = BA_OutputStream.toByteArray();
		}
		
		// Return the full result pair.
		return new MutablePair<String, byte[]>(json, bin);
	}
	
	/**
	 * DeSerializing the data map from the two object pairs consisting of - a JSON
	 * string - serialized data binary (eg. byte[])
	 *
	 * @param {String} the json data string
	 * @param {byte[]} the binary data
	 *
	 * @return Full Map of both data
	 */
	public static Map<String, Object> deserializeDataMap(String jsonData, byte[] binData) {
		// The json map
		Map<String, Object> jsonMap = ConvertJSON.toMap(jsonData);
		
		// if bin data is null, return the json data as it is
		if (binData == null) {
			return jsonMap;
		}
		
		// Lets process the bin data
		ByteArrayInputStream BA_InputStream = new ByteArrayInputStream(binData);
		InflaterInputStream I_InputStream = new InflaterInputStream(BA_InputStream);
		Input kyroInput = new Input(I_InputStream);
		
		// Get the kyro instance
		Kryo kryo = kryoThreadLocal.get();
		
		// Read the binary data map
		Map<String, Object> binMap = kryo.readObject(kyroInput, HashMap.class);
		
		// Build the return map
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.putAll(jsonMap);
		retMap.putAll(binMap);
		
		// And return
		return retMap;
	}
	
	/**
	 * Given the JSON column name, return its remapped value (or its optimized
	 * varients)
	 */
	public static String jsonColumnRemap(String inColumn) {
		
		// Skip the system columns (no need to replace)
		// no change is needed
		if (inColumn.equals("oID")) {
			return "oID";
		}
		
		// Remap _oid to the optimized version instead
		if (inColumn.equals("_oid")) {
			return "oID";
		}
		
		// Return the json mapped column
		return "data->>'" + inColumn.replaceAll("\'", "\\'") + "'";
	}
	
	/**
	 * CustomQueryStr - used to overwrite specific segments that cannot be supported
	 * with the current query implementation
	 */
	protected static class CustomQueryStr implements Query {
		//
		// Constructor Setup
		// --------------------------------------------------------------------
		
		// Custom string overwrite
		public String queryStr = null;
		
		/**
		 * The constructor with the field name, and default argument
		 *
		 * @param default field to test
		 **/
		public CustomQueryStr(String inQueryStr) {
			queryStr = inQueryStr;
		}
		
		/**
		 * The overwritten string value
		 **/
		@Override
		public String toString() {
			return queryStr;
		}
		
		//
		// Unsupported overwrite
		//--------------------------------------------------------------------
		
		@Override
		public Map<String, List<Object>> keyValuesMap(Map<String, List<Object>> arg0) {
			return null;
		}
		
		@Override
		public String operatorSymbol() {
			return null;
		}
		
		@Override
		public boolean test(Object arg0) {
			return false;
		}
		
		@Override
		public boolean test(Object arg0, Map<String, Object> arg1) {
			return false;
		}
		
		@Override
		public QueryType type() {
			return null;
		}
	}
	
	/**
	 * Given the where clause values, build the query specific for JSON based data tables
	 * 
	 * @param whereClause
	 * @param whereValues
	 * @return built query object, to be applied directly
	 */
	public static Query jsonQueryBuilder(String whereClause, Object[] whereValues) {
		// Quick skip if null
		if (whereClause == null) {
			return null;
		}
		
		// The basic Query to build on
		Query queryObj = Query.build(whereClause, whereValues);
		
		// Get the internal fieldQueryMap and queryArgumentsMap
		Map<String, List<Query>> fieldQueryMap = queryObj.fieldQueryMap();
		Map<String, Object> queryArgsMap = queryObj.queryArgumentsMap();
		
		// Lets iterate the fields
		for (String column : fieldQueryMap.keySet()) {
			
			// Get its remapped key
			String remapColumn = JsonbUtils.jsonColumnRemap(column);
			
			// Skip if column is equals
			if (column.equals(remapColumn)) {
				continue;
			}
			
			// Get the various columns that needs to be replaced
			List<Query> toReplaceQueries = fieldQueryMap.get(column);
			
			// // No query filtering was done, therefor, NULL must be suported
			// if (toReplaceQueries == null || toReplaceQueries.size() <= 0) {
			// 	keysWhichMustHandleNullValues.add(column);
			// 	continue;
			// }
			
			// Lets do the replacement, one by one
			for (Query toReplace : toReplaceQueries) {
				
				// The final remap collumn
				String remapColumnWithTypeCast = remapColumn;
				
				// Check if its comparing against an int, if so cast it
				if (toReplace.defaultArgumentValue() instanceof Number) {
					remapColumnWithTypeCast = "(" + remapColumn + ")::numeric";
				}
				
				// Prepare the replacement query
				Query remapQuery = QueryFilter.basicQueryFromTokens( //
					queryArgsMap, remapColumnWithTypeCast, //
					toReplace.operatorSymbol(), ":" + toReplace.argumentName() //
				);
				Query newQuery = remapQuery;
				
				// The element exist queries to build on (if needed)
				// note that ?? translate to ? , the first "?" is used as an escape character
				Query elementExistsQuery = new JsonbUtils.CustomQueryStr("data??'"
					+ column.replaceAll("\'", "\\'") + "'");
				Query elementNotExistsQuery = new JsonbUtils.CustomQueryStr("NOT data??'"
					+ column.replaceAll("\'", "\\'") + "'");
				
				//
				// NULL value operation support
				//
				if (toReplace.operatorSymbol().equalsIgnoreCase("!=")) {
					// Check for inequality check, where NULL must be supported
					// -----
					
					// != NULL support
					if (toReplace.defaultArgumentValue() == null) {
						// Replace with : elementExistsQuery OR != null
						newQuery = new Or(remapQuery, elementExistsQuery, queryArgsMap);
					} else {
						// Allow matching against "NULL" or empty values
						newQuery = new Or(remapQuery, elementNotExistsQuery, queryArgsMap);
					}
					
				} else if ( //
				toReplace.operatorSymbol().equalsIgnoreCase("=") && //
					toReplace.defaultArgumentValue() == null //
				) {
					// Check for equality condition, with NULL values
					// -----
					
					// The NOT( element exist query to build on )
					newQuery = new Or(remapQuery, elementNotExistsQuery, queryArgsMap);
				}
				
				// Replaces old query with new query
				queryObj = queryObj.replaceQuery(toReplace, newQuery);
			}
		}
		
		// Return the full query that was built
		return queryObj;
	}
	
	/**
	 * Given the where clause values, build the query specific for JSON based data tables
	 * 
	 * @param inQuery
	 * @return built query object, to be applied directly
	 */
	public static Query jsonQueryBuilder(Query inQuery) {
		return JsonbUtils.jsonQueryBuilder(inQuery.toSqlString(), inQuery.queryArgumentsArray());
	}
	
	/**
	 * Given a generated jsonQuery, convert it into multablePair parts, for easier usage
	 * @param inQuery
	 * @return
	 */
	public static MutablePair<String, Object[]> queryToPair(Query inQuery) {
		// Quick abort
		if (inQuery == null) {
			return null;
		}
		
		// Normalizing the postgres string
		String queryStr = inQuery.toSqlString().replaceAll("\"(.+?->>.+?)\" ([LIKE=><!NOT]+) \\?",
			"$1 $2 ?");
		queryStr = queryStr.replaceAll("\"oID\" ([LIKE=><!NOT]+) \\?", "oID $1 ?");
		queryStr = queryStr.replaceAll("'oID' ([LIKE=><!NOT]+) \\?", "oID $1 ?");
		
		Object[] queryArgs = inQuery.queryArgumentsArray();
		
		// System.out.println("JSONB : queryToPair conversion");
		// System.out.println(queryStr);
		// System.out.println( ConvertJSON.fromArray(queryArgs) );
		
		// Due to the quoting limitations placed on `Query.toString`, 
		// we need to readjust for JSON compatibility by removing the outer quotes
		// (there is already an inner quote)
		
		// Build the pair
		return new MutablePair<String, Object[]>(queryStr, queryArgs);
	}
	
	/**
	 * Given the where clause values, build the query specific for JSON based data tables
	 * 
	 * @param whereClause
	 * @param whereValues
	 * @return built query object, to be applied directly
	 */
	public static MutablePair<String, Object[]> jsonQueryPairBuilder(String whereClause,
		Object[] whereValues) {
		return queryToPair(jsonQueryBuilder(whereClause, whereValues));
	}
	
	/**
	 * Given the where clause values, build the query specific for JSON based data tables
	 * 
	 * @param whereClause
	 * @param whereValues
	 * @return built query object, to be applied directly
	 */
	public static MutablePair<String, Object[]> jsonQueryPairBuilder(Query inQuery) {
		return queryToPair(jsonQueryBuilder(inQuery));
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
	public static OrderBy<DataObject> jsonOrderByBuilder(String rawString) {
		// Skip if blank
		if (rawString == null) {
			return null;
		}
		
		// Clear out excess whtiespace
		rawString = rawString.trim().replaceAll("\\s+", " ");
		if (rawString.length() <= 0) {
			throw new RuntimeException("Unexpected blank found in OrderBy query : " + rawString);
		}
		
		// Build the object
		OrderBy<DataObject> orderByObj = new OrderBy<DataObject>(rawString);
		
		// Iterate the keynames, and do replacements as needed
		for (String key : orderByObj.getKeyNames()) {
			String newKey = JsonbUtils.jsonColumnRemap(key);
			if (!key.equals(newKey)) {
				orderByObj.replaceKeyName(key, newKey);
			}
		}
		
		// Return the OrderBy object
		return orderByObj;
	}
	
	/**
	 * Sanatize the order by string, and places the field name as query arguments
	 *
	 * @param  Raw order by string
	 *
	 * @return  Order by function obj
	 **/
	public static String jsonOrderByStringBuilder(String rawString) {
		// Get the OrderBy obj
		OrderBy<DataObject> orderByObj = JsonbUtils.jsonOrderByBuilder(rawString);
		if (orderByObj != null) {
			return orderByObj.toString().replaceAll("\"(.*->>.*)\"", "$1");
		}
		return null;
	}
	
	//-----------------------------------------------------------------------------------------------
	//
	//  Raw JSON query builder
	//
	//-----------------------------------------------------------------------------------------------
	
	/**
	 * Builde the "giant complex query" to query against the JSON based backend
	 * @param dataStorageTable    Table name to use
	 * @param selectCol           Column to select
	 * @param whereClause         where query statement
	 * @param whereValues         where clause values array
	 * @param orderByStr          orderBy clause string, to sort result
	 * @param offset              offset of the result to display, use -1 to ignore
	 * @param limit               number of objects to return max, use -1 to ignore
	 * @return
	 */
	public static MutablePair<String, Object[]> fullQueryRawBuilder(String dataStorageTable,
		String selectCol, String whereClause, Object[] whereValues, String orderByStr, int offset,
		int limit) {
		
		// Build the WHERE clause query first
		MutablePair<String, Object[]> reqQuery = JsonbUtils.jsonQueryPairBuilder(whereClause,
			whereValues);
		
		// Build the orderBy object
		String reqOrderByStr = JsonbUtils.jsonOrderByStringBuilder(orderByStr);
		
		// Lets build the FULL query string and args
		StringBuilder fullQuery = new StringBuilder("SELECT " + selectCol + " FROM "
			+ dataStorageTable);
		Object[] fullArgs = EmptyArray.OBJECT;
		
		// Handle WHERE clause
		if (reqQuery != null) {
			fullQuery.append(" WHERE " + reqQuery.left);
			fullArgs = reqQuery.right;
		}
		
		// Handle OrderBy clause (fallsback to oID)
		if (reqOrderByStr != null) {
			fullQuery.append(" ORDER BY " + reqOrderByStr);
		} else if (selectCol.startsWith("COUNT")) {
			// No ordering is needed for COUNT
		} else {
			fullQuery.append(" ORDER BY oID");
		}
		
		// Limit and offset clause
		if (limit > 0) {
			fullQuery.append(" LIMIT " + limit);
			
			if (offset > 0) {
				fullQuery.append(" OFFSET " + offset);
			}
		}
		
		// Return the argument pair
		return new MutablePair<>(fullQuery.toString(), fullArgs);
	}
}