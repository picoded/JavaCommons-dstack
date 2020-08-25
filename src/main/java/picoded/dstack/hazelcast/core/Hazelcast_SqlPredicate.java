package picoded.dstack.hazelcast.core;

// Java imports
import java.io.Serializable;
import java.util.*;

// JavaCommons imports
import picoded.core.struct.query.Query;
import picoded.core.conv.StringEscape;

// Hazelcast implementation
import com.hazelcast.query.*;

/**
 * Build the predicate using the hazelcast SQL if possible
 * Fallsback to JC query predicate otherwise.
 */
public class Hazelcast_SqlPredicate implements Predicate<String, Map<String, Object>>, Serializable {
	
	// Sqerializable Query string to use 
	public String _queryString = null;
	
	// Sqerializable Query arguments list
	public Object[] _queryArgs = null;
	
	/**
	 * Build the Hazelcast_SqlPredicate using the javacommons predicate
	 * 
	 * @param  originalQuery  javacommons query to remap for hazelcast
	 */
	public Hazelcast_SqlPredicate(Query originalQuery) {
		_queryString = originalQuery.toSqlString();
		_queryArgs = originalQuery.queryArgumentsArray();
	}
	
	// The underlying Query class is not serialize intentionally
	// to help reduce overall serializable size
	private transient Query _localQuery = null;
	
	/**
	 * Apply the predicate against the internal hazlecast store
	 */
	public boolean apply(Map.Entry<String, Map<String, Object>> mapEntry) {
		// Initialize the local query if needed
		if (_localQuery == null) {
			_localQuery = Query.build(_queryString, _queryArgs);
		}
		
		// Apply the query against the value
		return _localQuery.test(mapEntry.getValue());
	}
	
	//------------------------------------------------------------------
	//
	//  Predicate builder
	//
	//------------------------------------------------------------------
	
	/**
	 * Build the Hazelcast compatible predicate using the javacommons predicate
	 * 
	 * @param  originalQuery  javacommons query to remap for hazelcast
	 */
	public static Predicate<String, Map<String, Object>> build(Query originalQuery) {
		// Use the hazelcast predicatre if possible
		try {
			return Predicates.sql(queryStringify(originalQuery));
		} catch (Exception e) {
			// does nothing - use the full fallback
		}
		
		// Full fallback
		return new Hazelcast_SqlPredicate(originalQuery);
	}
	
	/**
	 * Converts a conv.Query into a full SQL string
	 **/
	protected static String queryStringify(Query queryClause) {
		
		// Converts into SQL string with ? value clause, and its arguments value
		String sqlString = queryClause.toSqlString();
		
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
		
		// Iterate each sql argument, and inject it
		// Note: This is now dropped as 4.0, with proper args support?
		Object[] sqlArgs = queryClause.queryArgumentsArray();
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
	
}