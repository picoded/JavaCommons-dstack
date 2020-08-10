package picoded.dstack.hazelcast.core;

// Java imports
import java.io.Serializable;
import java.util.Map;

// JavaCommons imports
import picoded.core.struct.query.Query;

// Hazelcast implementation
import com.hazelcast.query.*;

/**
 * This replaces the previous SqlPredicate that was present in 3.11
 * which was dropped on 4.0 onwards
 * 
 * This reimplements a serializable Query, that is compliant with hazelcast.
 * 
 * Note that ideally we should rebuild the query using the hazelcast predicateBuilder
 * instead, so that it would be able to optimize for any index used in the map.
 */
public class Hazelcast_SqlPredicate implements Predicate<String,Map<String,Object>>,Serializable {
	
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
	public boolean apply(Map.Entry<String,Map<String,Object>> mapEntry) {
		// Initialize the local query if needed
		if( _localQuery == null ) {
			_localQuery = Query.build(_queryString, _queryArgs);
		}

		// Apply the query against the value
		return _localQuery.test( mapEntry.getValue() );
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
	public static Predicate<String,Map<String,Object>> build(Query originalQuery) {

		// @TODO - optimize simple query to the respective hazelcast predicate

		// Full fallback
		return new Hazelcast_SqlPredicate(originalQuery);
	}
}