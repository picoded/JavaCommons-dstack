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
 * Custom query string class, whose main (or sole) purpose,
 * is to overwrite the expected SQL toString() / toSqlString()
 * out put with a custom implementation.
 * 
 * This is used internally, to facilitate overwrites for certain,
 * SQL specific edge cases. While proxying all other functionality
 * to the original query.
 * 
 * This allows for common workaround of known JSQL queries, 
 * needing to be rewritten for specific use cases
 */
class JSql_QueryStringOverwrite implements Query {
	
	/**
	 * The original query used internally
	 */
	Query original = null;
	
	/**
	 * Overwrite SQL string to expose
	 */
	String toStringOverwrite = null;
	
	/**
	 * Constructor, with the respective query, and SQL string overwrite
	 */
	public JSql_QueryStringOverwrite(Query inOriginal, String inOverwrite) {
		original = inOriginal;
		toStringOverwrite = inOverwrite;
	}
	
	//
	// Proxy the respective compulsory command to the original query
	//
	@Override
	public boolean test(Object t) {
		return original.test(t);
	}
	
	@Override
	public boolean test(Object t, Map<String, Object> argMap) {
		return original.test(t, argMap);
	}
	
	@Override
	public QueryType type() {
		return original.type();
	}
	
	@Override
	public String operatorSymbol() {
		return original.operatorSymbol();
	}
	
	@Override
	public Map<String, List<Object>> keyValuesMap(Map<String, List<Object>> mapToReturn) {
		return original.keyValuesMap(mapToReturn);
	}
	
	//
	// Operator type overwrites
	//
	
	@Override
	public boolean isBasicOperator() {
		return original.isBasicOperator();
	}
	
	@Override
	public boolean isCombinationOperator() {
		return original.isCombinationOperator();
	}
	
	//
	// Non compulsory overwrites
	//
	
	public List<Query> childrenQuery() {
		return original.childrenQuery();
	}
	
	public String fieldName() {
		return original.fieldName();
	}
	
	public String argumentName() {
		return original.argumentName();
	}
	
	public Map<String, Object> defaultArgumentMap() {
		return original.defaultArgumentMap();
	}
	
	public Object defaultArgumentValue() {
		return original.defaultArgumentValue();
	}
	
	//
	// toString() overwrite, this also effectively overwrites the toSqlString() equilavent
	//
	
	@Override
	public String toString() {
		return toStringOverwrite;
	}
	
}
