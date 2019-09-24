package picoded.dstack.connector.jsql;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.connector.jsql.statement.*;

/**
 * JSql provides a wrapper around several common SQL implementations. Via an (almost) single set of syntax.
 *
 * How this works is by using a core base syntax, which is based off mysql/sqlite. And writing an intermidiary
 * parser for each SQL implementation. To work around its vendor specific issue, and run its respective commands.
 *
 * The major down side, is that there is no true multi statement or transaction support.
 * Not that most programmers actually know how to properly do so.
 *
 * SECURITY NOTE: care should ALWAYS be taken to prevent SQL injection when dealing with query strings.
 **/
public abstract class JSql implements StatementBuilderTableAndIndex, StatementBuilderUpsert,
	StatementBuilderSelect, StatementBuilderRandomSelect, StatementBuilderDelete,
	StatementBuilderInsert {
	
	//-------------------------------------------------------------------------
	//
	// Reusable output logging
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Internal self used logger
	 **/
	protected static final Logger LOGGER = Logger.getLogger(JSql.class.getName());
	
	//-------------------------------------------------------------------------
	//
	// Database specific constructors
	//
	//-------------------------------------------------------------------------
	
	/**
	 * SQLite static constructor, returns picoded.dstack.connector.jsql.JSql_Sqlite
	 **/
	public static JSql sqlite(GenericConvertMap<String, Object> config) {
		return new JSql_Sqlite(config);
	}
	
	/**
	 * Mysql static constructor, returns picoded.dstack.connector.jsql.JSql_Mysql
	 **/
	public static JSql mysql(GenericConvertMap<String, Object> config) {
		return new JSql_Mysql(config);
	}
	
	/**
	 * Mssql static constructor, returns picoded.dstack.connector.jsql.JSql_Mssql
	 **/
	public static JSql mssql(GenericConvertMap<String, Object> config) {
		return new JSql_Mssql(config);
	}
	
	/**
	 * Oracle static constructor, returns picoded.dstack.connector.jsql.JSql_Oracle
	 **/
	public static JSql oracle(GenericConvertMap<String, Object> config) {
		return new JSql_Oracle(config);
	}
	
	/**
	 * Postgres static constructor, returns picoded.dstack.connector.jsql.JSql_Postgres
	 **/
	public static JSql postgres(GenericConvertMap<String, Object> config) {
		return new JSql_Postgres(config);
	}
	
	//-------------------------------------------------------------------------
	//
	// Database config object constructor
	//
	//-------------------------------------------------------------------------
	
	public static JSql setupFromConfig(Map<String, Object> configObj) {
		GenericConvertMap<String, Object> config = GenericConvertMap.build(configObj);
		
		// Get the minimum string values
		String type = config.getString("type");
		
		// Does validation
		if (type == null || type.isEmpty()) {
			throw new IllegalArgumentException("Missing DB type parameter in DB config object");
		}
		
		//
		// SQL type specific support
		//
		if (type.equalsIgnoreCase("sqlite")) {
			return sqlite(config);
		}
		if (type.equalsIgnoreCase("mysql")) {
			return mysql(config);
		}
		if (type.equalsIgnoreCase("mssql")) {
			return mssql(config);
		}
		if (type.equalsIgnoreCase("oracle")) {
			return oracle(config);
		}
		if (type.equalsIgnoreCase("postgres")) {
			return postgres(config);
		}
		
		// Invalid / Unsupported db type
		throw new IllegalArgumentException("Unsupported DB type in DB config object : " + type);
	}
	
	//-------------------------------------------------------------------------------------
	//
	// PreparedStatement builder
	//
	//-------------------------------------------------------------------------------------
	
	/**
	 * Prepare an SQL statement, for execution subsequently later
	 *
	 * Custom SQL specific parsing occurs here
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  Prepared statement
	 **/
	public JSqlPreparedStatement prepareStatement(String qString, Object... values) {
		return new JSqlPreparedStatement(qString, values, this);
	}
	
	//-------------------------------------------------------------------------
	//
	// Database type support
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Returns the current sql type, this is read only
	 *
	 * @return JSqlType  current implmentation mode
	 **/
	public abstract JSqlType sqlType();
	
	//-------------------------------------------------------------------------
	//
	// Connection closure / disposal
	//
	//-------------------------------------------------------------------------
	
	/**
	 * @return true, if close() function was called prior
	 **/
	public abstract boolean isClosed();
	
	/**
	 * Dispose of the respective SQL driver / connection
	 **/
	public abstract void close();
	
	/**
	 * Just incase a user forgets to dispose "as per normal"
	 **/
	protected void finalize() throws Throwable {
		try {
			close(); // close open files?
		} catch (Exception ex) {
			JSql.LOGGER.log(Level.WARNING, ex.getMessage(), ex);
		}
	}
	
	//-------------------------------------------------------------------------
	//
	// Utility helper functions used to prepare common complex SQL quries
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Merge the 2 arrays together
	 * Used to join arguments together
	 *
	 * @param  Array of arguments 1
	 * @param  Array of arguments 2
	 *
	 * @return  Resulting array of arguments 1 & 2
	 **/
	public Object[] joinArguments(Object[] arr1, Object[] arr2) {
		return org.apache.commons.lang3.ArrayUtils.addAll(arr1, arr2);
	}
	
}
