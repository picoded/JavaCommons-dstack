package picoded.dstack.connector.jsql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;
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
public abstract class JSql implements StatementBuilderBaseInterface {
	
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
	
	// /**
	//  * SQLite static constructor, returns picoded.dstack.jsql.connector.db.JSql_Sqlite
	//  **/
	// public static JSql sqlite() {
	// 	return new picoded.dstack.jsql.connector.db.JSql_Sqlite();
	// }
	
	// /**
	//  * SQLite static constructor, returns picoded.dstack.jsql.connector.db.JSql_Sqlite
	//  **/
	// public static JSql sqlite(String sqliteLoc) {
	// 	return new picoded.dstack.jsql.connector.db.JSql_Sqlite(sqliteLoc);
	// }
	
	// /**
	//  * MySql static constructor, returns picoded.JSql.JSql_Mysql
	//  **/
	// public static JSql mysql(String dbServerAddress, String dbName, String dbUser, String dbPass) {
	// 	return new picoded.dstack.jsql.connector.db.JSql_Mysql(dbServerAddress, dbName, dbUser,
	// 		dbPass);
	// }
	
	// /**
	//  * MySql static constructor, returns picoded.JSql.JSql_Mysql
	//  **/
	// public static JSql mysql(String connectionUrl, Properties connectionProps) {
	// 	return new picoded.dstack.jsql.connector.db.JSql_Mysql(connectionUrl, connectionProps);
	// }
	
	// /**
	//  * Mssql static constructor, returns picoded.JSql.JSql_Mssql
	//  **/
	// public static JSql mssql(String dbUrl, String dbName, String dbUser, String dbPass) {
	// 	return new picoded.dstack.jsql.connector.db.JSql_Mssql(dbUrl, dbName, dbUser, dbPass);
	// }
	
	// /*
	//  * Oracle static constructor, returns picoded.dstack.jsql.connector.db.JSql_Oracle
	//  */
	// public static JSql oracle(String oraclePath, String dbUser, String dbPass) {
	// 	return new picoded.dstack.jsql.connector.db.JSql_Oracle(oraclePath, dbUser, dbPass);
	// }
	
	// public static JSql oracle(Connection inSqlConn) {
	// 	return new picoded.dstack.jsql.connector.db.JSql_Oracle(inSqlConn);
	// }
	
	//-------------------------------------------------------------------------
	//
	// Database config object constructor
	//
	//-------------------------------------------------------------------------
	
	public static JSql setupFromConfig(Map<String, Object> configObj) {
		GenericConvertMap<String, Object> config = GenericConvertMap.build(configObj);
		
		// Get the minimum string values
		String type = config.getString("type");
		String path = config.getString("path");
		
		// Does validation
		if (type == null || type.isEmpty()) {
			throw new IllegalArgumentException("Missing DB type parameter in DB config object");
		}
		
		/*
		// SQLite handling, only requires path (if given)
		if (type.equalsIgnoreCase("sqlite")) {
			if (path == null || path.isEmpty()) {
				return JSql.sqlite();
			} else {
				return JSql.sqlite(path);
			}
		}
		
		// Remote SQL handling string values
		String name = config.getString("name");
		String user = config.getString("user");
		String pass = config.getString("pass");
		
		// Does validation of remote params
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException("Missing database name in DB config object");
		}
		if (user == null || user.isEmpty()) {
			throw new IllegalArgumentException("Missing login username in DB config object");
		}
		if (pass == null || pass.isEmpty()) {
			throw new IllegalArgumentException("Missing login password in DB config object");
		}
		
		// Does the remote DB connection
		if (type.equalsIgnoreCase("mysql")) {
			return JSql.mysql(path, name, user, pass);
		}
		if (type.equalsIgnoreCase("mssql")) {
			return JSql.mssql(path, name, user, pass);
		}
		 */
		
		// Invalid / Unsupported db type
		throw new IllegalArgumentException("Unsupported DB type in DB config object : " + type);
	}
	
	// //-------------------------------------------------------------------------
	
	// // Database connection settings variables
	// //-------------------------------------------------------------------------
	
	// /**
	//  * Internal refrence of the current sqlType the system is running as
	//  **/
	// protected JSqlType sqlType = JSqlType.INVALID;
	
	// /**
	//  * Java standard database connection
	//  **/
	// protected Connection sqlConn = null;
	
	// /**
	//  * database connection properties
	//  **/
	// protected Map<String, Object> connectionProps = null;
	
	// // Database connection settings functions
	// //-------------------------------------------------------------------------
	
	// /**
	//  * Returns the current sql type, this is read only
	//  *
	//  * @return JSqlType  current implmentation mode
	//  **/
	// public JSqlType sqlType() {
	// 	return this.sqlType;
	// }
	
	// // /**
	// //  * Store the database connection parameters for recreating the connection
	// //  *
	// //  * setup the connection properties, this is normally set by the constructor
	// //  * and is reused via the recreate command.
	// //  *
	// //  * @param  Database location
	// //  * @param  Database name
	// //  * @param  Database username
	// //  * @param  Database password
	// //  * @param  Additional connection properties
	// //  **/
	// // protected void setConnectionProperties(String dbUrl, String dbName, String dbUser,
	// // 	String dbPass, Properties connProps) {
	// // 	connectionProps = new HashMap<String, Object>();
	// // 	if (dbUrl != null) {
	// // 		connectionProps.put("dbUrl", dbUrl);
	// // 	}
	// // 	if (dbName != null) {
	// // 		connectionProps.put("dbName", dbName);
	// // 	}
	// // 	if (dbUser != null) {
	// // 		connectionProps.put("dbUser", dbUser);
	// // 	}
	// // 	if (dbPass != null) {
	// // 		connectionProps.put("dbPass", dbPass);
	// // 	}
	// // 	if (connProps != null) {
	// // 		connectionProps.put("connectionProps", connProps);
	// // 	}
	// // }
	
	// // /**
	// //  * Recreate the current SQL connection.
	// //  * This forcefully close any existing SQL connection, in the process if configured.
	// //  *
	// //  * A common use case would be to forcefully clear and flush temporary sessions
	// //  * or resolve session / memory / ram related issues.
	// //  *
	// //  * @param   Flag to indicate that the connection should be recreated even if it already exists
	// //  **/
	// // public void recreate(boolean force) {
	// // 	throw new UnsupportedOperationException(JSqlException.invalidDatabaseImplementationException);
	// // }
	
	//-------------------------------------------------------------------------
	//
	// Database connection pool handling
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Gets and return the connection from the data source (connection pool)
	 * Connection MUST be "closed" after processing of results
	 * 
	 * @return connection from the pool
	 */
	abstract protected Connection getConn();
	
	// //-------------------------------------------------------------------------
	// //
	// // Standard raw query/execute command sets
	// //
	// //-------------------------------------------------------------------------
	
	// /**
	//  * Executes the argumented SQL query, and immediately fetches the result from
	//  * the database into the result set.
	//  *
	//  * This is a raw execution. As such no special parsing occurs to the request
	//  *
	//  * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	//  *
	//  * @param  Query strings including substituable variable "?"
	//  * @param  Array of arguments to do the variable subtitution
	//  *
	//  * @return  JSQL result set
	//  **/
	// public JSqlResult query_raw(String qString, Object... values) {
	// 	JSqlResult result = noFetchQuery_raw(qString, values);
	// 	if (result != null) {
	// 		result.fetchAllRows();
	// 	}
	// 	return result;
	// }
	
	// /**
	//  * Executes the argumented SQL query, and returns the result object *without*
	//  * fetching the result data from the database.
	//  *
	//  * This is a raw execution. As such no special parsing occurs to the request
	//  *
	//  * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	//  *
	//  * @param  Query strings including substituable variable "?"
	//  * @param  Array of arguments to do the variable subtitution
	//  *
	//  * @return  JSQL result set
	//  **/
	// public JSqlResult noFetchQuery_raw(String qString, Object... values) {
	// 	throw new UnsupportedOperationException(JSqlException.invalidDatabaseImplementationException);
	// }
	
	// /**
	//  * Executes the argumented SQL update.
	//  *
	//  * Returns false if no result object is given by the execution call.
	//  *
	//  * This is a raw execution. As such no special parsing occurs to the request
	//  *
	//  * @param  Query strings including substituable variable "?"
	//  * @param  Array of arguments to do the variable subtitution
	//  *
	//  * @return  -1 if failed, 0 and above for affected rows
	//  **/
	// public int update_raw(String qString, Object... values) {
	// 	JSqlResult r = query_raw(qString, values);
	// 	if (r == null) {
	// 		return -1;
	// 	} else {
	// 		return r.affectedRows();
	// 	}
	// }
	
	// //-------------------------------------------------------------------------
	// //
	// // Generic SQL conversion and query
	// //
	// //-------------------------------------------------------------------------
	
	// /**
	//  * Used for refrence checks / debugging only. This represents the core
	//  * generic SQL statement refactoring engine. That is currenlty used internally
	//  * by query / update. Doing common regex substitutions if needed.
	//  *
	//  * Long term plan is to convert this to a much more proprely structed AST engine.
	//  *
	//  * @param  SQL query to "normalize"
	//  *
	//  * @return  SQL query that was converted
	//  **/
	// public String genericSqlParser(String qString) {
	// 	return qString;
	// }
	
	// /**
	//  * Internal exception catching, used for cases which its not possible to
	//  * easily handle with pure SQL query. Or cases where the performance cost in the
	//  * the query does not justify its usage (edge cases)
	//  *
	//  * This acts as a filter for query, noFetchQuery, and update respectively
	//  *
	//  * @param  SQL query to "normalize"
	//  * @param  The "normalized" sql query
	//  * @param  The exception caught
	//  *
	//  * @return  TRUE, if the exception can be safely ignored
	//  **/
	// protected boolean sanatizeErrors(String originalQuery, String normalizedQuery, JSqlException e) {
	// 	String stackTrace = picoded.core.exception.ExceptionUtils.getStackTrace(e);
	// 	return sanatizeErrors(originalQuery.toUpperCase(), normalizedQuery.toUpperCase(), stackTrace);
	// }
	
	// /**
	//  * Internal exception catching, used for cases which its not possible to
	//  * easily handle with pure SQL query. Or cases where the performance cost in the
	//  * the query does not justify its usage (edge cases)
	//  *
	//  * This is the actual implmentation to overwrite
	//  *
	//  * This acts as a filter for query, noFetchQuery, and update respectively
	//  *
	//  * @param  SQL query to "normalize"
	//  * @param  The "normalized" sql query
	//  * @param  The exception caught, as a stack trace string
	//  *
	//  * @return  TRUE, if the exception can be safely ignored
	//  **/
	// protected boolean sanatizeErrors(String originalQuery, String normalizedQuery, String stackTrace) {
	// 	if (originalQuery.indexOf("DROP TABLE IF EXISTS ") >= 0) {
	// 		if (stackTrace.indexOf("missing database") > 0) {
	// 			return true;
	// 		}
	// 	}
	// 	return false;
	// }
	
	// /**
	//  * Executes the argumented SQL query, and immediately fetches the result from
	//  * the database into the result set.
	//  *
	//  * Custom SQL specific parsing occurs here
	//  *
	//  * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	//  *
	//  * @param  Query strings including substituable variable "?"
	//  * @param  Array of arguments to do the variable subtitution
	//  *
	//  * @return  JSQL result set
	//  **/
	// public JSqlResult query(String qString, Object... values) {
	// 	String parsedQuery = genericSqlParser(qString);
	// 	try {
	// 		return query_raw(parsedQuery, values);
	// 	} catch (JSqlException e) {
	// 		if (sanatizeErrors(qString, parsedQuery, e)) {
	// 			// Sanatization passed, return a token JSqlResult
	// 			return new JSqlResult(null, null, 0);
	// 		} else {
	// 			// If sanatization fails, rethrows error
	// 			throw e;
	// 		}
	// 	}
	// }
	
	// /**
	//  * Executes the argumented SQL query, and returns the result object *without*
	//  * fetching the result data from the database.
	//  *
	//  * Custom SQL specific parsing occurs here
	//  *
	//  * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	//  *
	//  * @param  Query strings including substituable variable "?"
	//  * @param  Array of arguments to do the variable subtitution
	//  *
	//  * @return  JSQL result set
	//  **/
	// public JSqlResult noFetchQuery(String qString, Object... values) {
	// 	String parsedQuery = genericSqlParser(qString);
	// 	try {
	// 		return noFetchQuery_raw(parsedQuery, values);
	// 	} catch (JSqlException e) {
	// 		if (sanatizeErrors(qString, parsedQuery, e)) {
	// 			// Sanatization passed, return a token JSqlResult
	// 			return new JSqlResult(null, null, 0);
	// 		} else {
	// 			// If sanatization fails, rethrows error
	// 			throw e;
	// 		}
	// 	}
	// }
	
	// /**
	//  * Executes the argumented SQL update.
	//  *
	//  * Returns false if no result object is given by the execution call.
	//  *
	//  * Custom SQL specific parsing occurs here
	//  *
	//  * @param  Query strings including substituable variable "?"
	//  * @param  Array of arguments to do the variable subtitution
	//  *
	//  * @return  -1 if failed, 0 and above for affected rows
	//  **/
	// public int update(String qString, Object... values) {
	// 	String parsedQuery = genericSqlParser(qString);
	// 	try {
	// 		return update_raw(parsedQuery, values);
	// 	} catch (JSqlException e) {
	// 		if (sanatizeErrors(qString, parsedQuery, e)) {
	// 			// Sanatization passed, return a token JSqlResult
	// 			return 0;
	// 		} else {
	// 			// If sanatization fails, rethrows error
	// 			throw e;
	// 		}
	// 	}
	// }
	
	// /**
	//  * Prepare an SQL statement, for execution subsequently later
	//  *
	//  * Custom SQL specific parsing occurs here
	//  *
	//  * @param  Query strings including substituable variable "?"
	//  * @param  Array of arguments to do the variable subtitution
	//  *
	//  * @return  Prepared statement
	//  **/
	// public JSqlPreparedStatement prepareStatement(String qString, Object... values) {
	// 	return new JSqlPreparedStatement(qString, values, this);
	// }
	
	// //-------------------------------------------------------------------------
	// //
	// // Connection closure / disposal
	// //
	// //-------------------------------------------------------------------------
	
	// /**
	//  * Returns true, if close() function was called prior
	//  **/
	// public boolean isClosed() {
	// 	return sqlConn == null;
	// }
	
	// /**
	//  * Dispose of the respective SQL driver / connection
	//  **/
	// public void close() {
	// 	// Disposes the instancce connection
	// 	if (sqlConn != null) {
	// 		try {
	// 			//sqlConn.join();
	// 			sqlConn.close();
	// 		} catch (SQLException e) {
	// 			throw new RuntimeException(e);
	// 		}
	// 		sqlConn = null;
	// 	}
	// }
	
	// /**
	//  * Just incase a user forgets to dispose "as per normal"
	//  **/
	// protected void finalize() throws Throwable {
	// 	try {
	// 		close(); // close open files
	// 	} finally {
	// 		super.finalize();
	// 	}
	// }
	
	// //-------------------------------------------------------------------------
	// //
	// // Utility helper functions used to prepare common complex SQL quries
	// //
	// //-------------------------------------------------------------------------
	
	// /**
	//  * Merge the 2 arrays together
	//  * Used to join arguments together
	//  *
	//  * @param  Array of arguments 1
	//  * @param  Array of arguments 2
	//  *
	//  * @return  Resulting array of arguments 1 & 2
	//  **/
	// public Object[] joinArguments(Object[] arr1, Object[] arr2) {
	// 	return org.apache.commons.lang3.ArrayUtils.addAll(arr1, arr2);
	// }
	
	// /**
	//  * Sets the auto commit level
	//  *
	//  * @param  The auto commit level flag to set
	//  **/
	// public void setAutoCommit(boolean autoCommit) {
	// 	try {
	// 		sqlConn.setAutoCommit(autoCommit);
	// 	} catch (Exception e) {
	// 		throw new JSqlException(e);
	// 	}
	// }
	
	// /**
	//  * Gets the current auto commit setting
	//  *
	//  * @return true if auto commit is enabled
	//  **/
	// public boolean getAutoCommit() {
	// 	try {
	// 		return sqlConn.getAutoCommit();
	// 	} catch (Exception e) {
	// 		throw new JSqlException(e);
	// 	}
	// }
	
	// /**
	//  * Runs the commit (use only if setAutoCommit is false)
	//  **/
	// public void commit() {
	// 	try {
	// 		sqlConn.commit();
	// 	} catch (Exception e) {
	// 		throw new JSqlException(e);
	// 	}
	// }
	
}
