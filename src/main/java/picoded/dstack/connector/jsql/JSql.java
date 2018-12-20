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
	protected abstract void finalize() throws Throwable;
	
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
