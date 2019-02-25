package picoded.dstack.connector.jsql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.zaxxer.hikari.*;

import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.CaseInsensitiveHashMap;
import picoded.core.struct.MutablePair;

/**
 * Default generic JSQL implmentation,
 * with shared usage across multiple DB's
 * while not being usable for any of them on its own.
 **/
public abstract class JSql_Base extends JSql {
	
	//-------------------------------------------------------------------------
	//
	// Database type support
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Internal refrence of the current sqlType the system is running as
	 **/
	protected JSqlType sqlType = JSqlType.INVALID;
	
	/**
	 * Returns the current sql type, this is read only
	 *
	 * @return JSqlType  current implmentation mode
	 **/
	public JSqlType sqlType() {
		return this.sqlType;
	}
	
	//-------------------------------------------------------------------------
	//
	// HikariCP data connection
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Internal HirkariDataSource, used for connection pooling and requests
	 */
	protected HikariDataSource datasource = null;
	
	//-------------------------------------------------------------------------
	//
	// Connection closure / disposal
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Returns true, if close() function was called prior
	 **/
	public boolean isClosed() {
		return datasource == null;
	}
	
	/**
	 * Dispose of the respective SQL driver / connection
	 **/
	public void close() {
		// Disposes the instancce connection
		if (datasource != null) {
			try {
				datasource.close();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			datasource = null;
		}
	}
	
	//-------------------------------------------------------------------------
	//
	// Internal utility functions
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helper function, used to prepare the sql statment in multiple situations
	 * to a PreparedStatement object. 
	 * 
	 * IMPORTANT NOTE : This should not be confused with JSqlPreparedStatement,
	 * which is a proxy place holder to facilitate this known usage pattern.
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  The SQL prepared statement
	 **/
	protected PreparedStatement prepareSqlStatment(Connection sqlConn, String qString,
		Object... values) {
		int pt = 0;
		final Object[] parts = (values != null) ? values : (new Object[] {});
		
		Object argObj = null;
		PreparedStatement ps = null;
		
		try {
			ps = sqlConn.prepareStatement(qString);
			
			for (pt = 0; pt < parts.length; ++pt) {
				argObj = parts[pt];
				if (argObj == null) {
					ps.setNull(pt + 1, 0);
				} else if (String.class.isInstance(argObj)) {
					ps.setString(pt + 1, (String) argObj);
				} else if (Integer.class.isInstance(argObj)) {
					ps.setInt(pt + 1, (Integer) argObj);
				} else if (Long.class.isInstance(argObj)) {
					ps.setLong(pt + 1, (Long) argObj);
				} else if (Double.class.isInstance(argObj)) {
					ps.setDouble(pt + 1, (Double) argObj);
				} else if (Float.class.isInstance(argObj)) {
					ps.setFloat(pt + 1, (Float) argObj);
				} else if (Date.class.isInstance(argObj)) {
					java.sql.Date sqlDate = new java.sql.Date(((Date) argObj).getTime());
					ps.setDate(pt + 1, sqlDate);
				} else if (argObj instanceof byte[]) {
					ps.setBytes(pt + 1, (byte[]) argObj);
				} else if (java.sql.Blob.class.isInstance(argObj)) {
					ps.setBlob(pt + 1, (java.sql.Blob) argObj);
				} else {
					String argClassName = argObj.getClass().getName();
					throw new JSqlException("Unknown argument type (" + pt + ") : " + (argClassName));
				}
			}
		} catch (Exception e) {
			// An exception occured, try to close the half initialized prepared statement
			try {
				if (ps != null) {
					ps.close();
				}
				if (sqlConn != null) {
					sqlConn.close();
				}
			} catch (Exception ex) {
				JSql.LOGGER.log(Level.WARNING, ex.getMessage(), ex);
			}
			
			// Throw the JSqlException
			throw new JSqlException("Invalid statement argument/parameter (" + pt + ")", e);
		}
		return ps;
	}
	
	//-------------------------------------------------------------------------
	//
	// Raw SQL query support
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Executes the argumented SQL query, and immediately fetches the result from
	 * the database into the result set.
	 *
	 * This is a raw execution. As such no special parsing occurs to the request
	 *
	 * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  JSQL result set
	 **/
	public JSqlResult query_raw(String qString, Object... values) {
		// Connection variable (to setup inside try,catch,finally)
		Connection conn = null;
		PreparedStatement sqlpstmt = null;
		
		// Get the connection, and perform the query request
		// within a try-catch block
		try {
			// Getting the connection
			conn = datasource.getConnection();
			
			// Prepare the statement
			sqlpstmt = prepareSqlStatment(conn, qString, values);
			System.out.println("JSql_Base query : " + qString);
			for (Object value : values)
				System.out.println("args : " + value.toString());
			// Performing the query, get the result set, and immediately pass it to JSqlResult
			//
			// Note: internally JSqlResult, already does a try,catch,finally to close the result set
			// as such there isnt a need for an additional close check within this try,catch
			return new JSqlResult(sqlpstmt.executeQuery());
		} catch (Exception e) {
			throw new JSqlException(e);
		} finally {
			// Try to close the SQL Prepared statment, and connection (if not previously closed)
			// and log any error occured while trying to close the connection
			try {
				if (sqlpstmt != null) {
					sqlpstmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception ex) {
				JSql.LOGGER.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
	}
	
	/**
	 * Executes the argumented SQL update.
	 *
	 * Returns -1 if no result object is given by the execution call.
	 * Else returns the number of rows affected
	 *
	 * This is a raw execution. As such no special parsing occurs to the request
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  -1 if failed, 0 and above for affected rows
	 **/
	public int update_raw(String qString, Object... values) {
		// Connection variable (to setup inside try,catch,finally)
		Connection conn = null;
		PreparedStatement sqlpstmt = null;
		
		System.out.println("<------------");
		System.out.println(qString);
		qString = qString.replaceAll("AUTOINCREMENT", "");
		
		// Get the connection, and perform the query request
		// within a try-catch block
		try {
			// Getting the connection
			conn = datasource.getConnection();
			
			// Prepare the statement
			sqlpstmt = prepareSqlStatment(conn, qString, values);
			System.out.println("PREPARED SQL STATEMENT:");
			System.out.println(sqlpstmt.toString());
			System.out.println("------------>");
			
			// Performing the query, get the affected row count
			return sqlpstmt.executeUpdate();
		} catch (Exception e) {
			throw new JSqlException(e);
		} finally {
			// Try to close the SQL Prepared statment, and connection (if not previously closed)
			// and log any error occured while trying to close the connection
			try {
				if (sqlpstmt != null) {
					sqlpstmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception ex) {
				JSql.LOGGER.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
	}
	
	//-------------------------------------------------------------------------
	//
	// Table Column type info map
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Executes and fetch a table column information as a map, note that due to the 
	 * HIGHLY different standards involved across SQL backends for this command, 
	 * it has been normalized to only return a map containing collumn name and types
	 * 
	 * Furthermore due to the generic SQL conversion from known common types to SQL specific
	 * type being applied on table create. The collumn type may not match the input collumn
	 * type previously applied on table create. (Unless update_raw was used)
	 * 
	 * This immediately executes a query, and process the information directly 
	 * (to normalize the results across SQL implementations).
	 * 
	 * Note : returned map should be a `CaseInsensitiveHashMap`
	 *
	 * @param  tablename to get information on
	 *
	 * @return  Collumn name to type mapping
	 **/
	public GenericConvertMap<String, String> getTableColumnTypeMap(String tablename) {
		// Prepare return information map
		GenericConvertMap<String, String> ret = new CaseInsensitiveHashMap<String, String>();
		
		// Remove quotations in table name, and trim out excess whitespace
		tablename = tablename.replaceAll("`", "").replaceAll("'", "").replaceAll("\"", "").trim();
		
		// Get the column information
		MutablePair<GenericConvertList<Object>, GenericConvertList<Object>> columnTypePair = getTableColumnTypeMap_core(tablename);
		
		// Parse it into a map format
		GenericConvertList<Object> column_name = columnTypePair.getLeft();
		GenericConvertList<Object> column_type = columnTypePair.getRight();
		
		// Iterate name/type, and get the info
		for (int i = 0; i < column_name.size(); ++i) {
			ret.put(column_name.getString(i), column_type.getString(i).toUpperCase());
		}
		
		// Return the meta map
		return ret;
	}
	
	//-------------------------------------------------------------------------
	//
	// Table Column type info map internal implementation
	// [TO OVERWRITE AND EXTEND]
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Executes and fetch a table column information as a map, note that due to the 
	 * HIGHLY different standards involved across SQL backends for this command, 
	 * it has been normalized to only return a map containing collumn name and types
	 * 
	 * Furthermore due to the generic SQL conversion from known common types to SQL specific
	 * type being applied on table create. The collumn type may not match the input collumn
	 * type previously applied on table create. (Unless update_raw was used)
	 * 
	 * This immediately executes a query, and process the information directly 
	 * (to normalize the results across SQL implementations).
	 * 
	 * Note : returned map should be a `CaseInsensitiveHashMap`
	 *
	 * @param  tablename to get information on
	 *
	 * @return  Pair containing < collumn_name, collumn_type >
	 **/
	protected MutablePair<GenericConvertList<Object>, GenericConvertList<Object>> getTableColumnTypeMap_core(
		String tablename) {
		throw new UnsupportedOperationException(
			"getTableColumnTypeMap for given SQL type is not supported yet");
	}
	
	//-------------------------------------------------------------------------
	//
	// Generic SQL conversion, and error sanatization
	// [TO OVERWRITE AND EXTEND]
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Used for refrence checks / debugging only. This represents the core
	 * generic SQL statement refactoring engine. That is currenlty used internally
	 * by query / update. Doing common regex substitutions if needed.
	 *
	 * Long term plan is to convert this to a much more proprely structed AST engine.
	 *
	 * @param  SQL query to "normalize"
	 *
	 * @return  SQL query that was converted
	 **/
	public String genericSqlParser(String qString) {
		return qString;
	}
	
	/**
	 * Internal exception catching, used for cases which its not possible to
	 * easily handle with pure SQL query. Or cases where the performance cost in the
	 * the query does not justify its usage (edge cases)
	 *
	 * This is the actual implmentation to overwrite
	 *
	 * This acts as a filter for query, noFetchQuery, and update respectively
	 *
	 * @param  SQL query to "normalize"
	 * @param  The "normalized" sql query
	 * @param  The exception caught, as a stack trace string
	 *
	 * @return  TRUE, if the exception can be safely ignored
	 **/
	protected boolean sanatizeErrors(String originalQuery, String normalizedQuery, String stackTrace) {
		if (originalQuery.indexOf("DROP TABLE IF EXISTS ") >= 0) {
			if (stackTrace.indexOf("missing database") > 0) {
				return true;
			}
		}
		return false;
	}
	
	//-------------------------------------------------------------------------
	//
	// Generic SQL query and update calls (with sanitization of errors)
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Internal exception catching, used for cases which its not possible to
	 * easily handle with pure SQL query. Or cases where the performance cost in the
	 * the query does not justify its usage (edge cases)
	 *
	 * This acts as a filter for query, noFetchQuery, and update respectively
	 *
	 * @param  SQL query to "normalize"
	 * @param  The "normalized" sql query
	 * @param  The exception caught
	 *
	 * @return  TRUE, if the exception can be safely ignored
	 **/
	protected boolean sanatizeErrors(String originalQuery, String normalizedQuery, JSqlException e) {
		String stackTrace = picoded.core.exception.ExceptionUtils.getStackTrace(e);
		return sanatizeErrors(originalQuery.toUpperCase(), normalizedQuery.toUpperCase(), stackTrace);
	}
	
	/**
	 * Executes the argumented SQL query, and immediately fetches the result from
	 * the database into the result set.
	 *
	 * Custom SQL specific parsing occurs here
	 *
	 * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  JSQL result set
	 **/
	public JSqlResult query(String qString, Object... values) {
		String parsedQuery = genericSqlParser(qString);
		try {
			return query_raw(parsedQuery, values);
		} catch (JSqlException e) {
			if (sanatizeErrors(qString, parsedQuery, e)) {
				// Sanatization passed, return a token JSqlResult
				return new JSqlResult(null, 0);
			} else {
				// If sanatization fails, rethrows error
				throw e;
			}
		}
	}
	
	/**
	 * Executes the argumented SQL update.
	 *
	 * Returns false if no result object is given by the execution call.
	 *
	 * Custom SQL specific parsing occurs here
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  -1 if failed, 0 and above for affected rows
	 **/
	public int update(String qString, Object... values) {
		String parsedQuery = genericSqlParser(qString);
		try {
			return update_raw(parsedQuery, values);
		} catch (JSqlException e) {
			if (sanatizeErrors(qString, parsedQuery, e)) {
				// Sanatization passed, return a token JSqlResult
				return 0;
			} else {
				// If sanatization fails, rethrows error
				throw e;
			}
		}
	}
	
}
