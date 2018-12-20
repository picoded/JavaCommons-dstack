package picoded.dstack.jsql.connector.jsql;

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
import java.util.logging.Logger;

import picoded.dstack.jsql.connector.JSqlType;
import picoded.dstack.jsql.connector.*;

/**
 * Default generic JSQL implmentation,
 * with shared usage across multiple DB's
 * while not being usable for any of them on its own.
 **/
public class JSql_Base extends JSql {
	
	//-------------------------------------------------------------------------
	//
	// Database keywords, reuse vars
	//
	//-------------------------------------------------------------------------
	
	protected static final String COALESCE = "COALESCE(";
	protected static final String WHERE = " WHERE ";
	
	/**
	 * Blank JSQL result set, initialized once and reused
	 **/
	// protected static final JSqlResult BLANK_JSQLRESULT = new JSqlResult();
	
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
	
	// //-------------------------------------------------------------------------
	// //
	// // Database connection pool handling
	// //
	// //-------------------------------------------------------------------------
	
	// /**
	//  * Gets and return the connection from the data source (connection pool)
	//  * Connection MUST be "closed" after processing of results
	//  * 
	//  * @return connection from the pool
	//  */
	// abstract protected Connection getConn();
	
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
	
	//-------------------------------------------------------------------------
	//
	// Helper utility functions
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helper function, used to prepare the sql statment in multiple situations
	 * to a PreparedStatement object. 
	 * 
	 * IMPORTANT NOTE : This should not be confused with JSqlPreparedStatement,
	 * as this is a place holder to facilitate this known usage pattern.
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  The SQL prepared statement
	 **/
	protected PreparedStatement prepareSqlStatment(String qString, Object... values) {
		int pt = 0;
		final Object[] parts = (values != null) ? values : (new Object[] {});
		
		Object argObj;
		PreparedStatement ps;
		
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
			throw new JSqlException("Invalid statement argument/parameter (" + pt + ")", e);
		}
		return ps;
	}
	
	//-------------------------------------------------------------------------
	//
	// Generic SQL conversion and query
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
				return new JSqlResult(null, null, 0);
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
	
	//-------------------------------------------------------------------------
	//
	// SELECT Query builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 *
	 * Helps generate an SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors (if any).
	 *
	 * The syntax below, is an example of such an SELECT statement for SQLITE.
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * SELECT
	 *	col1, col2   //select collumn
	 * FROM tableName //table name to select from
	 * WHERE
	 *	col1=?       //where clause
	 * ORDER BY
	 *	col2 DESC    //order by clause
	 * LIMIT 2        //limit clause
	 * OFFSET 3       //offset clause
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 * @param  Order by statement         (eg: col2 DESC)
	 * @param  Row count limit            (eg: 2)
	 * @param  Row offset                 (eg: 3)
	 *
	 * @return  A prepared select statement
	 **/
	public JSqlPreparedStatement selectStatement( //
		String tableName, // Table name to select from
		//
		String selectStatement, // The Columns to select, null means all
		//
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues, // Values that corresponds to the where statement
		//
		String orderStatement, // Order by statements, must be either ASC / DESC
		//
		long limit, // Limit row count to, use 0 to ignore / disable
		long offset // Offset limit by?
	) {
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		StringBuilder queryBuilder = new StringBuilder("SELECT ");
		
		// Select collumns
		if (selectStatement == null || (selectStatement = selectStatement.trim()).length() <= 0) {
			queryBuilder.append("*");
		} else {
			queryBuilder.append(selectStatement);
		}
		
		// From table names
		queryBuilder.append(" FROM `" + tableName + "`");
		
		// Where clauses
		if (whereStatement != null && (whereStatement = whereStatement.trim()).length() >= 3) {
			
			queryBuilder.append(WHERE);
			queryBuilder.append(whereStatement);
			
			if (whereValues != null) {
				for (int b = 0; b < whereValues.length; ++b) {
					queryArgs.add(whereValues[b]);
				}
			}
		}
		
		// Order By clause
		if (orderStatement != null && (orderStatement = orderStatement.trim()).length() > 3) {
			queryBuilder.append(" ORDER BY ");
			queryBuilder.append(orderStatement);
		}
		
		// Limit and offset clause
		if (limit > 0) {
			queryBuilder.append(" LIMIT " + limit);
			
			if (offset > 0) {
				queryBuilder.append(" OFFSET " + offset);
			}
		}
		
		// Create the query set
		return prepareStatement(queryBuilder.toString(), queryArgs.toArray());
	}
	
	//-------------------------------------------------------------------------
	//
	// CREATE TABLE Query builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helps generate an SQL CREATE TABLE IF NOT EXISTS request. This function was created to acommedate the various
	 * syntax differances of CREATE TABLE IF NOT EXISTS across the various SQL vendors (if any).
	 *
	 * The syntax below, is an example of such an CREATE TABLE IF NOT EXISTS statement for SQLITE.
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * CREATE TABLE IF NOT EXISTS TABLENAME ( COLLUMNS_NAME TYPE, ... )
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to create          (eg: col1, col2)
	 * @param  Columns types              (eg: int, text)
	 **/
	public JSqlPreparedStatement createTableStatement( //
		String tableName, // Table name to create
		String[] columnName, // The column names
		String[] columnTypes // The column types
	) {
		
		// Tablename length warning
		if (tableName.length() > 30) {
			LOGGER.warning(JSqlException.oracleNameSpaceWarning + tableName);
		}
		
		// Column names checks
		if (columnName == null || columnTypes == null || columnTypes.length != columnName.length) {
			throw new IllegalArgumentException("Invalid columnName/Type provided: " + columnName
				+ " : " + columnTypes);
		}
		
		StringBuilder queryBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS `");
		queryBuilder.append(tableName);
		queryBuilder.append("` ( ");
		
		for (int a = 0; a < columnName.length; ++a) {
			if (a > 0) {
				queryBuilder.append(", ");
			}
			queryBuilder.append(columnName[a]);
			queryBuilder.append(" ");
			queryBuilder.append(columnTypes[a]);
		}
		queryBuilder.append(" )");
		
		// Create the query set
		return prepareStatement(queryBuilder.toString());
	}
	
	//-------------------------------------------------------------------------
	//
	// random SELECT Query Builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helps generate an random SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors.
	 *
	 * SEE: https://stackoverflow.com/questions/580639/how-to-randomly-select-rows-in-sql
	 *
	 * The syntax below, is an example of such a random SELECT statement for SQLITE.
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * SELECT
	 *	col1, col2      // select collumn
	 * FROM tableName    // table name to select from
	 * WHERE
	 *	col1=?          // where clause
	 * ORDER BY RANDOM() // Random sorting
	 * LIMIT 1           // number of rows to fetch
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 * @param  Row count, 0 for all       (eg: 3)
	 *
	 * @return  A prepared SELECT statement
	 **/
	public JSqlPreparedStatement randomSelectStatement( //
		String tableName, // Table name to select from
		//
		String selectStatement, // The Columns to select, null means all
		//
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues, // Values that corresponds to the where statement
		//
		long rowLimit // Number of rows
	) {
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		StringBuilder queryBuilder = new StringBuilder("SELECT ");
		
		// Select collumns
		if (selectStatement == null || (selectStatement = selectStatement.trim()).length() <= 0) {
			queryBuilder.append("*");
		} else {
			queryBuilder.append(selectStatement);
		}
		
		// From table names
		queryBuilder.append(" FROM `" + tableName + "`");
		
		// Where clauses
		if (whereStatement != null && (whereStatement = whereStatement.trim()).length() >= 3) {
			
			queryBuilder.append(WHERE);
			queryBuilder.append(whereStatement);
			
			if (whereValues != null) {
				for (int b = 0; b < whereValues.length; ++b) {
					queryArgs.add(whereValues[b]);
				}
			}
		}
		
		// Order By clause
		queryBuilder.append(" ORDER BY RANDOM()");
		
		// Limit and offset clause
		if (rowLimit > 0) {
			queryBuilder.append(" LIMIT " + rowLimit);
		}
		
		// Create the query set
		return prepareStatement(queryBuilder.toString(), queryArgs.toArray());
	}
	
	//-------------------------------------------------------------------------
	//
	// UPSERT Query Builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 * SQLite specific UPSERT support
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * INSERT OR REPLACE INTO Employee (
	 *	id,      // Unique Columns to check for upsert
	 *	fname,   // Insert Columns to update
	 *	lname,   // Insert Columns to update
	 *	role,    // Default Columns, that has default fallback value
	 *   note,    // Misc Columns, which existing values are preserved (if exists)
	 * ) VALUES (
	 *	1,       // Unique value
	 * 	'Tom',   // Insert value
	 * 	'Hanks', // Update value
	 *	COALESCE((SELECT role FROM Employee WHERE id = 1), 'Benchwarmer'), // Values with default
	 *	(SELECT note FROM Employee WHERE id = 1) // Misc values to preserve
	 * );
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Unique column names        (eg: id)
	 * @param  Unique column values       (eg: 1)
	 * @param  Upsert column names        (eg: fname,lname)
	 * @param  Upsert column values       (eg: 'Tom','Hanks')
	 * @param  Default column to use existing values if exists   (eg: 'role')
	 * @param  Default column values to use if not exists        (eg: 'Benchwarmer')
	 * @param  All other column names to maintain existing value (eg: 'note')
	 *
	 * @return  A prepared upsert statement
	 **/
	public JSqlPreparedStatement upsertStatement( //
		String tableName, // Table name to upsert on
		//
		String[] uniqueColumns, // The unique column names
		Object[] uniqueValues, // The row unique identifier values
		//
		String[] insertColumns, // Columns names to update
		Object[] insertValues, // Values to update
		//
		String[] defaultColumns, //
		// Columns names to apply default value, if not exists
		// Values to insert, that is not updated. Note that this is ignored if pre-existing values exists
		Object[] defaultValues, //
		// Various column names where its existing value needs to be maintained (if any),
		// this is important as some SQL implementation will fallback to default table values, if not properly handled
		String[] miscColumns //
	) {
		throw new UnsupportedOperationException(JSqlException.invalidDatabaseImplementationException);
	}
	
}
