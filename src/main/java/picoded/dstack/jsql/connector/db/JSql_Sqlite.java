package picoded.dstack.jsql.connector.db;

import java.util.Locale;
import java.util.ArrayList;

import picoded.dstack.jsql.connector.*;
import picoded.dstack.jsql.connector.JSqlType;

/**
 * Pure SQLite implentation of JSql
 **/
public class JSql_Sqlite extends JSql_Base {
	
	/**
	 * Setup database as pure SQLite mode
	 **/
	public JSql_Sqlite() {
		this(":memory:");
	}
	
	/**
	 * Runs JSql with the JDBC sqlite engine
	 *
	 * @param  File path for the sqlite file
	 **/
	public JSql_Sqlite(String sqliteLoc) {
		// store database connection properties
		setConnectionProperties(sqliteLoc, null, null, null, null);
		// call internal method to create the connection
		setupConnection();
	}
	
	/**
	 * Internal common reuse constructor
	 * Setsup the internal connection settings and driver
	 **/
	private void setupConnection() {
		sqlType = JSqlType.SQLITE;
		
		try {
			// This is only imported on demand, avoid preloading until needed
			Class.forName("org.sqlite.JDBC");
			
			// Getting the required SQLite connection
			sqlConn = java.sql.DriverManager.getConnection("jdbc:sqlite:"
				+ (String) connectionProps.get("dbUrl"));
		} catch (Exception e) {
			throw new RuntimeException("Failed to load sqlite connection: ", e);
		}
	}
	
	/**
	 * As this is the base class varient, this funciton isnt suported
	 **/
	public void recreate(boolean force) {
		if (force) {
			close();
		}
		// call internal method to create the connection
		setupConnection();
	}
	
	/**
	 * Internal parser that converts some of the common sql statements to sqlite
	 * This converts one SQL convention to another as needed
	 *
	 * @param  SQL query to "normalize"
	 *
	 * @return  SQL query that was converted
	 **/
	public String genericSqlParser(String inString) {
		final String truncateTable = "TRUNCATE TABLE";
		final String deleteFrom = "DELETE FROM";
		
		inString = inString.toUpperCase(Locale.ENGLISH);
		inString = inString.trim().replaceAll("(\\s){1}", " ").replaceAll("\\s+", " ")
			.replaceAll("(?i)VARCHAR\\(MAX\\)", "VARCHAR").replaceAll("(?i)BIGINT", "INTEGER");
		
		if (inString.startsWith(truncateTable)) {
			inString = inString.replaceAll(truncateTable, deleteFrom);
		}
		return inString;
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
	 * 	'Hanks', // Insert value
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
		// Columns names to apply default value, if not exists
		// Values to insert, that is not updated. Note that this is ignored if pre-existing values exists
		String[] defaultColumns, //
		Object[] defaultValues, //
		// Various column names where its existing value needs to be maintained (if any),
		// this is important as some SQL implementation will fallback to default table values, if not properly handled
		String[] miscColumns //
	) {
		
		/**
		 * Checks that unique collumn and values length to be aligned
		 **/
		if (uniqueColumns == null || uniqueValues == null
			|| uniqueColumns.length != uniqueValues.length) {
			throw new JSqlException(
				"Upsert query requires unique column and values to be equal length");
		}
		
		/**
		 * Preparing inner default select, this will be used repeatingly for COALESCE, DEFAULT and MISC values
		 **/
		ArrayList<Object> innerSelectArgs = new ArrayList<Object>();
		StringBuilder innerSelectSB = new StringBuilder(" FROM ");
		innerSelectSB.append("`" + tableName + "`");
		innerSelectSB.append(WHERE);
		for (int a = 0; a < uniqueColumns.length; ++a) {
			if (a > 0) {
				innerSelectSB.append(" AND ");
			}
			innerSelectSB.append(uniqueColumns[a] + " = ?");
			innerSelectArgs.add(uniqueValues[a]);
		}
		innerSelectSB.append(")");
		
		String innerSelectPrefix = "(SELECT ";
		String innerSelectSuffix = innerSelectSB.toString();
		
		/**
		 * Building the query for INSERT OR REPLACE
		 **/
		StringBuilder queryBuilder = new StringBuilder("INSERT OR REPLACE INTO `" + tableName + "` (");
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		
		/**
		 * Building the query for both sides of '(...columns...) VALUE (...vars...)' clauses in upsert
		 * Note that the final trailing ", " seperator will be removed prior to final query conversion
		 **/
		StringBuilder columnNames = new StringBuilder();
		StringBuilder columnValues = new StringBuilder();
		String columnSeperator = ", ";
		
		/**
		 * Setting up unique values
		 **/
		for (int a = 0; a < uniqueColumns.length; ++a) {
			columnNames.append(uniqueColumns[a]);
			columnNames.append(columnSeperator);
			//
			columnValues.append("?");
			columnValues.append(columnSeperator);
			//
			queryArgs.add(uniqueValues[a]);
		}
		
		/**
		 * Inserting updated values
		 **/
		if (insertColumns != null) {
			for (int a = 0; a < insertColumns.length; ++a) {
				columnNames.append(insertColumns[a]);
				columnNames.append(columnSeperator);
				//
				columnValues.append("?");
				columnValues.append(columnSeperator);
				//
				queryArgs.add((insertValues != null && insertValues.length > a) ? insertValues[a]
					: null);
			}
		}
		
		/**
		 * Handling default values
		 **/
		if (defaultColumns != null) {
			for (int a = 0; a < defaultColumns.length; ++a) {
				columnNames.append(defaultColumns[a]);
				columnNames.append(columnSeperator);
				columnValues.append(COALESCE);
				columnValues.append(innerSelectPrefix);
				columnValues.append(defaultColumns[a]);
				columnValues.append(innerSelectSuffix);
				queryArgs.addAll(innerSelectArgs);
				columnValues.append(", ?)");
				columnValues.append(columnSeperator);
				queryArgs.add((defaultValues != null && defaultValues.length > a) ? defaultValues[a]
					: null);
			}
		}
		
		/**
		 * Handling Misc values
		 **/
		if (miscColumns != null) {
			for (int a = 0; a < miscColumns.length; ++a) {
				columnNames.append(miscColumns[a]);
				columnNames.append(columnSeperator);
				columnValues.append(innerSelectPrefix);
				columnValues.append(miscColumns[a]);
				columnValues.append(innerSelectSuffix);
				queryArgs.addAll(innerSelectArgs);
				columnValues.append(columnSeperator);
			}
		}
		
		/**
		 * Building the final query
		 **/
		queryBuilder
			.append(columnNames.substring(0, columnNames.length() - columnSeperator.length()));
		queryBuilder.append(") VALUES (");
		queryBuilder.append(columnValues.substring(0,
			columnValues.length() - columnSeperator.length()));
		queryBuilder.append(")");
		return new JSqlPreparedStatement(queryBuilder.toString(), queryArgs.toArray(), this);
	}
	
}
