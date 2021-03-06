// package picoded.dstack.connector.jsql;

// import java.sql.Connection;
// import java.sql.SQLException;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.Map;
// import java.util.List;
// import java.util.Properties;
// import java.util.logging.Logger;

// import picoded.core.struct.GenericConvertMap;
// import picoded.dstack.connector.jsql.JSqlType;

// /**
//  * Various JSQL query interfaces built ontop of raw queries
//  **/
// interface JSqlQueryInterface {

// 	//-------------------------------------------------------------------------
// 	//
// 	// Standard raw query/execute command sets
// 	//
// 	//-------------------------------------------------------------------------

// 	/**
// 	 * Executes the argumented SQL query, and immediately fetches the result from
// 	 * the database into the result set.
// 	 *
// 	 * This is a raw execution. As such no special parsing occurs to the request
// 	 *
// 	 * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
// 	 *
// 	 * @param  Query strings including substituable variable "?"
// 	 * @param  Array of arguments to do the variable subtitution
// 	 *
// 	 * @return  JSQL result set
// 	 **/
// 	JSqlResult query_raw(String qString, Object... values);

// 	/**
// 	 * Executes the argumented SQL update.
// 	 *
// 	 * Returns -1 if no result object is given by the execution call.
// 	 * Else returns the number of rows affected
// 	 *
// 	 * This is a raw execution. As such no special parsing occurs to the request
// 	 *
// 	 * @param  Query strings including substituable variable "?"
// 	 * @param  Array of arguments to do the variable subtitution
// 	 *
// 	 * @return  -1 if failed, 0 and above for affected rows
// 	 **/
// 	int update_raw(String qString, Object... values);

// 	// //-------------------------------------------------------------------------
// 	// //
// 	// // Generic SQL conversion and query
// 	// //
// 	// //-------------------------------------------------------------------------

// 	// /**
// 	//  * Used for refrence checks / debugging only. This represents the core
// 	//  * generic SQL statement refactoring engine. That is currenlty used internally
// 	//  * by query / update. Doing common regex substitutions if needed.
// 	//  *
// 	//  * Long term plan is to convert this to a much more proprely structed AST engine.
// 	//  *
// 	//  * @param  SQL query to "normalize"
// 	//  *
// 	//  * @return  SQL query that was converted
// 	//  **/
// 	// public String genericSqlParser(String qString) {
// 	// 	return qString;
// 	// }

// 	// /**
// 	//  * Internal exception catching, used for cases which its not possible to
// 	//  * easily handle with pure SQL query. Or cases where the performance cost in the
// 	//  * the query does not justify its usage (edge cases)
// 	//  *
// 	//  * This acts as a filter for query, noFetchQuery, and update respectively
// 	//  *
// 	//  * @param  SQL query to "normalize"
// 	//  * @param  The "normalized" sql query
// 	//  * @param  The exception caught
// 	//  *
// 	//  * @return  TRUE, if the exception can be safely ignored
// 	//  **/
// 	// protected boolean sanatizeErrors(String originalQuery, String normalizedQuery, JSqlException e) {
// 	// 	String stackTrace = picoded.core.exception.ExceptionUtils.getStackTrace(e);
// 	// 	return sanatizeErrors(originalQuery.toUpperCase(), normalizedQuery.toUpperCase(), stackTrace);
// 	// }

// 	// /**
// 	//  * Internal exception catching, used for cases which its not possible to
// 	//  * easily handle with pure SQL query. Or cases where the performance cost in the
// 	//  * the query does not justify its usage (edge cases)
// 	//  *
// 	//  * This is the actual implmentation to overwrite
// 	//  *
// 	//  * This acts as a filter for query, noFetchQuery, and update respectively
// 	//  *
// 	//  * @param  SQL query to "normalize"
// 	//  * @param  The "normalized" sql query
// 	//  * @param  The exception caught, as a stack trace string
// 	//  *
// 	//  * @return  TRUE, if the exception can be safely ignored
// 	//  **/
// 	// protected boolean sanatizeErrors(String originalQuery, String normalizedQuery, String stackTrace) {
// 	// 	if (originalQuery.indexOf("DROP TABLE IF EXISTS ") >= 0) {
// 	// 		if (stackTrace.indexOf("missing database") > 0) {
// 	// 			return true;
// 	// 		}
// 	// 	}
// 	// 	return false;
// 	// }

// 	// /**
// 	//  * Executes the argumented SQL query, and immediately fetches the result from
// 	//  * the database into the result set.
// 	//  *
// 	//  * Custom SQL specific parsing occurs here
// 	//  *
// 	//  * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
// 	//  *
// 	//  * @param  Query strings including substituable variable "?"
// 	//  * @param  Array of arguments to do the variable subtitution
// 	//  *
// 	//  * @return  JSQL result set
// 	//  **/
// 	// public JSqlResult query(String qString, Object... values) {
// 	// 	String parsedQuery = genericSqlParser(qString);
// 	// 	try {
// 	// 		return query_raw(parsedQuery, values);
// 	// 	} catch (JSqlException e) {
// 	// 		if (sanatizeErrors(qString, parsedQuery, e)) {
// 	// 			// Sanatization passed, return a token JSqlResult
// 	// 			return new JSqlResult(null, null, 0);
// 	// 		} else {
// 	// 			// If sanatization fails, rethrows error
// 	// 			throw e;
// 	// 		}
// 	// 	}
// 	// }

// 	// /**
// 	//  * Executes the argumented SQL query, and returns the result object *without*
// 	//  * fetching the result data from the database.
// 	//  *
// 	//  * Custom SQL specific parsing occurs here
// 	//  *
// 	//  * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
// 	//  *
// 	//  * @param  Query strings including substituable variable "?"
// 	//  * @param  Array of arguments to do the variable subtitution
// 	//  *
// 	//  * @return  JSQL result set
// 	//  **/
// 	// public JSqlResult noFetchQuery(String qString, Object... values) {
// 	// 	String parsedQuery = genericSqlParser(qString);
// 	// 	try {
// 	// 		return noFetchQuery_raw(parsedQuery, values);
// 	// 	} catch (JSqlException e) {
// 	// 		if (sanatizeErrors(qString, parsedQuery, e)) {
// 	// 			// Sanatization passed, return a token JSqlResult
// 	// 			return new JSqlResult(null, null, 0);
// 	// 		} else {
// 	// 			// If sanatization fails, rethrows error
// 	// 			throw e;
// 	// 		}
// 	// 	}
// 	// }

// 	// /**
// 	//  * Executes the argumented SQL update.
// 	//  *
// 	//  * Returns false if no result object is given by the execution call.
// 	//  *
// 	//  * Custom SQL specific parsing occurs here
// 	//  *
// 	//  * @param  Query strings including substituable variable "?"
// 	//  * @param  Array of arguments to do the variable subtitution
// 	//  *
// 	//  * @return  -1 if failed, 0 and above for affected rows
// 	//  **/
// 	// public int update(String qString, Object... values) {
// 	// 	String parsedQuery = genericSqlParser(qString);
// 	// 	try {
// 	// 		return update_raw(parsedQuery, values);
// 	// 	} catch (JSqlException e) {
// 	// 		if (sanatizeErrors(qString, parsedQuery, e)) {
// 	// 			// Sanatization passed, return a token JSqlResult
// 	// 			return 0;
// 	// 		} else {
// 	// 			// If sanatization fails, rethrows error
// 	// 			throw e;
// 	// 		}
// 	// 	}
// 	// }

// 	// /**
// 	//  * Prepare an SQL statement, for execution subsequently later
// 	//  *
// 	//  * Custom SQL specific parsing occurs here
// 	//  *
// 	//  * @param  Query strings including substituable variable "?"
// 	//  * @param  Array of arguments to do the variable subtitution
// 	//  *
// 	//  * @return  Prepared statement
// 	//  **/
// 	// public JSqlPreparedStatement prepareStatement(String qString, Object... values) {
// 	// 	return new JSqlPreparedStatement(qString, values, this);
// 	// }

// 	// //-------------------------------------------------------------------------
// 	// //
// 	// // Connection closure / disposal
// 	// //
// 	// //-------------------------------------------------------------------------

// 	// /**
// 	//  * Returns true, if close() function was called prior
// 	//  **/
// 	// public boolean isClosed() {
// 	// 	return sqlConn == null;
// 	// }

// 	// /**
// 	//  * Dispose of the respective SQL driver / connection
// 	//  **/
// 	// public void close() {
// 	// 	// Disposes the instancce connection
// 	// 	if (sqlConn != null) {
// 	// 		try {
// 	// 			//sqlConn.join();
// 	// 			sqlConn.close();
// 	// 		} catch (SQLException e) {
// 	// 			throw new RuntimeException(e);
// 	// 		}
// 	// 		sqlConn = null;
// 	// 	}
// 	// }

// 	// /**
// 	//  * Just incase a user forgets to dispose "as per normal"
// 	//  **/
// 	// protected void finalize() throws Throwable {
// 	// 	try {
// 	// 		close(); // close open files
// 	// 	} finally {
// 	// 		super.finalize();
// 	// 	}
// 	// }

// 	// //-------------------------------------------------------------------------
// 	// //
// 	// // INSERT statement builder
// 	// //
// 	// // @TODO : While not needed for current JavaCommons use case
// 	// //         due to the extensive GUID usage of dstack. This may
// 	// //         have use cases outside the core JavaCommons stack.
// 	// //
// 	// //-------------------------------------------------------------------------

// 	// //-------------------------------------------------------------------------
// 	// //
// 	// // UPDATE statement builder
// 	// //
// 	// // @TODO : While not needed for current JavaCommons use case
// 	// //         due to the extensive GUID usage of dstack. This may
// 	// //         have use cases outside the core JavaCommons stack.
// 	// //
// 	// //-------------------------------------------------------------------------

// 	// //-------------------------------------------------------------------------
// 	// //
// 	// // UPSERT statement builder
// 	// //
// 	// //-------------------------------------------------------------------------

// 	// /**
// 	//  * Helps generate an SQL UPSERT request. This function was created to acommedate the various
// 	//  * syntax differances of UPSERT across the various SQL vendors.
// 	//  *
// 	//  * IMPORTANT, See : upsertStatement for full docs
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Unique column names        (eg: id)
// 	//  * @param  Unique column values       (eg: 1)
// 	//  * @param  Upsert column names        (eg: fname,lname)
// 	//  * @param  Upsert column values       (eg: 'Tom','Hanks')
// 	//  *
// 	//  * @return  true, if UPSERT statement executed succesfuly
// 	//  **/
// 	// public boolean upsert( //
// 	// 	String tableName, // Table name to upsert on
// 	// 	//
// 	// 	String[] uniqueColumns, // The unique column names
// 	// 	Object[] uniqueValues, // The row unique identifier values
// 	// 	//
// 	// 	String[] insertColumns, // Columns names to update
// 	// 	Object[] insertValues // Values to update
// 	// ) {
// 	// 	return upsertStatement( //
// 	// 		tableName, //
// 	// 		uniqueColumns, uniqueValues, //
// 	// 		insertColumns, insertValues, //
// 	// 		null, null, //
// 	// 		null //
// 	// 	).update() >= 1;
// 	// }

// 	// /**
// 	//  * Helps generate an SQL UPSERT request. This function was created to acommedate the various
// 	//  * syntax differances of UPSERT across the various SQL vendors.
// 	//  *
// 	//  * IMPORTANT, See : upsertStatement for full docs
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Unique column names        (eg: id)
// 	//  * @param  Unique column values       (eg: 1)
// 	//  * @param  Upsert column names        (eg: fname,lname)
// 	//  * @param  Upsert column values       (eg: 'Tom','Hanks')
// 	//  * @param  Default column to use existing values if exists   (eg: 'role')
// 	//  * @param  Default column values to use if not exists        (eg: 'Benchwarmer')
// 	//  * @param  All other column names to maintain existing value (eg: 'note')
// 	//  *
// 	//  * @return  true, if UPSERT statement executed succesfuly
// 	//  **/
// 	// public boolean upsert( //
// 	// 	String tableName, // Table name to upsert on
// 	// 	//
// 	// 	String[] uniqueColumns, // The unique column names
// 	// 	Object[] uniqueValues, // The row unique identifier values
// 	// 	//
// 	// 	String[] insertColumns, // Columns names to update
// 	// 	Object[] insertValues, // Values to update
// 	// 	//
// 	// 	String[] defaultColumns, //
// 	// 	// Columns names to apply default value, if not exists
// 	// 	// Values to insert, that is not updated. Note that this is ignored if pre-existing values exists
// 	// 	Object[] defaultValues, //
// 	// 	// Various column names where its existing value needs to be maintained (if any),
// 	// 	// this is important as some SQL implementation will fallback to default table values, if not properly handled
// 	// 	String[] miscColumns //
// 	// ) {
// 	// 	return upsertStatement( //
// 	// 		tableName, //
// 	// 		uniqueColumns, uniqueValues, //
// 	// 		insertColumns, insertValues, //
// 	// 		defaultColumns, defaultValues, //
// 	// 		miscColumns //
// 	// 	).update() >= 1;
// 	// }

// 	// /**
// 	//  * Helps generate an SQL UPSERT request. This function was created to acommedate the various
// 	//  * syntax differances of UPSERT across the various SQL vendors.
// 	//  *
// 	//  * First note that the term UPSERT is NOT an offical SQL syntax, but an informal combination of insert/update.
// 	//  *
// 	//  * Note that misc column, while "this sucks" to fill up is required to ensure cross DB
// 	//  * competibility as in certain cases this is required!
// 	//  *
// 	//  * This query alone is one of the largest reason this whole library exists,
// 	//  * (the other being create table if not exists) due to its high usage,
// 	//  * and extremely non consistent SQL implmentations across systems.
// 	//  *
// 	//  * Its such a bad topic that even within versions of a single SQL vendor,
// 	//  * the defacto method for achieving this changes. Its also because of this complexity
// 	//  * why it probably will not be part of the generic query parser.
// 	//  *
// 	//  * PostgreSQL specific notes : This does an "INSERT and ON CONFLICT UPDATE",
// 	//  * in general this has the same meaning if you only have a single unique primary key.
// 	//  * However a "CONFLICT" can occur through other cases such as primary/foreign keys (i think).
// 	//  * Long story short, its complicated if you have multiple unique keys.
// 	//  *
// 	//  * See: http://stackoverflow.com/questions/17267417/how-to-upsert-merge-insert-on-duplicate-update-in-postgresql
// 	//  *
// 	//  * The syntax below, is an example of such an UPSERT statement for SQLITE.
// 	//  *
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
// 	//  * INSERT OR REPLACE INTO Employee (
// 	//  *	id,      // Unique Columns to check for upsert
// 	//  *	fname,   // Insert Columns to update
// 	//  *	lname,   // Insert Columns to update
// 	//  *	role,    // Default Columns, that has default fallback value
// 	//  *   note,    // Misc Columns, which existing values are preserved (if exists)
// 	//  * ) VALUES (
// 	//  *	1,       // Unique value
// 	//  * 	'Tom',   // Insert value
// 	//  * 	'Hanks', // Update value
// 	//  *	COALESCE((SELECT role FROM Employee WHERE id = 1), 'Benchwarmer'), // Values with default
// 	//  *	(SELECT note FROM Employee WHERE id = 1) // Misc values to preserve
// 	//  * );
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// 	//  *
// 	//  * In general, for those compliant with SQL 2003 standard, with good performance.
// 	//  *
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
// 	//  * MERGE INTO Employee AS TARGET USING (
// 	//  *    SELECT 1 AS id
// 	//  * ) AS SOURCE ON (
// 	//  *   TARGET.id = SOURCE.id
// 	//  * ) WHEN MATCHED THEN
// 	//  *   ... UPDATE statement ...
// 	//  * WHEN NOT MATCHED THEN
// 	//  *	... INSERT statement ...
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Unique column names        (eg: id)
// 	//  * @param  Unique column values       (eg: 1)
// 	//  * @param  Upsert column names        (eg: fname,lname)
// 	//  * @param  Upsert column values       (eg: 'Tom','Hanks')
// 	//  * @param  Default column to use existing values if exists   (eg: 'role')
// 	//  * @param  Default column values to use if not exists        (eg: 'Benchwarmer')
// 	//  * @param  All other column names to maintain existing value (eg: 'note')
// 	//  *
// 	//  * @return  A prepared UPSERT statement
// 	//  **/
// 	// public JSqlPreparedStatement upsertStatement( //
// 	// 	String tableName, // Table name to upsert on
// 	// 	//
// 	// 	String[] uniqueColumns, // The unique column names
// 	// 	Object[] uniqueValues, // The row unique identifier values
// 	// 	//
// 	// 	String[] insertColumns, // Columns names to update
// 	// 	Object[] insertValues, // Values to update
// 	// 	//
// 	// 	String[] defaultColumns, //
// 	// 	// Columns names to apply default value, if not exists
// 	// 	// Values to insert, that is not updated. Note that this is ignored if pre-existing values exists
// 	// 	Object[] defaultValues, //
// 	// 	// Various column names where its existing value needs to be maintained (if any),
// 	// 	// this is important as some SQL implementation will fallback to default table values, if not properly handled
// 	// 	String[] miscColumns //
// 	// ) {
// 	// 	throw new UnsupportedOperationException(JSqlException.invalidDatabaseImplementationException);
// 	// }

// 	// //-------------------------------------------------------------------------
// 	// //
// 	// // Multiple UPSERT
// 	// //
// 	// //-------------------------------------------------------------------------

// 	// /**
// 	//  * Does multiple UPSERT continously. Use this command when doing,
// 	//  * a large number of UPSERT's to the same table with the same format.
// 	//  *
// 	//  * In certain SQL deployments, this larger multi-UPSERT would be optimized as a
// 	//  * single transaction call. However this behaviour is not guranteed across all platforms.
// 	//  *
// 	//  * This is incredibily useful for large meta-table object updates.
// 	//  *
// 	//  * @param  Table name to query
// 	//  * @param  Unique column names
// 	//  * @param  Unique column values, as a list. Each item in a list represents the respecitve row record
// 	//  * @param  Upsert column names
// 	//  * @param  Upsert column values, as a list. Each item in a list represents the respecitve row record
// 	//  * @param  Default column to use existing values if exists
// 	//  * @param  Default column values to use if not exists, as a list. Each item in a list represents the respecitve row record
// 	//  * @param  All other column names to maintain existing value
// 	//  *
// 	//  * @return  true, if UPSERT statement executed succesfuly
// 	//  **/
// 	// public boolean multiUpsert( //
// 	// 	String tableName, // Table name to upsert on
// 	// 	//
// 	// 	String[] uniqueColumns, // The unique column names
// 	// 	List<Object[]> uniqueValuesList, // The row unique identifier values
// 	// 	//
// 	// 	String[] insertColumns, // Columns names to update
// 	// 	List<Object[]> insertValuesList, // Values to update
// 	// 	// Columns names to apply default value, if not exists
// 	// 	// Values to insert, that is not updated. Note that this is ignored if pre-existing values exists
// 	// 	String[] defaultColumns, //
// 	// 	List<Object[]> defaultValuesList, //
// 	// 	// Various column names where its existing value needs to be maintained (if any),
// 	// 	// this is important as some SQL implementation will fallback to default table values, if not properly handled
// 	// 	String[] miscColumns //
// 	// ) {
// 	// 	int rows = uniqueValuesList.size();
// 	// 	boolean res = true;

// 	// 	// For each record, do the respective upsert
// 	// 	for (int i = 0; i < rows; ++i) {
// 	// 		Object[] uniqueValues = (uniqueValuesList != null && uniqueValuesList.size() > i) ? uniqueValuesList
// 	// 			.get(i) : null;
// 	// 		Object[] insertValues = (insertValuesList != null && insertValuesList.size() > i) ? insertValuesList
// 	// 			.get(i) : null;
// 	// 		Object[] defaultValues = (defaultValuesList != null && defaultValuesList.size() > i) ? defaultValuesList
// 	// 			.get(i) : null;

// 	// 		res = res && upsert( //
// 	// 			tableName, //
// 	// 			uniqueColumns, uniqueValues, //
// 	// 			insertColumns, insertValues, //
// 	// 			defaultColumns, defaultValues, //
// 	// 			miscColumns);
// 	// 	}

// 	// 	return res;
// 	// }

// 	// //-------------------------------------------------------------------------
// 	// //
// 	// // DELETE statement builder
// 	// //
// 	// //-------------------------------------------------------------------------

// 	// /**
// 	//  * Helps generate an SQL DELETE request. This function was created to acommedate the various
// 	//  * syntax differances of DELETE across the various SQL vendors (if any).
// 	//  *
// 	//  * See : deleteStatement for full docs
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  *
// 	//  * @return  the number of rows affected
// 	//  **/
// 	// public int delete( //
// 	// 	String tableName // Table name to select from
// 	// ) {
// 	// 	return deleteStatement(tableName, null, null).update();
// 	// }

// 	// /**
// 	//  * Helps generate an SQL DELETE request. This function was created to acommedate the various
// 	//  * syntax differances of DELETE across the various SQL vendors (if any).
// 	//  *
// 	//  * See : deleteStatement for full docs
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Where statement to filter  (eg: col1=?)
// 	//  * @param  Where arguments value      (eg: [value/s])
// 	//  *
// 	//  * @return  the number of rows affected
// 	//  **/
// 	// public int delete( //
// 	// 	String tableName, // Table name to select from
// 	// 	//
// 	// 	String whereStatement, // The Columns to apply where clause, this must be sql neutral
// 	// 	Object[] whereValues // Values that corresponds to the where statement
// 	// ) {
// 	// 	return deleteStatement(tableName, whereStatement, whereValues).update();
// 	// }

// 	// /**
// 	//  * Helps generate an SQL DELETE request. This function was created to acommedate the various
// 	//  * syntax differances of DELETE across the various SQL vendors (if any).
// 	//  *
// 	//  * The syntax below, is an example of such an DELETE statement for SQLITE.
// 	//  *
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
// 	//  * DELETE
// 	//  * FROM tableName //table name to select from
// 	//  * WHERE
// 	//  *	col1=?       //where clause
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Where statement to filter  (eg: col1=?)
// 	//  * @param  Where arguments value      (eg: [value/s])
// 	//  *
// 	//  * @return  A prepared DELETE statement
// 	//  **/
// 	// public JSqlPreparedStatement deleteStatement( //
// 	// 	String tableName, // Table name to select from
// 	// 	//
// 	// 	String whereStatement, // The Columns to apply where clause, this must be sql neutral
// 	// 	Object[] whereValues // Values that corresponds to the where statement
// 	// ) {

// 	// 	if (tableName.length() > 30) {
// 	// 		LOGGER.warning(JSqlException.oracleNameSpaceWarning + tableName);
// 	// 	}

// 	// 	ArrayList<Object> queryArgs = new ArrayList<Object>();
// 	// 	StringBuilder queryBuilder = new StringBuilder("DELETE ");

// 	// 	// From table names
// 	// 	queryBuilder.append(" FROM `" + tableName + "`");

// 	// 	// Where clauses
// 	// 	if (whereStatement != null && (whereStatement = whereStatement.trim()).length() >= 3) {
// 	// 		queryBuilder.append(" WHERE ");
// 	// 		queryBuilder.append(whereStatement);

// 	// 		if (whereValues != null) {
// 	// 			for (int b = 0; b < whereValues.length; ++b) {
// 	// 				queryArgs.add(whereValues[b]);
// 	// 			}
// 	// 		}
// 	// 	}

// 	// 	// Create the query set
// 	// 	return new JSqlPreparedStatement(queryBuilder.toString(), queryArgs.toArray(), this);
// 	// }

// 	// //-------------------------------------------------------------------------
// 	// //
// 	// // DROP TABLE statement builder
// 	// //
// 	// // @TODO : While not needed for current JavaCommons use case
// 	// //         due to the extensive GUID usage of dstack. This may
// 	// //         have use cases outside the core JavaCommons stack.
// 	// //
// 	// //-------------------------------------------------------------------------

// 	// /**
// 	//  * Helps generate an SQL DROP TABLE IF EXISTS request. This function was created to acommedate the various
// 	//  * syntax differances of DROP TABLE IF EXISTS across the various SQL vendors (if any).
// 	//  *
// 	//  * The syntax below, is an example of such an DROP TABLE IF EXISTS statement for SQLITE.
// 	//  *
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
// 	//  * DROP TABLE IF NOT EXISTS TABLENAME
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// 	//  *
// 	//  * @param  Table name to drop        (eg: tableName)
// 	//  *
// 	//  * @return  true, if DROP TABLE execution is succesful
// 	//  **/
// 	// public boolean dropTable(String tablename //Table name to drop
// 	// ) {
// 	// 	return (dropTableStatement(tablename).update() >= 0);
// 	// }

// 	// /**
// 	//  * Helps generate an SQL DROP TABLE IF EXISTS request. This function was created to acommedate the various
// 	//  * syntax differances of DROP TABLE IF EXISTS across the various SQL vendors (if any).
// 	//  *
// 	//  * The syntax below, is an example of such an DROP TABLE IF EXISTS statement for SQLITE.
// 	//  *
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
// 	//  * DROP TABLE IF NOT EXISTS TABLENAME
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// 	//  *
// 	//  * @param  Table name to drop        (eg: tableName)
// 	//  *
// 	//  * @return  true, if DROP TABLE execution is succesful
// 	//  **/
// 	// public JSqlPreparedStatement dropTableStatement(String tablename //Table name to drop
// 	// ) {
// 	// 	return prepareStatement("DROP TABLE IF EXISTS " + tablename);
// 	// }

// 	// //-------------------------------------------------------------------------
// 	// //
// 	// // CREATE INDEX statement builder
// 	// //
// 	// //-------------------------------------------------------------------------

// 	// /**
// 	//  * Helps generate an SQL CREATE INDEX request. This function was created to acommedate the various
// 	//  * syntax differances of CREATE INDEX across the various SQL vendors (if any).
// 	//  *
// 	//  * See : createIndexStatement for full docs
// 	//  *
// 	//  * @param  Table name to query            (eg: tableName)
// 	//  * @param  Column names to build an index (eg: col1,col2)
// 	//  *
// 	//  * @return  A prepared CREATE INDEX statement
// 	//  **/
// 	// public boolean createIndex( //
// 	// 	String tableName, // Table name to select from
// 	// 	//
// 	// 	String columnNames // The column name to create the index on
// 	// ) {
// 	// 	return createIndexStatement(tableName, columnNames, null, null).update() >= 0;
// 	// }

// 	// /**
// 	//  * Helps generate an SQL CREATE INDEX request. This function was created to acommedate the various
// 	//  * syntax differances of CREATE INDEX across the various SQL vendors (if any).
// 	//  *
// 	//  * See : createIndexStatement for full docs
// 	//  *
// 	//  * @param  Table name to query            (eg: tableName)
// 	//  * @param  Column names to build an index (eg: col1,col2)
// 	//  * @param  Index type to build            (eg: UNIQUE)
// 	//  *
// 	//  * @return  A prepared CREATE INDEX statement
// 	//  **/
// 	// public boolean createIndex( //
// 	// 	String tableName, // Table name to select from
// 	// 	//
// 	// 	String columnNames, // The column name to create the index on
// 	// 	//
// 	// 	String indexType // The index type if given, can be null
// 	// ) {
// 	// 	return createIndexStatement(tableName, columnNames, indexType, null).update() >= 0;
// 	// }

// 	// /**
// 	//  * Helps generate an SQL CREATE INDEX request. This function was created to acommedate the various
// 	//  * syntax differances of CREATE INDEX across the various SQL vendors (if any).
// 	//  *
// 	//  * See : createIndexStatement for full docs
// 	//  *
// 	//  * @param  Table name to query            (eg: tableName)
// 	//  * @param  Column names to build an index (eg: col1,col2)
// 	//  * @param  Index type to build            (eg: UNIQUE)
// 	//  * @param  Index suffix to use            (eg: SpecialIndex)
// 	//  *
// 	//  * @return  A prepared CREATE INDEX statement
// 	//  **/
// 	// public boolean createIndex( //
// 	// 	String tableName, // Table name to select from
// 	// 	//
// 	// 	String columnNames, // The column name to create the index on
// 	// 	//
// 	// 	String indexType, // The index type if given, can be null
// 	// 	//
// 	// 	String indexSuffix // The index name suffix, its auto generated if null
// 	// ) {
// 	// 	return createIndexStatement(tableName, columnNames, indexType, indexSuffix).update() >= 0;
// 	// }

// 	// /**
// 	//  * Helps generate an SQL CREATE INDEX request. This function was created to acommedate the various
// 	//  * syntax differances of CREATE INDEX across the various SQL vendors (if any).
// 	//  *
// 	//  * The syntax below, is an example of such an CREATE INDEX statement for SQLITE.
// 	//  *
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
// 	//  * CREATE (UNIQUE|FULLTEXT) INDEX IF NOT EXISTS TABLENAME_SUFFIX ON TABLENAME ( COLLUMNS )
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// 	//  *
// 	//  * @param  Table name to query            (eg: tableName)
// 	//  * @param  Column names to build an index (eg: col1,col2)
// 	//  * @param  Index type to build            (eg: UNIQUE)
// 	//  * @param  Index suffix to use            (eg: SpecialIndex)
// 	//  *
// 	//  * @return  A prepared CREATE INDEX statement
// 	//  **/
// 	// public JSqlPreparedStatement createIndexStatement( //
// 	// 	String tableName, // Table name to select from
// 	// 	//
// 	// 	String columnNames, // The column name to create the index on
// 	// 	//
// 	// 	String indexType, // The index type if given, can be null
// 	// 	//
// 	// 	String indexSuffix // The index name suffix, its auto generated if null
// 	// ) {
// 	// 	if (tableName.length() > 30) {
// 	// 		LOGGER.warning(JSqlException.oracleNameSpaceWarning + tableName);
// 	// 	}

// 	// 	ArrayList<Object> queryArgs = new ArrayList<Object>();
// 	// 	StringBuilder queryBuilder = new StringBuilder("CREATE ");

// 	// 	if (indexType != null && indexType.length() > 0) {
// 	// 		queryBuilder.append(indexType);
// 	// 		queryBuilder.append(" ");
// 	// 	}

// 	// 	queryBuilder.append("INDEX IF NOT EXISTS ");

// 	// 	// Creates a suffix, based on the collumn names
// 	// 	if (indexSuffix == null || indexSuffix.length() <= 0) {
// 	// 		indexSuffix = columnNames.replaceAll("/[^A-Za-z0-9]/", ""); //.toUpperCase(Locale.ENGLISH)?
// 	// 	}

// 	// 	if ((tableName.length() + 1 + indexSuffix.length()) > 30) {
// 	// 		LOGGER.warning(JSqlException.oracleNameSpaceWarning + tableName + "_" + indexSuffix);
// 	// 	}

// 	// 	queryBuilder.append("`");
// 	// 	queryBuilder.append(tableName);
// 	// 	queryBuilder.append("_");
// 	// 	queryBuilder.append(indexSuffix);
// 	// 	queryBuilder.append("` ON `");
// 	// 	queryBuilder.append(tableName);
// 	// 	queryBuilder.append("` (");
// 	// 	queryBuilder.append(columnNames);
// 	// 	queryBuilder.append(")");

// 	// 	// Create the query set
// 	// 	return new JSqlPreparedStatement(queryBuilder.toString(), queryArgs.toArray(), this);
// 	// }

// 	// //-------------------------------------------------------------------------
// 	// //
// 	// // Randomly SELECT statement builder
// 	// //
// 	// //-------------------------------------------------------------------------

// 	// /**
// 	//  * Helps generate an random SQL SELECT request. This function was created to acommedate the various
// 	//  * syntax differances of SELECT across the various SQL vendors.
// 	//  *
// 	//  * See : randomSelectStatement for full docs
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Columns to select          (eg: col1, col2)
// 	//  * @param  Where statement to filter  (eg: col1=?)
// 	//  * @param  Where arguments value      (eg: [value/s])
// 	//  *
// 	//  * @return  The JSqlResult
// 	//  **/
// 	// public JSqlResult randomSelect( //
// 	// 	String tableName, // Table name to select from
// 	// 	String selectStatement // The Columns to select, null means all
// 	// ) {
// 	// 	return randomSelect(tableName, selectStatement, null, null, 0);
// 	// }

// 	// /**
// 	//  * Helps generate an random SQL SELECT request. This function was created to acommedate the various
// 	//  * syntax differances of SELECT across the various SQL vendors.
// 	//  *
// 	//  * See : randomSelectStatement for full docs
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Columns to select          (eg: col1, col2)
// 	//  * @param  Where statement to filter  (eg: col1=?)
// 	//  * @param  Where arguments value      (eg: [value/s])
// 	//  *
// 	//  * @return  The JSqlResult
// 	//  **/
// 	// public JSqlResult randomSelect( //
// 	// 	String tableName, // Table name to select from
// 	// 	//
// 	// 	String selectStatement, // The Columns to select, null means all
// 	// 	//
// 	// 	String whereStatement, // The Columns to apply where clause, this must be sql neutral
// 	// 	Object[] whereValues // Values that corresponds to the where statement
// 	// ) {
// 	// 	return randomSelect(tableName, selectStatement, whereStatement, whereValues, 0);
// 	// }

// 	// /**
// 	//  * Helps generate an random SQL SELECT request. This function was created to acommedate the various
// 	//  * syntax differances of SELECT across the various SQL vendors.
// 	//  *
// 	//  * See : randomSelectStatement for full docs
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Columns to select          (eg: col1, col2)
// 	//  * @param  Where statement to filter  (eg: col1=?)
// 	//  * @param  Where arguments value      (eg: [value/s])
// 	//  * @param  Row count, 0 for all       (eg: 3)
// 	//  *
// 	//  * @return  The JSqlResult
// 	//  **/
// 	// public JSqlResult randomSelect( //
// 	// 	String tableName, // Table name to select from
// 	// 	//
// 	// 	String selectStatement, // The Columns to select, null means all
// 	// 	//
// 	// 	String whereStatement, // The Columns to apply where clause, this must be sql neutral
// 	// 	Object[] whereValues, // Values that corresponds to the where statement
// 	// 	//
// 	// 	long rowLimit // Number of rows
// 	// ) {
// 	// 	return randomSelectStatement(tableName, selectStatement, whereStatement, whereValues,
// 	// 		rowLimit).query();
// 	// }

// 	// /**
// 	//  * Helps generate an random SQL SELECT request. This function was created to acommedate the various
// 	//  * syntax differances of SELECT across the various SQL vendors.
// 	//  *
// 	//  * See : randomSelectStatement for full docs
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Columns to select          (eg: col1, col2)
// 	//  *
// 	//  * @return  A prepared SELECT statement
// 	//  **/
// 	// public JSqlPreparedStatement randomSelectStatement( //
// 	// 	String tableName, // Table name to select from
// 	// 	String selectStatement // The Columns to select, null means all
// 	// ) {
// 	// 	return randomSelectStatement(tableName, selectStatement, null, null, 0);
// 	// }

// 	// /**
// 	//  * Helps generate an random SQL SELECT request. This function was created to acommedate the various
// 	//  * syntax differances of SELECT across the various SQL vendors.
// 	//  *
// 	//  * See : randomSelectStatement for full docs
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Columns to select          (eg: col1, col2)
// 	//  * @param  Where statement to filter  (eg: col1=?)
// 	//  * @param  Where arguments value      (eg: [value/s])
// 	//  *
// 	//  * @return  A prepared SELECT statement
// 	//  **/
// 	// public JSqlPreparedStatement randomSelectStatement( //
// 	// 	String tableName, // Table name to select from
// 	// 	//
// 	// 	String selectStatement, // The Columns to select, null means all
// 	// 	//
// 	// 	String whereStatement, // The Columns to apply where clause, this must be sql neutral
// 	// 	Object[] whereValues // Values that corresponds to the where statement
// 	// ) {
// 	// 	return randomSelectStatement(tableName, selectStatement, whereStatement, whereValues, 0);
// 	// }

// 	// /**
// 	//  * Helps generate an random SQL SELECT request. This function was created to acommedate the various
// 	//  * syntax differances of SELECT across the various SQL vendors.
// 	//  *
// 	//  * SEE: https://stackoverflow.com/questions/580639/how-to-randomly-select-rows-in-sql
// 	//  *
// 	//  * The syntax below, is an example of such a random SELECT statement for SQLITE.
// 	//  *
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
// 	//  * SELECT
// 	//  *	col1, col2      // select collumn
// 	//  * FROM tableName    // table name to select from
// 	//  * WHERE
// 	//  *	col1=?          // where clause
// 	//  * ORDER BY RANDOM() // Random sorting
// 	//  * LIMIT 1           // number of rows to fetch
// 	//  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// 	//  *
// 	//  * @param  Table name to query        (eg: tableName)
// 	//  * @param  Columns to select          (eg: col1, col2)
// 	//  * @param  Where statement to filter  (eg: col1=?)
// 	//  * @param  Where arguments value      (eg: [value/s])
// 	//  * @param  Row count, 0 for all       (eg: 3)
// 	//  *
// 	//  * @return  A prepared SELECT statement
// 	//  **/
// 	// public JSqlPreparedStatement randomSelectStatement( //
// 	// 	String tableName, // Table name to select from
// 	// 	//
// 	// 	String selectStatement, // The Columns to select, null means all
// 	// 	//
// 	// 	String whereStatement, // The Columns to apply where clause, this must be sql neutral
// 	// 	Object[] whereValues, // Values that corresponds to the where statement
// 	// 	//
// 	// 	long rowLimit // Number of rows
// 	// ) {
// 	// 	throw new UnsupportedOperationException(JSqlException.invalidDatabaseImplementationException);
// 	// }

// }