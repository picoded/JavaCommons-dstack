package picoded.dstack.connector.jsql.statement;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.connector.jsql.*;

/**
 * UPSERT statement builder
 **/
public interface StatementBuilderUpsert extends StatementBuilderBaseInterface {
	
	//-------------------------------------------------------------------------
	//
	// UPSERT statement builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helps generate an SQL UPSERT request. This function was created to acommedate the various
	 * syntax differances of UPSERT across the various SQL vendors.
	 *
	 * IMPORTANT, See : upsertStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Unique column names        (eg: id)
	 * @param  Unique column values       (eg: 1)
	 * @param  Upsert column names        (eg: fname,lname)
	 * @param  Upsert column values       (eg: 'Tom','Hanks')
	 *
	 * @return  true, if UPSERT statement executed succesfuly
	 **/
	default boolean upsert( //
		String tableName, // Table name to upsert on
		//
		String[] uniqueColumns, // The unique column names
		Object[] uniqueValues, // The row unique identifier values
		//
		String[] insertColumns, // Columns names to update
		Object[] insertValues // Values to update
	) {
		return upsertStatement( //
			tableName, //
			uniqueColumns, uniqueValues, //
			insertColumns, insertValues, //
			null, null, //
			null //
		).update() >= 1;
	}
	
	/**
	 * Helps generate an SQL UPSERT request. This function was created to acommedate the various
	 * syntax differances of UPSERT across the various SQL vendors.
	 *
	 * IMPORTANT, See : upsertStatement for full docs
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
	 * @return  true, if UPSERT statement executed succesfuly
	 **/
	default boolean upsert( //
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
		return upsertStatement( //
			tableName, //
			uniqueColumns, uniqueValues, //
			insertColumns, insertValues, //
			defaultColumns, defaultValues, //
			miscColumns //
		).update() >= 1;
	}
	
	/**
	 * Helps generate an SQL UPSERT request. This function was created to acommedate the various
	 * syntax differances of UPSERT across the various SQL vendors.
	 *
	 * First note that the term UPSERT is NOT an offical SQL syntax, but an informal combination of insert/update.
	 *
	 * Note that misc column, while "this sucks" to fill up is required to ensure cross DB
	 * competibility as in certain cases this is required!
	 *
	 * This query alone is one of the largest reason this whole library exists,
	 * (the other being create table if not exists) due to its high usage,
	 * and extremely non consistent SQL implmentations across systems.
	 *
	 * Its such a bad topic that even within versions of a single SQL vendor,
	 * the defacto method for achieving this changes. Its also because of this complexity
	 * why it probably will not be part of the generic query parser.
	 *
	 * PostgreSQL specific notes : This does an "INSERT and ON CONFLICT UPDATE",
	 * in general this has the same meaning if you only have a single unique primary key.
	 * However a "CONFLICT" can occur through other cases such as primary/foreign keys (i think).
	 * Long story short, its complicated if you have multiple unique keys.
	 *
	 * See: http://stackoverflow.com/questions/17267417/how-to-upsert-merge-insert-on-duplicate-update-in-postgresql
	 *
	 * The syntax below, is an example of such an UPSERT statement for SQLITE.
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
	 * In general, for those compliant with SQL 2003 standard, with good performance.
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * MERGE INTO Employee AS TARGET USING (
	 *    SELECT 1 AS id
	 * ) AS SOURCE ON (
	 *   TARGET.id = SOURCE.id
	 * ) WHEN MATCHED THEN
	 *   ... UPDATE statement ...
	 * WHEN NOT MATCHED THEN
	 *	... INSERT statement ...
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
	 * @return  A prepared UPSERT statement
	 **/
	default JSqlPreparedStatement upsertStatement( //
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
	
	//-------------------------------------------------------------------------
	//
	// Multiple UPSERT
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Does multiple UPSERT continously. Use this command when doing,
	 * a large number of UPSERT's to the same table with the same format.
	 *
	 * In certain SQL deployments, this larger multi-UPSERT would be optimized as a
	 * single transaction call. However this behaviour is not guranteed across all platforms.
	 *
	 * This is incredibily useful for large meta-table object updates.
	 *
	 * @param  Table name to query
	 * @param  Unique column names
	 * @param  Unique column values, as a list. Each item in a list represents the respecitve row record
	 * @param  Upsert column names
	 * @param  Upsert column values, as a list. Each item in a list represents the respecitve row record
	 * @param  Default column to use existing values if exists
	 * @param  Default column values to use if not exists, as a list. Each item in a list represents the respecitve row record
	 * @param  All other column names to maintain existing value
	 *
	 * @return  true, if UPSERT statement executed succesfuly
	 **/
	default boolean multiUpsert( //
		String tableName, // Table name to upsert on
		//
		String[] uniqueColumns, // The unique column names
		List<Object[]> uniqueValuesList, // The row unique identifier values
		//
		String[] insertColumns, // Columns names to update
		List<Object[]> insertValuesList, // Values to update
		// Columns names to apply default value, if not exists
		// Values to insert, that is not updated. Note that this is ignored if pre-existing values exists
		String[] defaultColumns, //
		List<Object[]> defaultValuesList, //
		// Various column names where its existing value needs to be maintained (if any),
		// this is important as some SQL implementation will fallback to default table values, if not properly handled
		String[] miscColumns //
	) {
		int rows = uniqueValuesList.size();
		boolean res = true;
		
		// For each record, do the respective upsert
		for (int i = 0; i < rows; ++i) {
			Object[] uniqueValues = (uniqueValuesList != null && uniqueValuesList.size() > i) ? uniqueValuesList
				.get(i) : null;
			Object[] insertValues = (insertValuesList != null && insertValuesList.size() > i) ? insertValuesList
				.get(i) : null;
			Object[] defaultValues = (defaultValuesList != null && defaultValuesList.size() > i) ? defaultValuesList
				.get(i) : null;
			
			res = res && upsert( //
				tableName, //
				uniqueColumns, uniqueValues, //
				insertColumns, insertValues, //
				defaultColumns, defaultValues, //
				miscColumns);
		}
		
		return res;
	}
	
}