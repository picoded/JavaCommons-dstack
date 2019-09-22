package picoded.dstack.connector.jsql.statement;

import java.util.ArrayList;
import java.util.List;

import picoded.dstack.connector.jsql.*;

/**
 * insert statement builder
 **/
public interface StatementBuilderInsert extends StatementBuilderBaseInterface {
	
	//-------------------------------------------------------------------------
	//
	// insert statement builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helps execute an SQL insert request. 
	 *
	 * IMPORTANT, See : insertStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  insert column names        (eg: fname,lname)
	 * @param  insert column values       (eg: 'Tom','Hanks')
	 *
	 * @return  true, if insert statement executed succesfuly
	 **/
	default boolean insert( //
		String tableName, // Table name to insert on
		//
		String[] insertColumns, // Columns names to insert
		Object[] insertValues // Values to insert
	) {
		return insertStatement( //
			tableName, //
			insertColumns, insertValues //
		).update() >= 1;
	}
	
	/**
	 * Geenrate an insert statement for the table
	 *
	 * The syntax below, is an example of such an insert statement for SQLITE.
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * INSERT INTO Employee (
	 *	  id,      // Unique Columns to check for insert
	 *	  fname,   // Insert Columns 
	 *	  lname    // Insert Columns
	 * ) VALUES (
	 *	  1,       // Unique value
	 *   'Tom',   // Insert value
	 *   'Hanks', // Insert value
	 * );
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  insert column names        (eg: fname,lname)
	 * @param  insert column values       (eg: 'Tom','Hanks')
	 *
	 * @return  A prepared insert statement
	 **/
	default JSqlPreparedStatement insertStatement( //
		String tableName, // Table name to insert on
		//
		String[] insertColumns, // Columns names to insert
		Object[] insertValues // Values to insert
	) {
		// Query arguments, and query builder
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		StringBuilder queryBuilder = new StringBuilder("INSERT INTO ");
		
		// From table names
		queryBuilder.append("`" + tableName + "` (");
		
		// Collumn names
		int insertColumns_len = insertColumns.length;
		for (int i = 0; i < insertColumns_len; ++i) {
			if (i > 0) {
				queryBuilder.append(",");
			}
			queryBuilder.append(insertColumns[i]);
		}
		
		// Values  appending
		queryBuilder.append(") VALUES (");
		for (int i = 0; i < insertColumns_len; ++i) {
			if (i > 0) {
				queryBuilder.append(",");
			}
			queryBuilder.append("?");
			queryArgs.add(insertValues[i]);
		}
		queryBuilder.append(")");
		
		// Create the query set
		return prepareStatement(queryBuilder.toString(), queryArgs.toArray());
	}
	
	//-------------------------------------------------------------------------
	//
	// Multiple insert
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Does multiple insert continously. Use this command when doing,
	 * a large number of insert's to the same table with the same format.
	 *
	 * In certain SQL deployments, this larger multi-insert would be optimized as a
	 * single transaction call. However this behaviour is not guranteed across all platforms.
	 *
	 * This is incredibily useful for large meta-table object updates.
	 *
	 * @param  Table name to query
	 * @param  insert column names
	 * @param  insert column values, as a list. Each item in a list represents the respecitve row record
	 *
	 * @return  true, if insert statement executed succesfuly
	 **/
	default boolean multiinsert( //
		String tableName, // Table name to insert on
		//
		String[] insertColumns, // Columns names to update
		List<Object[]> insertValuesList // Values to update
	) {
		//
		// Note - the following is intentionally COPY - pasta
		// for the initial query building with tablename / insert columns
		//
		// This is intentional to avoid creating a whole new class object
		// just for this limited use case - in future java 9,
		// this should be abstracted into a private static function instead
		//
		
		// Query arguments, and query builder
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		StringBuilder queryBuilder = new StringBuilder("INSERT INTO ");
		
		// From table names
		queryBuilder.append("`" + tableName + "` (");
		
		// Collumn names
		int insertColumns_len = insertColumns.length;
		for (int i = 0; i < insertColumns_len; ++i) {
			if (i > 0) {
				queryBuilder.append(",");
			}
			queryBuilder.append(insertColumns[i]);
		}
		
		// Values appending
		queryBuilder.append(") VALUES ");
		int insertValuesList_len = insertValuesList.size();
		for (int r = 0; r < insertValuesList_len; ++r) {
			// Get the row record to be inserted
			Object[] insertValues = insertValuesList.get(r);
			if (r > 0) {
				queryBuilder.append(",");
			}
			
			// Insert each row
			queryBuilder.append("(");
			for (int i = 0; i < insertValues.length; ++i) {
				if (i > 0) {
					queryBuilder.append(",");
				}
				queryBuilder.append("?");
				queryArgs.add(insertValues[i]);
			}
			queryBuilder.append(")");
		}
		
		// Execute the statement
		return prepareStatement(queryBuilder.toString(), queryArgs.toArray()).update() > 0;
	}
	
}