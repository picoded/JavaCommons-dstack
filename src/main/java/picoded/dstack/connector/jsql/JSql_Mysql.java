package picoded.dstack.connector.jsql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Properties;
import java.util.ArrayList;

import picoded.dstack.connector.jsql.*;
import picoded.dstack.connector.jsql.JSqlType;
import picoded.core.conv.ConvertJSON;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.GenericConvertHashMap;
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.CaseInsensitiveHashMap;
import picoded.core.struct.MutablePair;

/**
 * Pure "MY"SQL implentation of JSql
 **/
public class JSql_Mysql extends JSql_Base {
	
	//-------------------------------------------------------------------------
	//
	// Connection constructor
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Runs JSql with the JDBC "MY"SQL engine
	 *
	 * @param   dbHost, is just IP or HOSTNAME. For example, "127.0.0.1"
	 * @param   dbPort to connect to
	 * @param   dbName name to connect to (database name)
	 * @param   dbUser user to connect to
	 * @param   dbPass password to use
	 **/
	public JSql_Mysql(String dbHost, int dbPort, String dbName, String dbUser, String dbPass) {
		// set connection properties
		GenericConvertMap<String, Object> config = new GenericConvertHashMap<>();
		
		// Basic path, dbname, user, pass configuration
		config.put("host", dbHost);
		config.put("port", dbPort);
		config.put("name", dbName);
		config.put("user", dbUser);
		config.put("pass", dbPass);
		
		// Setup with config
		datasource = HikaricpUtil.mysql(config);
	}
	
	/**
	 * Runs JSql with the JDBC "MY"SQL engine
	 *
	 * @param config  config map
	 **/
	public JSql_Mysql(GenericConvertMap<String, Object> config) {
		constructor_setup(config);
	}
	
	/**
	 * Actual internal constructor setup function
	 * (called internally by all other constructor types used to
	 * work around call to constructor 'must be first statement')
	 * 
	 * @param config  config map
	 */
	public void constructor_setup(GenericConvertMap<String, Object> config) {
		datasource = HikaricpUtil.mysql(config);
	}
	
	//-------------------------------------------------------------------------
	//
	// Table type info fetching
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
	 * @param  tablename to get information on
	 *
	 * @return  Pair containing < collumn_name, collumn_type >
	 **/
	protected MutablePair<GenericConvertList<Object>, GenericConvertList<Object>> getTableColumnTypeMap_core(
		String tablename) {
		// Get the column information
		JSqlResult tableInfo = query_raw(
			"SELECT column_name, column_type FROM information_schema.columns WHERE table_name=?",
			new Object[] { tablename });
		
		// And return it as a list pair
		return new MutablePair<>(tableInfo.get("column_name"), tableInfo.get("column_type"));
	}
	
	//-------------------------------------------------------------------------
	//
	// Generic SQL conversion, and error sanatization
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Enforces column index limits for BLOB and TEXT type (used in genericSqlParser, on index creation)
	 *
	 * @param  tablename to extract information from (to normalize collumn limits)
	 * @param  Query string to sanatize
	 * @param  Columns to scan and replace
	 *
	 * @return  Sanatized query
	 **/
	protected String enforceColumnIndexLimit(String tablename, String qStringUpper, String columns) {
		if (tablename != null) {
			// fetch the table meta data info
			Map<String, String> colType = getTableColumnTypeMap(tablename);
			
			// Get the relevent column names
			String[] columnsArr = columns.split(",");
			
			// Does a search and replace if a BLOB / TEXT column without a index size limit,
			// and apply a default limit of 255
			for (String column : columnsArr) {
				column = column.trim();
				// check if column type is BLOB or TEXT
				if ("BLOB".equalsIgnoreCase(colType.get(column))
					|| "TEXT".equalsIgnoreCase(colType.get(column))) {
					// repalce the column name in the origin sql statement with column name and a suffix of (255)
					qStringUpper = qStringUpper.replace(column, column + "(255)");
				}
			}
		}
		return qStringUpper;
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
		
		// Normalize all syntax to upper case
		String qString = inString.toUpperCase(Locale.ENGLISH);
		qString = inString.trim().replaceAll("(\\s){1}", " ").replaceAll("\\s+", " ")
			.replaceAll("\"", "`")
			//.replaceAll("\'", "`")
			.replaceAll("AUTOINCREMENT", "AUTO_INCREMENT").replace("VARCHAR(MAX)", "TEXT")
			.replaceAll("RANDOM\\(\\)", "RAND()");
		
		//
		// MySQL does not support the inner query in create view
		//
		// Check if create view query has an inner query.
		// If yes, create a view from the inner query and replace the inner query with created view.
		//
		if (qString.contains("CREATE VIEW")) {
			// get view name
			int indexAs = qString.indexOf("AS");
			String viewName = "";
			if (indexAs != -1) {
				viewName = qString.substring("CREATE VIEW".length(), indexAs);
			}
			// check if any inner query
			int indexOpeningBracket = -1;
			int indexFrom = qString.indexOf("FROM") + "FROM".length();
			// find if next char is bracket
			for (int i = indexFrom; i < qString.length(); i++) {
				if (qString.charAt(i) == ' ') {
					continue;
				}
				if (qString.charAt(i) == '(') {
					indexOpeningBracket = i;
				} else {
					break;
				}
			}
			
			int indexClosingIndex = -1;
			String tmpViewName = null;
			String createViewQuery = null;
			if (indexOpeningBracket != -1) {
				tmpViewName = viewName;
				if (viewName.indexOf('_') != -1) {
					tmpViewName = viewName.split("_")[0];
				}
				tmpViewName += "_inner_view";
				indexClosingIndex = qString.indexOf(')', indexOpeningBracket);
				
				String innerQuery = qString.substring(indexOpeningBracket + 1, indexClosingIndex);
				createViewQuery = "CREATE VIEW " + tmpViewName + " AS " + innerQuery;
			}
			
			if (createViewQuery != null) {
				// execute query to drop the view if exist
				String dropViewQuery = "DROP VIEW IF EXISTS " + tmpViewName;
				
				// Drop the view 'temporarily' if previously exists
				update_raw(genericSqlParser(dropViewQuery));
				
				// execute the query to create view 'temporarily'
				update_raw(genericSqlParser(createViewQuery));
				
				// replace the inner query with created view name
				qString = qString.substring(0, indexFrom) + tmpViewName
					+ qString.substring(indexClosingIndex + 1);
			}
		}
		
		//
		// Possible "INDEX IF NOT EXISTS" call for mysql, suppress duplicate index error if needed
		//
		// This is a work around for MYSQL not supporting the "CREATE X INDEX IF NOT EXISTS" syntax
		//
		if (qString.indexOf("INDEX IF NOT EXISTS") != -1) {
			// index conflict try catch
			qString = qString.replaceAll("INDEX IF NOT EXISTS", "INDEX");
			
			// It is must to define the The length of the BLOB and TEXT column type
			//
			// This is how to enforce it
			// sample script : "CREATE UNIQUE INDEX `JSQLTEST_UNIQUE` ON `JSQLTEST` ( COL1, COL2, COL3 )"
			//
			// Find the "ON" word index
			int onIndex = qString.indexOf("ON");
			// if index == -1 then it is not a valid sql statement
			if (onIndex != -1) {
				// subtract the table name and columns from the sql statement string
				String tableAndColumnsName = qString.substring(onIndex + "ON".length());
				// Find the index of opening bracket index.
				// The column names will be enclosed between the opening and closing bracket
				// And table name will be before the opening bracket
				int openBracketIndex = tableAndColumnsName.indexOf('(');
				if (openBracketIndex != -1) {
					// extract the table name which is till the opening bracket
					String tablename = tableAndColumnsName.substring(0, openBracketIndex);
					// find the closing bracket index
					int closeBracketIndex = tableAndColumnsName.lastIndexOf(')');
					// extract the columns between the opening and closing brackets
					String columns = tableAndColumnsName.substring(openBracketIndex + 1,
						closeBracketIndex);
					
					// Enforce the column index limits
					// It is must to define the The length of the BLOB and TEXT column type
					// Append the maximum length "255" to BLOB and TEXT columns
					qString = enforceColumnIndexLimit(tablename, qString, columns);
					
					//System.out.println(">>>  "+ConvertJSON.fromMap(metadata));
					//System.out.println(">>>  "+qString);
					
				}
			}
		}
		
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
		if (super.sanatizeErrors(originalQuery, normalizedQuery, stackTrace)) {
			return true;
		}
		
		// Possible "INDEX IF NOT EXISTS" call for mysql, suppress duplicate index error if needed
		if (originalQuery.indexOf("INDEX IF NOT EXISTS") >= 0) {
			// This is the new implementation CAA 07082018.
			if (stackTrace.indexOf("java.sql.SQLSyntaxErrorException: Duplicate key name '") > 0) {
				return true;
			}
			// This is the old implementation. It uses the old verison of mysql library
			//			if (stackTrace
			//				.indexOf("com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException: Duplicate key name '") > 0) {
			//				return true;
			//			}
		}
		
		return false;
	}
	
	//-------------------------------------------------------------------------
	//
	// UPSERT Query Builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 * MYSQL specific UPSERT support
	 *
	 * The following is an example syntax
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * INSERT INTO Employee (
	 * 	id,      // Unique Columns to check for upsert
	 * 	fname,   // Insert Columns to update
	 * 	lname,   // Insert Columns to update
	 * 	role     // Default Columns, that has default fallback value
	 * ) VALUES (
	 * 	1,       // Unique value
	 * 	'Tom',   // Insert value
	 * 	'Hanks', // Insert value
	 * 	'Benchwarmer' // Default fallback value
	 * ) ON DUPLICATE KEY UPDATE
	 * 	fname = 'Tom'   //Only update upsert values
	 * 	lname = 'Hanks' // Only update upsert values
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
		String[] miscColumns // This is ignored in mysql
	) {
		
		// Checks that unique collumn and values length to be aligned
		if (uniqueColumns == null || uniqueValues == null
			|| uniqueColumns.length != uniqueValues.length) {
			throw new JSqlException(
				"Upsert query requires unique column and values to be equal length");
		}
		
		// Build the final, actual query args
		StringBuilder queryBuilder = new StringBuilder();
		StringBuilder valuesSegment = new StringBuilder();
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		
		// Prepare the prefix
		queryBuilder.append("INSERT INTO ");
		queryBuilder.append("`" + tableName + "`");
		queryBuilder.append(" ( ");
		valuesSegment.append("VALUES ( ");
		
		// Lets start with unique names / values
		for (int i = 0; i < uniqueColumns.length; ++i) {
			if (i > 0) {
				queryBuilder.append(", ");
				valuesSegment.append(", ");
			}
			
			// Actual name and values tokens
			queryBuilder.append(uniqueColumns[i]);
			valuesSegment.append("?");
			queryArgs.add(uniqueValues[i]);
		}
		
		// And the insert columns names / values
		if (insertColumns != null) {
			for (int i = 0; i < insertColumns.length; ++i) {
				// Since this is post unique value, always valid
				queryBuilder.append(", ");
				valuesSegment.append(", ");
				
				// Actual name and value tokens
				queryBuilder.append(insertColumns[i]);
				valuesSegment.append("?");
				queryArgs.add(insertValues[i]);
			}
		}
		
		// And the default columns names / values
		if (defaultColumns != null) {
			for (int i = 0; i < defaultColumns.length; ++i) {
				// Since this is post unique value, always valid
				queryBuilder.append(", ");
				valuesSegment.append(", ");
				
				// Actual name and value tokens
				queryBuilder.append(defaultColumns[i]);
				valuesSegment.append("?");
				queryArgs.add(defaultValues[i]);
			}
		}
		
		// Condensing the information
		valuesSegment.append(" ) ");
		queryBuilder.append(" ) ");
		queryBuilder.append(valuesSegment);
		
		// Handling the insert values on key conflict, see
		// https://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html
		if (insertColumns != null) {
			
			// If there is no non unique keys, its not needed to handle duplicate keys udpdate
			queryBuilder.append("ON DUPLICATE KEY UPDATE ");
			
			for (int i = 0; i < insertColumns.length; ++i) {
				if (i > 0) {
					queryBuilder.append(", ");
				}
				queryBuilder.append(insertColumns[i] + " = VALUES(" + insertColumns[i] + ")");
			}
		}
		
		// Builde the actual statement, to run!
		return new JSqlPreparedStatement(queryBuilder.toString(), queryArgs.toArray(), this);
	}
	
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
	public boolean multiUpsert( //
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
		
		// Checks that unique collumn and values
		if (uniqueColumns == null || uniqueValuesList == null) {
			throw new JSqlException("Upsert query requires unique column and values");
		}
		
		// Build the final, actual query args
		StringBuilder queryBuilder = new StringBuilder();
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		
		// Prepare the prefix
		queryBuilder.append("INSERT INTO ");
		queryBuilder.append("`" + tableName + "`");
		
		// Column definition
		//-----------------------------------------------------
		
		// Column definition opening
		queryBuilder.append(" ( ");
		
		// Lets start with unique names
		for (int i = 0; i < uniqueColumns.length; ++i) {
			if (i > 0) {
				queryBuilder.append(", ");
			}
			queryBuilder.append(uniqueColumns[i]);
		}
		
		// And the insert columns names
		if (insertColumns != null) {
			for (int i = 0; i < insertColumns.length; ++i) {
				// Since this is post unique value, always valid to append ", "
				queryBuilder.append(", ");
				queryBuilder.append(insertColumns[i]);
			}
		}
		
		// And the default columns names
		if (defaultColumns != null) {
			for (int i = 0; i < defaultColumns.length; ++i) {
				// Since this is post unique value, always valid to append ", "
				queryBuilder.append(", ");
				queryBuilder.append(defaultColumns[i]);
			}
		}
		
		// Column definition closing
		queryBuilder.append(" ) ");
		
		// Values definition
		//-----------------------------------------------------
		
		// Values definition opening
		queryBuilder.append("VALUES ");
		
		// Iterate every row record to upsert
		for (int r = 0; r < uniqueValuesList.size(); ++r) {
			if (r > 0) { // Multiple row record, requires comma seperator
				queryBuilder.append(", ");
			}
			
			// Open the row
			queryBuilder.append("( ");
			
			// Unique values always assumed
			Object[] uniqueValues = uniqueValuesList.get(r);
			// Lets start with unique values processing
			for (int i = 0; i < uniqueColumns.length; ++i) {
				if (i > 0) {
					queryBuilder.append(", ");
				}
				queryBuilder.append("?");
				queryArgs.add(uniqueValues[i]);
			}
			
			// Lets move on to insert, and default values
			Object[] insertValues = null;
			Object[] defaultValues = null;
			
			// Insert, and default values may or may not occur
			if (insertValuesList != null && r < insertValuesList.size()) {
				insertValues = insertValuesList.get(r);
			}
			if (defaultValuesList != null && r < defaultValuesList.size()) {
				defaultValues = defaultValuesList.get(r);
			}
			
			// Regardless if the list produced values handle based on names
			// And the insert columns names / values
			if (insertColumns != null) {
				for (int i = 0; i < insertColumns.length; ++i) {
					// Since this is post unique value, always valid to append ", "
					queryBuilder.append(", ");
					queryBuilder.append("?");
					
					// Process the value
					if (insertValues != null && i < insertValues.length) {
						queryArgs.add(insertValues[i]);
					} else {
						queryArgs.add(null);
					}
				}
			}
			// And the insert columns names / values
			if (defaultColumns != null) {
				for (int i = 0; i < defaultColumns.length; ++i) {
					// Since this is post unique value, always valid to append ", "
					queryBuilder.append(", ");
					queryBuilder.append("?");
					
					// Process the value
					if (defaultValues != null && i < defaultValues.length) {
						queryArgs.add(defaultValues[i]);
					} else {
						queryArgs.add(null);
					}
				}
			}
			
			// Close the row
			queryBuilder.append(") ");
		}
		
		// Handling the insert values on key conflict ruling, see
		// https://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html
		if (insertColumns != null) {
			
			// If there is no non unique keys, its not needed to handle duplicate keys udpdate
			queryBuilder.append("ON DUPLICATE KEY UPDATE ");
			
			// Set the rule for insert columns replacement
			for (int i = 0; i < insertColumns.length; ++i) {
				if (i > 0) {
					queryBuilder.append(", ");
				}
				queryBuilder.append(insertColumns[i] + " = VALUES(" + insertColumns[i] + ")");
			}
		}
		// Builde the actual statement, to run!
		JSqlPreparedStatement statement = new JSqlPreparedStatement(queryBuilder.toString(),
			queryArgs.toArray(), this);
		
		// Run it
		return statement.update() >= 1;
	}
	
}
