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
 * Pure "Postgres"SQL implentation of JSql
 **/
public class JSql_Postgres extends JSql_Base {
	
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
	public JSql_Postgres(String dbHost, int dbPort, String dbName, String dbUser, String dbPass) {
		// set connection properties
		GenericConvertMap<String, Object> config = new GenericConvertHashMap<>();
		
		// Basic path, dbname, user, pass configuration
		config.put("host", dbHost);
		config.put("port", dbPort);
		config.put("name", dbName);
		config.put("user", dbUser);
		config.put("pass", dbPass);
		
		// Setup with config
		constructor_setup(config);
	}
	
	/**
	 * Runs JSql with the JDBC "MY"SQL engine
	 *
	 * @param config  config map
	 **/
	public JSql_Postgres(GenericConvertMap<String, Object> config) {
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
		sqlType = JSqlType.POSTGRESQL;
		datasource = HikaricpUtil.postgres(config);
	}
	
	//-------------------------------------------------------------------------
	//
	// Generic SQL conversion, and error sanatization
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
		// Quotes are just "wierd" in postgres (to say the least)
		// this is due to the messy mess of lower case preference done in postgres
		// see : https://dev.to/lefebvre/dont-get-bit-by-postgresql-case-sensitivity--457i
		//
		// Ironically making it the opposite of oracle
		// qString = StringReplacer.replace(qString,
		// Pattern.compile("\\S+"),
		// )


		return qString; //.replaceAll("`", "").replaceAll("'", "").replaceAll("", "");
	}
	
	//-------------------------------------------------------------------------
	//
	// JSQL prepared statements overwrites
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helps generate an SQL DROP TABLE IF EXISTS request. This function was created to acommedate the various
	 * syntax differances of DROP TABLE IF EXISTS across the various SQL vendors (if any).
	 *
	 * The syntax below, is an example of such an DROP TABLE IF EXISTS statement for SQLITE.
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * DROP TABLE IF NOT EXISTS TABLENAME
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 *
	 * @param  Table name to drop        (eg: tableName)
	 *
	 * @return  true, if DROP TABLE execution is succesful
	 **/
	public JSqlPreparedStatement dropTableStatement(String tablename //Table name to drop
	) {
		return prepareStatement("DROP TABLE IF EXISTS " + tablename + " CASCADE");
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
	 * Or more specifically for POSTGRES
	 * 
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * INSERT INTO users (id, level)
	 * VALUES (1, 0)
	 * ON CONFLICT (id) DO UPDATE
	 * SET level = users.level + 1;
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
		
		//
		// Checks that unique collumn and values length are aligned
		//
		if (uniqueColumns == null || uniqueValues == null
			|| uniqueColumns.length != uniqueValues.length) {
			throw new JSqlException(
				"Upsert query requires unique column and values to be equal length");
		}
		
		//
		// Query building of insert statement
		//
		StringBuilder queryBuilder = new StringBuilder();
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		
		// Setup the initial insert statment
		queryBuilder.append("INSERT INTO ").append(tableName).append(" (");
		
		//
		// Iterate insertion for unique, insert, and default columns
		//
		for (int i = 0; i < uniqueColumns.length; ++i) {
			if (i > 0) {
				queryBuilder.append(",");
			}
			queryBuilder.append(uniqueColumns[i]);
			queryArgs.add(uniqueValues[i]);
		}
		if (insertColumns != null) {
			for (int i = 0; i < insertColumns.length; ++i) {
				queryBuilder.append(",");
				queryBuilder.append(insertColumns[i]);
				queryArgs.add(insertValues[i]);
			}
		}
		if (defaultColumns != null) {
			for (int i = 0; i < defaultColumns.length; ++i) {
				queryBuilder.append(",");
				queryBuilder.append(defaultColumns[i]);
				queryArgs.add(defaultValues[i]);
			}
		}
		
		//
		// Close up the insert statement
		//
		queryBuilder.append(") VALUES (?");
		for (int i = 1; i < queryArgs.size(); ++i) {
			// PS the above started on 1 index, intentionally, 
			// as we skip the first "?" appending done above
			queryBuilder.append(",?");
		}
		queryBuilder.append(") ");
		
		//
		// On CONFLICT handling
		//
		queryBuilder.append("ON CONFLICT (");
		for (int i = 0; i < uniqueColumns.length; ++i) {
			if (i > 0) {
				queryBuilder.append(",");
			}
			queryBuilder.append(uniqueColumns[i]);
		}
		queryBuilder.append(") ");
		
		if (insertColumns != null && insertColumns.length > 0) {
			// Does UPDATE
			queryBuilder.append("DO UPDATE SET ");
			for (int i = 0; i < insertColumns.length; ++i) {
				if (i > 0) {
					queryBuilder.append(",");
				}
				queryBuilder.append(insertColumns[i]);
				queryBuilder.append("=EXCLUDED.");
				queryBuilder.append(insertColumns[i]);
			}
			queryBuilder.append(";");
		} else {
			// OR NOT
			queryBuilder.append("DO NOTHING;");
		}
		
		System.out.println(queryBuilder.toString());
		
		// The actual query
		return new JSqlPreparedStatement(queryBuilder.toString(), queryArgs.toArray(), this);
	}
}
