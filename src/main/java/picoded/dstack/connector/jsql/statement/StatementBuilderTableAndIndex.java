package picoded.dstack.connector.jsql.statement;

import java.util.ArrayList;
import java.util.logging.Logger;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.connector.jsql.*;

/**
 * CREATE TABLE statement builder
 **/
public interface StatementBuilderTableAndIndex extends StatementBuilderBaseInterface {
	
	/**
	 * Internal self used logger
	 **/
	public static final Logger LOGGER = Logger.getLogger(JSql.class.getName());
	
	//-------------------------------------------------------------------------
	//
	// CREATE TABLE statement builder
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
	 *
	 * @return  true, if CREATE TABLE execution is succesful
	 **/
	default boolean createTable( //
		String tableName, // Table name to create
		String[] columnName, // The column names
		String[] columnTypes // The column types
	) {
		return createTableStatement(tableName, columnName, columnTypes).update() >= 0;
	}
	
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
	 *
	 * @return  A prepared CREATE TABLE statement
	 **/
	default JSqlPreparedStatement createTableStatement( //
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
	// DROP TABLE statement builder
	//
	// Note  : While not needed for current JavaCommons use case
	//         due to the extensive GUID usage of dstack. This may
	//         have use cases outside the core JavaCommons stack.
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
	default boolean dropTable(String tablename //Table name to drop
	) {
		return (dropTableStatement(tablename).update() >= 0);
	}
	
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
	default JSqlPreparedStatement dropTableStatement(String tablename //Table name to drop
	) {
		return prepareStatement("DROP TABLE IF EXISTS " + tablename);
	}
	
	//-------------------------------------------------------------------------
	//
	// CREATE INDEX statement builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helps generate an SQL CREATE INDEX request. This function was created to acommedate the various
	 * syntax differances of CREATE INDEX across the various SQL vendors (if any).
	 *
	 * See : createIndexStatement for full docs
	 *
	 * @param  Table name to query            (eg: tableName)
	 * @param  Column names to build an index (eg: col1,col2)
	 *
	 * @return  A prepared CREATE INDEX statement
	 **/
	default boolean createIndex( //
		String tableName, // Table name to select from
		//
		String columnNames // The column name to create the index on
	) {
		return createIndexStatement(tableName, columnNames, null, null).update() >= 0;
	}
	
	/**
	 * Helps generate an SQL CREATE INDEX request. This function was created to acommedate the various
	 * syntax differances of CREATE INDEX across the various SQL vendors (if any).
	 *
	 * See : createIndexStatement for full docs
	 *
	 * @param  Table name to query            (eg: tableName)
	 * @param  Column names to build an index (eg: col1,col2)
	 * @param  Index type to build            (eg: UNIQUE)
	 *
	 * @return  A prepared CREATE INDEX statement
	 **/
	default boolean createIndex( //
		String tableName, // Table name to select from
		//
		String columnNames, // The column name to create the index on
		//
		String indexType // The index type if given, can be null
	) {
		return createIndexStatement(tableName, columnNames, indexType, null).update() >= 0;
	}
	
	/**
	 * Helps generate an SQL CREATE INDEX request. This function was created to acommedate the various
	 * syntax differances of CREATE INDEX across the various SQL vendors (if any).
	 *
	 * See : createIndexStatement for full docs
	 *
	 * @param  Table name to query            (eg: tableName)
	 * @param  Column names to build an index (eg: col1,col2)
	 * @param  Index type to build            (eg: UNIQUE)
	 * @param  Index suffix to use            (eg: SpecialIndex)
	 *
	 * @return  A prepared CREATE INDEX statement
	 **/
	default boolean createIndex( //
		String tableName, // Table name to select from
		//
		String columnNames, // The column name to create the index on
		//
		String indexType, // The index type if given, can be null
		//
		String indexSuffix // The index name suffix, its auto generated if null
	) {
		return createIndexStatement(tableName, columnNames, indexType, indexSuffix).update() >= 0;
	}
	
	/**
	 * Helps generate an SQL CREATE INDEX request. This function was created to acommedate the various
	 * syntax differances of CREATE INDEX across the various SQL vendors (if any).
	 *
	 * The syntax below, is an example of such an CREATE INDEX statement for SQLITE.
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * CREATE (UNIQUE|FULLTEXT) INDEX IF NOT EXISTS TABLENAME_SUFFIX ON TABLENAME ( COLLUMNS )
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 *
	 * @param  Table name to query            (eg: tableName)
	 * @param  Column names to build an index (eg: col1,col2)
	 * @param  Index type to build            (eg: UNIQUE)
	 * @param  Index suffix to use            (eg: SpecialIndex)
	 *
	 * @return  A prepared CREATE INDEX statement
	 **/
	default JSqlPreparedStatement createIndexStatement( //
		String tableName, // Table name to select from
		//
		String columnNames, // The column name to create the index on
		//
		String indexType, // The index type if given, can be null
		//
		String indexSuffix // The index name suffix, its auto generated if null
	) {
		if (tableName.length() > 30) {
			LOGGER.warning(JSqlException.oracleNameSpaceWarning + tableName);
		}
		
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		StringBuilder queryBuilder = new StringBuilder("CREATE ");
		
		if (indexType != null && indexType.length() > 0) {
			queryBuilder.append(indexType);
			queryBuilder.append(" ");
		}
		
		queryBuilder.append("INDEX IF NOT EXISTS ");
		
		// Creates a suffix, based on the collumn names
		if (indexSuffix == null || indexSuffix.length() <= 0) {
			indexSuffix = columnNames.replaceAll("/[^A-Za-z0-9]/", ""); //.toUpperCase(Locale.ENGLISH)?
		}
		
		if ((tableName.length() + 1 + indexSuffix.length()) > 30) {
			LOGGER.warning(JSqlException.oracleNameSpaceWarning + tableName + "_" + indexSuffix);
		}
		
		queryBuilder.append("`");
		queryBuilder.append(tableName);
		queryBuilder.append("_");
		queryBuilder.append(indexSuffix);
		queryBuilder.append("` ON `");
		queryBuilder.append(tableName);
		queryBuilder.append("` (");
		queryBuilder.append(columnNames);
		queryBuilder.append(")");
		
		// Create the query set
		return new JSqlPreparedStatement(queryBuilder.toString(), queryArgs.toArray(), (JSql) this);
	}
	
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
	 * @return  Collumn name to type mapping
	 **/
	default public GenericConvertMap<String, String> getTableColumnTypeMap(String tablename) {
		throw new UnsupportedOperationException(
			"getTableColumnTypeMap for given SQL type is not supported yet");
	}
	
}