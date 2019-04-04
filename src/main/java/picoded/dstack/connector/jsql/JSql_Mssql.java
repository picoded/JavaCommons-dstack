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
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Arrays;

import picoded.dstack.connector.jsql.*;
import picoded.dstack.connector.jsql.JSqlType;
import picoded.core.conv.ConvertJSON;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.GenericConvertHashMap;
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.CaseInsensitiveHashMap;
import picoded.core.struct.MutablePair;

// We will not be using connection pooling for MSSQL,
// lets use the datasource directly
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

/**
 * Pure "MS"SQL implentation of JSql
 **/
public class JSql_Mssql extends JSql_Base {
	
	//-------------------------------------------------------------------------
	//
	// Connection constructor
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Runs JSql with the JDBC "MS"SQL engine
	 *
	 * @param   dbHost, is just IP or HOSTNAME. For example, "127.0.0.1"
	 * @param   dbPort to connect to
	 * @param   dbName name to connect to (database name)
	 * @param   dbUser user to connect to
	 * @param   dbPass password to use
	 **/
	public JSql_Mssql(String dbHost, int dbPort, String dbName, String dbUser, String dbPass) {
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
	public JSql_Mssql(GenericConvertMap<String, Object> config) {
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
		sqlType = JSqlType.MSSQL;
		
		// Create MSSQL datasource.
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser(config.getString("user", ""));
		ds.setPassword(config.getString("pass", ""));
		ds.setServerName(config.getString("host", ""));
		ds.setPortNumber(config.getInt("port", 1433));
		ds.setDatabaseName(config.getString("name", ""));
		datasource = ds;
		
		// HikariCP implementation is having huge connection overheads
		// datasource = HikaricpUtil.mssql(config);
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
	 * Note : returned map should be a `CaseInsensitiveHashMap`
	 *
	 * @param  tablename to get information on
	 *
	 * @return  Pair containing < collumn_name, collumn_type >
	 **/
	protected MutablePair<GenericConvertList<Object>, GenericConvertList<Object>> getTableColumnTypeMap_core(
		String tablename) {
		// Get the column information
		JSqlResult tableInfo = query_raw(
			"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME=?",
			new Object[] { tablename });
		
		// And return it as a list pair
		return new MutablePair<>(tableInfo.get("COLUMN_NAME"), tableInfo.get("DATA_TYPE"));
	}
	
	//-------------------------------------------------------------------------
	//
	// Generic SQL update - return count overwrite
	//
	//-------------------------------------------------------------------------
	
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
		// Perform the original update call
		int res = super.update(qString, values);
		
		// Normalize certain known age cases
		if (res < 0) {
			if (qString.contains("DROP") || qString.contains("IF EXISTS")) {
				return 0;
			}
			if (qString.contains("TRUNCATE TABLE")) {
				return 0;
			}
		}
		
		// Return original result
		return res;
	}
	
	//-------------------------------------------------------------------------
	//
	// Generic SQL conversion, and error sanatization
	//
	//-------------------------------------------------------------------------
	
	// Internal parser that converts some of the common sql statements to mssql
	public String genericSqlParser(String inString) {
		String fixedQuotes = inString.trim().replaceAll("(\\s){1}", " ").replaceAll("`", "\"")
			.replaceAll("'", "\"").replaceAll("\\s+", " ").replaceAll(" =", "=").replaceAll("= ", "=")
			.trim();
		
		String upperCaseStr = fixedQuotes.toUpperCase();
		String qString = fixedQuotes;
		
		String qStringPrefix = "";
		String qStringSuffix = "";
		
		final String ifExists = "IF EXISTS";
		final String ifNotExists = "IF NOT EXISTS";
		
		final String create = "CREATE";
		final String drop = "DROP";
		final String table = "TABLE";
		final String select = "SELECT";
		final String update = "UPDATE";
		
		final String view = "VIEW";
		
		final String insertInto = "INSERT INTO";
		final String deleteFrom = "DELETE FROM";
		
		final String[] indexTypeArr = { "UNIQUE", "FULLTEXT", "SPATIAL" };
		final String index = "INDEX";
		
		String indexType;
		String tmpStr;
		int tmpIndx;
		
		Pattern createIndexType = Pattern.compile("((UNIQUE|FULLTEXT|SPATIAL) ){0,1}INDEX.*");
		
		int prefixOffset = 0;
		if (upperCaseStr.startsWith(drop)) { //DROP
			upperCaseStr = upperCaseStr.replaceAll(ifNotExists, ifExists);
			fixedQuotes = fixedQuotes.replaceAll(ifNotExists, ifExists);
			
			prefixOffset = drop.length() + 1;
			
			if (upperCaseStr.startsWith(table, prefixOffset)) { //TABLE
				prefixOffset += table.length() + 1;
				
				if (upperCaseStr.startsWith(ifExists, prefixOffset)) { //IF EXISTS
					prefixOffset += ifExists.length() + 1;
					
					qStringPrefix = "BEGIN TRY IF OBJECT_ID('"
						+ fixedQuotes.substring(prefixOffset).toUpperCase() + "', 'U')"
						+ " IS NOT NULL DROP TABLE " + fixedQuotes.substring(prefixOffset)
						+ " END TRY BEGIN CATCH END CATCH";
				} else {
					qStringPrefix = "DROP TABLE ";
				}
				qString = qStringPrefix;
			} else if (upperCaseStr.startsWith(index, prefixOffset)) { //INDEX
			
			} else if (upperCaseStr.startsWith(view, prefixOffset)) { //VIEW
				prefixOffset += view.length() + 1;
				
				if (upperCaseStr.startsWith(ifExists, prefixOffset)) { //IF EXISTS
					prefixOffset += ifExists.length() + 1;
					
					qStringPrefix = "BEGIN TRY IF OBJECT_ID('"
						+ fixedQuotes.substring(prefixOffset).toUpperCase() + "', 'V')"
						+ " IS NOT NULL DROP VIEW " + fixedQuotes.substring(prefixOffset)
						+ " END TRY BEGIN CATCH END CATCH";
				} else {
					qStringPrefix = "DROP VIEW ";
				}
			}
		} else if (upperCaseStr.startsWith(create)) { //CREATE
			prefixOffset = create.length() + 1;
			
			if (upperCaseStr.startsWith(table, prefixOffset)) { //TABLE
				prefixOffset += table.length() + 1;
				
				if (upperCaseStr.startsWith(ifNotExists, prefixOffset)) { //IF NOT EXISTS
					prefixOffset += ifNotExists.length() + 1;
					//get the table name from incoming query
					String tableName = _getTableName(fixedQuotes.substring(prefixOffset));
					qStringPrefix = "BEGIN TRY IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'"
						+ tableName
						+ "')"
						+ " AND OBJECTPROPERTY(id, N'"
						+ tableName
						+ "')"
						+ " = 1) CREATE TABLE ";
					qStringSuffix = " END TRY BEGIN CATCH END CATCH";
				} else {
					qStringPrefix = "CREATE TABLE ";
				}
				qString = _fixTableNameInMssqlSubQuery(fixedQuotes.substring(prefixOffset));
				qString = _simpleMysqlToMssql_collumnSubstitude(qString);
			} else {
				// logger.finer("Trying to matched INDEX : " + upperCaseStr.substring(prefixOffset));
				if (createIndexType.matcher(upperCaseStr.substring(prefixOffset)).matches()) { //UNIQUE|FULLTEXT|SPATIAL|_ INDEX
					// logger.finer("Matched INDEX : " + inString);
					
					//Find the index type
					indexType = null;
					for (int a = 0; a < indexTypeArr.length; ++a) {
						if (upperCaseStr.startsWith(indexTypeArr[a], prefixOffset)) {
							prefixOffset += indexTypeArr[a].length() + 1;
							indexType = indexTypeArr[a];
							break;
						}
					}
					
					//only bother if it matches (shd be right?)
					if (upperCaseStr.startsWith(index, prefixOffset)) {
						prefixOffset += index.length() + 1;
						
						//If not exists wrapper
						if (upperCaseStr.startsWith(ifNotExists, prefixOffset)) {
							prefixOffset += ifNotExists.length() + 1;
							qStringPrefix = "";
							qStringSuffix = "";
						}
						
						tmpStr = _fixTableNameInMssqlSubQuery(fixedQuotes.substring(prefixOffset));
						tmpIndx = tmpStr.indexOf(" ON ");
						
						if (tmpIndx > 0) {
							qString = "BEGIN TRY CREATE " + ((indexType != null) ? indexType + " " : "")
								+ "INDEX " + tmpStr.substring(0, tmpIndx) + " ON "
								+ _fixTableNameInMssqlSubQuery(tmpStr.substring(tmpIndx + 4))
								+ " END TRY BEGIN CATCH END CATCH";
						}
						
					}
				}
			}
		} else if (upperCaseStr.startsWith(insertInto)) { //INSERT INTO
			prefixOffset = insertInto.length() + 1;
			
			tmpStr = _fixTableNameInMssqlSubQuery(fixedQuotes.substring(prefixOffset));
			
			qString = "INSERT INTO " + tmpStr;
		} else if (upperCaseStr.startsWith(select)) { //SELECT
			prefixOffset = select.length() + 1;
			
			tmpStr = qString.substring(prefixOffset);
			tmpIndx = qString.toUpperCase().indexOf(" FROM ");
			
			if (tmpIndx > 0) {
				qString = "SELECT " + tmpStr.substring(0, tmpIndx - 7)
				//.replaceAll("\"", "'")
					.replaceAll("`", "\"") + " FROM "
					+ _fixTableNameInMssqlSubQuery(tmpStr.substring(tmpIndx - 1));
			} else {
				qString = _fixTableNameInMssqlSubQuery(fixedQuotes);
			}
			
			prefixOffset = 0;
			//Fix the "AS" quotation
			while ((tmpIndx = qString.indexOf(" AS ", prefixOffset)) > 0) {
				prefixOffset = qString.indexOf(" ", tmpIndx + 4);
				
				if (prefixOffset > 0) {
					qString = qString.substring(0, tmpIndx)
						+ qString.substring(tmpIndx, prefixOffset).replaceAll("`", "\"")
							.replaceAll("'", "\"") + qString.substring(prefixOffset);
				} else {
					break;
				}
			}
			// Fix the pagination query as per the SQL Server 2012 syntax by using the OFFSET/FETCH
			String prefixQuery = null;
			int offsetIndex = qString.indexOf("OFFSET");
			String offsetQuery = "";
			if (offsetIndex != -1) {
				prefixQuery = qString.substring(0, offsetIndex);
				offsetQuery = qString.substring(offsetIndex);
				offsetQuery += " ROWS ";
			}
			int limitIndex = qString.indexOf("LIMIT");
			String limitQuery = "";
			if (limitIndex != -1) {
				
				// Includes offset 0, if its missing (required for MSSQL)
				if (offsetIndex == -1) {
					offsetQuery = "OFFSET 0 ROWS ";
				}
				
				prefixQuery = qString.substring(0, limitIndex);
				if (offsetIndex != -1) {
					limitQuery = qString.substring(limitIndex, offsetIndex);
				} else {
					limitQuery = qString.substring(limitIndex);
				}
				limitQuery = limitQuery.replace("LIMIT", "FETCH NEXT");
				limitQuery += " ROWS ONLY ";
			}
			if (prefixQuery != null) {
				qString = prefixQuery + offsetQuery + limitQuery;
			}
			
			// Replace ORDER BY RANDOM()
			// with ORDER BY NEWID()
			// https://stackoverflow.com/questions/19412/how-to-request-a-random-row-in-sql
			qString = qString.replaceAll("ORDER BY RANDOM\\(\\)", "ORDER BY NEWID()").replaceAll(
				"ORDER BY RAND\\(\\)", "ORDER BY NEWID()");
			
		} else if (upperCaseStr.startsWith(deleteFrom)) {
			prefixOffset = deleteFrom.length() + 1;
			
			tmpStr = _fixTableNameInMssqlSubQuery(qString.substring(prefixOffset));
			qString = deleteFrom + " " + tmpStr;
			
		} else if (upperCaseStr.startsWith(update)) { //UPDATE
			prefixOffset = update.length() + 1;
			
			tmpStr = _fixTableNameInMssqlSubQuery(qString.substring(prefixOffset));
			qString = update + " " + tmpStr;
		}
		//Drop table query modication
		if (qString.contains("DROP")) {
			qString = qStringPrefix;
		} else {
			qString = qStringPrefix + qString + qStringSuffix;
		}
		
		if (qString.contains("CREATE TABLE")) {
			// Replace PRIMARY KEY AUTOINCREMENT with IDENTITY
			if (qString.contains("AUTOINCREMENT")) {
				qString = qString.replaceAll("AUTOINCREMENT", "IDENTITY");
			}
			
			//Convert MY-Sql NUMBER data type to NUMERIC data type for Ms-sql
			if (qString.contains("NUMBER")) {
				qString = qString.replaceAll("NUMBER", "NUMERIC");
			}
		}
		
		//remove ON DELETE FOR CLIENTSTATUSHISTORY---> this block needs to be refined for future.
		if (qString.contains("ON DELETE")) { //qString.contains("CLIENTSTATUSHISTORY") &&
			qString = qString.replaceAll("ON DELETE SET NULL", "");
		}
		
		// Replace double quote (") with single quote (') for assignment values
		StringBuilder sb = new StringBuilder(qString);
		int endIndex = qString.indexOf("=");
		int beginIndex = 0;
		while (endIndex != -1) {
			endIndex++;
			beginIndex = endIndex;
			if (sb.charAt(beginIndex) == '"') {
				for (; beginIndex < sb.length(); beginIndex++) {
					if (sb.charAt(beginIndex) == '"') {
						sb.setCharAt(beginIndex, '\'');
					} else if (sb.charAt(beginIndex) == ' ') {
						break;
					}
				}
			}
			endIndex = sb.indexOf("=", beginIndex);
		}
		qString = sb.toString();
		return qString;
	}
	
	//-------------------------------------------------------------------------
	//
	// Utiility function used by genericCOnvert
	//
	//-------------------------------------------------------------------------
	
	// Method to return table name from incoming query string
	private static String _getTableName(String qString) {
		qString = qString.trim();
		int indxPt = ((indxPt = qString.indexOf(' ')) <= -1) ? qString.length() : indxPt;
		String tableStr = qString.substring(0, indxPt).toUpperCase();
		return tableStr; //retrun the table name
	}
	
	private static String _fixTableNameInMssqlSubQuery(String qString) {
		qString = qString.trim();
		int indxPt = ((indxPt = qString.indexOf(' ')) <= -1) ? qString.length() : indxPt;
		String tableStr = qString.substring(0, indxPt).toUpperCase();
		
		qString = tableStr + qString.substring(indxPt);
		
		while (qString.endsWith(";")) { //Remove uneeded trailing ";" semi collons
			qString = qString.substring(0, qString.length() - 1);
		}
		return qString;
	}
	
	/// Collumn type correction from mysql to ms sql
	private static String _simpleMysqlToMssql_collumnSubstitude(String qString) {
		qString = qString.replaceAll("(?i)BLOB", "VARBINARY(MAX)");
		
		// Work around default table value quotes
		// sadly variable arguments are NOT allowed in create table statements
		//
		// LIKE WHY?????
		// java.sql.SQLException: Variables are not allowed in the CREATE TABLE statement.
		
		// // @TODO : Properly fix default statement (does not seem to work)
		// int lastCheckedIdx = 0;
		// int defaultTxtIdx = 0;
		// while( (defaultTxtIdx = qString.indexOf("DEFAULT", lastCheckedIdx)) > 0 ) {
		// 	int defaultTxtIdxEnd = defaultTxtIdx + "DEFAULT".length();
		
		// 	// Replace double quote with literal string
		// 	String beforeDefaultStatement = qString.substring(0, defaultTxtIdx);
		// 	String afterDefaultStatement = qString.substring(defaultTxtIdxEnd);
		// 	afterDefaultStatement = afterDefaultStatement.replaceFirst("\\s+\"", "('").replaceFirst("\"", "')");
		
		// 	// Upate the string correctly
		// 	qString = beforeDefaultStatement + "CONSTRAINT D_"+defaultTxtIdx+" DEFAULT";
		// 	qString = qString.replaceAll("DEFAULT\\s+\\(", "DEFAULT(");
		// 	lastCheckedIdx = qString.length();
		// 	qString = qString + afterDefaultStatement;
		// 	return qString;
		// }
		
		return qString;
	}
	
	//-------------------------------------------------------------------------
	//
	// UPSERT statement support
	//
	//-------------------------------------------------------------------------
	
	///
	/// Helps generate an SQL UPSERT request. This function was created to acommedate the various
	/// syntax differances of UPSERT across the various SQL vendors.
	///
	/// Note that care should be taken to prevent SQL injection via the given statment strings.
	///
	/// The syntax below, is an example of such an UPSERT statement for Oracle.
	///
	/// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	/// MERGE
	/// INTO Employee AS destTable
	/// USING (SELECT
	///	1 AS id,      // Unique value
	/// 	'C3PO' AS name, // Insert value
	///	COALESCE((SELECT role FROM Employee WHERE id = 1), 'Benchwarmer') AS role, // Values with default
	///	(SELECT note FROM Employee WHERE id = 1) AS note // Misc values to preserve
	/// ) AS sourceTable
	/// ON (destTable.id = sourceTable.id)
	/// WHEN MATCHED THEN
	/// INSERT (
	///	id,     // Unique Columns to check for upsert
	///	name,   // Insert Columns to update
	///	role,   // Default Columns, that has default fallback value
	///   note,   // Misc Columns, which existing values are preserved (if exists)
	/// ) VALUES (
	///	1,      // Unique value
	/// 	'C3PO', // Insert value
	///	COALESCE((SELECT role FROM Employee WHERE id = 1), 'Benchwarmer'), // Values with default
	///	(SELECT note FROM Employee WHERE id = 1) // Misc values to preserve
	/// )
	/// WHEN NOT MATCHED THEN
	/// UPDATE
	/// SET     name = 'C3PO', // Insert value
	///         role = COALESCE((SELECT role FROM Employee WHERE id = 1), 'Benchwarmer'), // Values with default
	///         note = (SELECT note FROM Employee WHERE id = 1) // Misc values to preserve
	/// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	///
	public JSqlPreparedStatement upsertStatement( //
		String tableName, // Table name to upsert on
		//
		String[] uniqueColumns, // The unique column names
		Object[] uniqueValues, // The row unique identifier values
		//
		String[] insertColumns, // Columns names to update
		Object[] insertValues, // Values to update
		//
		String[] defaultColumns, // Columns names to apply default value, if not exists
		Object[] defaultValues, // Values to insert, that is not updated. Note that this is ignored if pre-existing values exists
		//
		// Various column names where its existing value needs to be maintained (if any),
		// this is important as some SQL implementation will fallback to default table values, if not properly handled
		String[] miscColumns //
	) throws JSqlException {
		
		if (tableName.length() > 30) {
			//logger.warning(JSqlException.oracleNameSpaceWarning + tableName);
		}
		
		/// Checks that unique collumn and values length to be aligned
		if (uniqueColumns == null || uniqueValues == null
			|| uniqueColumns.length != uniqueValues.length) {
			throw new JSqlException(
				"Upsert query requires unique column and values to be equal length");
		}
		
		/// Preparing inner default select, this will be used repeatingly for COALESCE, DEFAULT and MISC values
		ArrayList<Object> innerSelectArgs = new ArrayList<Object>();
		StringBuilder innerSelectSB = new StringBuilder(" FROM ");
		innerSelectSB.append("`" + tableName + "`");
		innerSelectSB.append(" WHERE ");
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
		
		String equalSign = "=";
		String targetTableAlias = "target";
		String sourceTableAlias = "source";
		String statementTerminator = ";";
		
		/// Building the query for INSERT OR REPLACE
		StringBuilder queryBuilder = new StringBuilder("MERGE INTO `" + tableName + "` AS "
			+ targetTableAlias);
		
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		ArrayList<Object> insertQueryArgs = new ArrayList<Object>();
		ArrayList<Object> updateQueryArgs = new ArrayList<Object>();
		ArrayList<Object> selectQueryArgs = new ArrayList<Object>();
		
		/// Building the query for both sides of '(...columns...) VALUE (...vars...)' clauses in upsert
		/// Note that the final trailing ", " seperator will be removed prior to final query conversion
		StringBuilder selectColumnNames = new StringBuilder();
		StringBuilder updateColumnNames = new StringBuilder();
		StringBuilder insertColumnNames = new StringBuilder();
		StringBuilder insertColumnValues = new StringBuilder();
		StringBuilder condition = new StringBuilder();
		String columnSeperator = ", ";
		
		/// Setting up unique values
		for (int a = 0; a < uniqueColumns.length; ++a) {
			// dual select
			selectColumnNames.append("?");
			selectColumnNames.append(" AS ");
			selectColumnNames.append(uniqueColumns[a]);
			selectColumnNames.append(columnSeperator);
			
			selectQueryArgs.add(uniqueValues[a]);
			
			// insert column list
			insertColumnNames.append(uniqueColumns[a]);
			insertColumnNames.append(columnSeperator);
			// insert column value list
			insertColumnValues.append("?");
			insertColumnValues.append(columnSeperator);
			//
			insertQueryArgs.add(uniqueValues[a]);
		}
		
		/// Inserting updated values
		if (insertColumns != null) {
			for (int a = 0; a < insertColumns.length; ++a) {
				// update column
				updateColumnNames.append(insertColumns[a]);
				updateColumnNames.append(equalSign);
				updateColumnNames.append("?");
				updateColumnNames.append(columnSeperator);
				
				updateQueryArgs.add((insertValues != null && insertValues.length > a) ? insertValues[a]
					: null);
				
				// select dual
				selectColumnNames.append("?");
				selectColumnNames.append(" AS ");
				selectColumnNames.append(insertColumns[a]);
				selectColumnNames.append(columnSeperator);
				
				selectQueryArgs.add((insertValues != null && insertValues.length > a) ? insertValues[a]
					: null);
				
				// insert column
				insertColumnNames.append(insertColumns[a]);
				insertColumnNames.append(columnSeperator);
				
				insertColumnValues.append("?");
				insertColumnValues.append(columnSeperator);
				
				insertQueryArgs.add((insertValues != null && insertValues.length > a) ? insertValues[a]
					: null);
			}
		}
		
		/// Handling default values
		if (defaultColumns != null) {
			for (int a = 0; a < defaultColumns.length; ++a) {
				// insert column
				insertColumnNames.append(defaultColumns[a]);
				insertColumnNames.append(columnSeperator);
				
				insertColumnValues.append("COALESCE(");
				insertColumnValues.append(innerSelectPrefix);
				insertColumnValues.append(defaultColumns[a]);
				insertColumnValues.append(innerSelectSuffix);
				
				insertQueryArgs.addAll(innerSelectArgs);
				
				insertColumnValues.append(", ?)");
				insertColumnValues.append(columnSeperator);
				
				insertQueryArgs
					.add((defaultValues != null && defaultValues.length > a) ? defaultValues[a] : null);
				
				// update column
				updateColumnNames.append(defaultColumns[a]);
				updateColumnNames.append(equalSign);
				updateColumnNames.append("COALESCE(");
				updateColumnNames.append(innerSelectPrefix);
				updateColumnNames.append(defaultColumns[a]);
				updateColumnNames.append(innerSelectSuffix);
				
				updateQueryArgs.addAll(innerSelectArgs);
				
				updateColumnNames.append(", ?)");
				updateColumnNames.append(columnSeperator);
				updateQueryArgs
					.add((defaultValues != null && defaultValues.length > a) ? defaultValues[a] : null);
				
				// select dual
				// COALESCE((SELECT col3 from t where a=?), ?) as col3
				selectColumnNames.append("COALESCE(");
				selectColumnNames.append(innerSelectPrefix);
				selectColumnNames.append(defaultColumns[a]);
				selectColumnNames.append(innerSelectSuffix);
				selectColumnNames.append(", ?)");
				
				selectQueryArgs.addAll(innerSelectArgs);
				
				selectColumnNames.append(" AS " + defaultColumns[a] + columnSeperator);
				selectQueryArgs
					.add((defaultValues != null && defaultValues.length > a) ? defaultValues[a] : null);
			}
		}
		
		/// Handling Misc values
		if (miscColumns != null) {
			for (int a = 0; a < miscColumns.length; ++a) {
				// insert column
				insertColumnNames.append(miscColumns[a]);
				insertColumnNames.append(columnSeperator);
				
				insertColumnValues.append(innerSelectPrefix);
				insertColumnValues.append(miscColumns[a]);
				insertColumnValues.append(innerSelectSuffix);
				
				insertQueryArgs.addAll(innerSelectArgs);
				
				insertColumnValues.append(columnSeperator);
				
				// updtae column
				updateColumnNames.append(miscColumns[a]);
				updateColumnNames.append(equalSign);
				updateColumnNames.append(innerSelectPrefix);
				updateColumnNames.append(miscColumns[a]);
				updateColumnNames.append(innerSelectSuffix);
				updateColumnNames.append(columnSeperator);
				
				updateQueryArgs.addAll(innerSelectArgs);
				
				// select dual
				selectColumnNames.append(innerSelectPrefix);
				selectColumnNames.append(miscColumns[a]);
				selectColumnNames.append(innerSelectSuffix);
				
				selectColumnNames.append(" AS ");
				selectColumnNames.append(miscColumns[a]);
				selectColumnNames.append(columnSeperator);
				
				selectQueryArgs.addAll(innerSelectArgs);
				
			}
		}
		
		/// Setting up the condition
		for (int a = 0; a < uniqueColumns.length; ++a) {
			if (a > 0) {
				condition.append(" and ");
			}
			condition.append(targetTableAlias);
			condition.append(".");
			condition.append(uniqueColumns[a]);
			condition.append(equalSign);
			condition.append(sourceTableAlias);
			condition.append(".");
			
			condition.append(uniqueColumns[a]);
		}
		
		/// Building the final query
		queryBuilder.append(" USING (SELECT ");
		queryBuilder.append(selectColumnNames.substring(0, selectColumnNames.length()
			- columnSeperator.length()));
		queryBuilder.append(")");
		queryBuilder.append(" AS ");
		queryBuilder.append(sourceTableAlias);
		queryBuilder.append(" ON ( ");
		queryBuilder.append(condition.toString());
		queryBuilder.append(" ) ");
		
		if (updateColumnNames.length() > 0) {
			queryBuilder.append(" WHEN MATCHED ");
			queryBuilder.append(" THEN UPDATE SET ");
			queryBuilder.append(updateColumnNames.substring(0, updateColumnNames.length()
				- columnSeperator.length()));
		}
		
		queryBuilder.append(" WHEN NOT MATCHED ");
		queryBuilder.append(" THEN INSERT (");
		queryBuilder.append(insertColumnNames.substring(0, insertColumnNames.length()
			- columnSeperator.length()));
		queryBuilder.append(") VALUES (");
		queryBuilder.append(insertColumnValues.substring(0, insertColumnValues.length()
			- columnSeperator.length()));
		queryBuilder.append(")");
		queryBuilder.append(statementTerminator);
		
		queryArgs.addAll(selectQueryArgs);
		queryArgs.addAll(updateQueryArgs);
		queryArgs.addAll(insertQueryArgs);
		
		return new JSqlPreparedStatement(queryBuilder.toString(), queryArgs.toArray(), this);
	}
	
	// Helper varient, without default or misc fields
	public JSqlPreparedStatement upsertStatement( //
		String tableName, // Table name to upsert on
		//
		String[] uniqueColumns, // The unique column names
		Object[] uniqueValues, // The row unique identifier values
		//
		String[] insertColumns, // Columns names to update
		Object[] insertValues // Values to update
	) throws JSqlException {
		return upsertStatement(tableName, uniqueColumns, uniqueValues, insertColumns, insertValues,
			null, null, null);
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
	protected JSqlPreparedStatement multiUpsert_statement( //
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
		/// Checks that unique column and values length are not null
		if (uniqueColumns == null || uniqueValuesList == null) {
			throw new JSqlException("Upsert query requires unique columns and values");
		}
		
		// if(uniqueValuesList.size() != insertValuesList.size() && uniqueValuesList.size() != defaultValuesList.size()){
		// 	throw new JSqlException("Upsert query requires unique all values list to be of same size");
		// }
		
		String equalSign = "=";
		String targetTableAlias = "target";
		String sourceTableAlias = "source";
		String statementTerminator = ";";
		
		/// Building the query for INSERT OR REPLACE
		StringBuilder queryBuilder = new StringBuilder();
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		
		// MERGE section
		queryBuilder.append("MERGE INTO `");
		queryBuilder.append(tableName);
		queryBuilder.append("` AS ");
		queryBuilder.append(targetTableAlias);
		queryBuilder.append(" ");
		
		// USING VALUES section
		queryBuilder.append("USING ( VALUES ");
		
		// dynamically append the rows in the VALUES section
		// first create one (?, ?, ?, ?) to reuse
		StringBuilder valuesParameter = new StringBuilder();
		valuesParameter.append("(");
		for (int x = 0; x < uniqueColumns.length; ++x) {
			valuesParameter.append("?,");
		}
		
		for (int x = 0; x < insertColumns.length; ++x) {
			valuesParameter.append("?,");
		}
		
		for (int x = 0; x < defaultColumns.length; ++x) {
			valuesParameter.append("?,");
		}
		valuesParameter.delete(valuesParameter.length() - 1, valuesParameter.length());
		valuesParameter.append(")");
		
		int rows = insertValuesList.size();
		for (int i = 0; i < rows; ++i) {
			queryBuilder.append(valuesParameter);
			if (i < (rows - 1)) {
				queryBuilder.append(",");
			}
		}
		queryBuilder.append(")");
		
		// AS
		queryBuilder.append(" AS ");
		queryBuilder.append(sourceTableAlias);
		queryBuilder.append("(");
		
		StringBuilder sourceCols = new StringBuilder();
		for (int x = 0; x < uniqueColumns.length; ++x) {
			sourceCols.append(uniqueColumns[x]);
			sourceCols.append(",");
		}
		
		for (int x = 0; x < insertColumns.length; ++x) {
			sourceCols.append(insertColumns[x]);
			sourceCols.append(",");
		}
		
		for (int x = 0; x < defaultColumns.length; ++x) {
			sourceCols.append(defaultColumns[x]);
			sourceCols.append(",");
		}
		sourceCols.delete(sourceCols.length() - 1, sourceCols.length());
		
		queryBuilder.append(sourceCols);
		queryBuilder.append(")");
		
		//ON
		queryBuilder.append(" ON (");
		for (int x = 0; x < uniqueColumns.length; ++x) {
			queryBuilder.append(sourceTableAlias);
			queryBuilder.append(".");
			queryBuilder.append(uniqueColumns[x]);
			
			queryBuilder.append("=");
			
			queryBuilder.append(targetTableAlias);
			queryBuilder.append(".");
			queryBuilder.append(uniqueColumns[x]);
			
			if (x < (uniqueColumns.length - 1)) {
				queryBuilder.append(" AND ");
			}
		}
		queryBuilder.append(")");
		
		// WHEN MATCHED THEN
		queryBuilder.append(" WHEN MATCHED THEN UPDATE SET ");
		
		StringBuilder updateCols = new StringBuilder();
		for (int x = 0; x < insertColumns.length; ++x) {
			updateCols.append(targetTableAlias);
			updateCols.append(".");
			updateCols.append(insertColumns[x]);
			
			updateCols.append("=");
			
			updateCols.append(sourceTableAlias);
			updateCols.append(".");
			updateCols.append(insertColumns[x]);
			
			updateCols.append(",");
		}
		for (int x = 0; x < defaultColumns.length; ++x) {
			updateCols.append(targetTableAlias);
			updateCols.append(".");
			updateCols.append(defaultColumns[x]);
			
			updateCols.append("=");
			
			updateCols.append(sourceTableAlias);
			updateCols.append(".");
			updateCols.append(defaultColumns[x]);
			
			updateCols.append(",");
		}
		updateCols.delete(updateCols.length() - 1, updateCols.length());
		
		queryBuilder.append(updateCols);
		
		// WHEN NOT MATCHED THEN INSERT
		queryBuilder.append(" WHEN NOT MATCHED THEN INSERT ");
		queryBuilder.append("(");
		
		StringBuilder insertCols = new StringBuilder();
		for (int x = 0; x < uniqueColumns.length; ++x) {
			insertCols.append(uniqueColumns[x]);
			insertCols.append(",");
		}
		
		for (int x = 0; x < insertColumns.length; ++x) {
			insertCols.append(insertColumns[x]);
			insertCols.append(",");
		}
		
		for (int x = 0; x < defaultColumns.length; ++x) {
			insertCols.append(defaultColumns[x]);
			insertCols.append(",");
		}
		insertCols.delete(insertCols.length() - 1, insertCols.length());
		
		queryBuilder.append(insertCols);
		queryBuilder.append(")");
		
		// VALUES
		queryBuilder.append(" VALUES ");
		queryBuilder.append("(");
		
		StringBuilder valueCols = new StringBuilder();
		for (int x = 0; x < uniqueColumns.length; ++x) {
			valueCols.append(sourceTableAlias);
			valueCols.append(".");
			valueCols.append(uniqueColumns[x]);
			valueCols.append(",");
		}
		
		for (int x = 0; x < insertColumns.length; ++x) {
			valueCols.append(sourceTableAlias);
			valueCols.append(".");
			valueCols.append(insertColumns[x]);
			valueCols.append(",");
		}
		
		for (int x = 0; x < defaultColumns.length; ++x) {
			valueCols.append(sourceTableAlias);
			valueCols.append(".");
			valueCols.append(defaultColumns[x]);
			valueCols.append(",");
		}
		valueCols.delete(valueCols.length() - 1, valueCols.length());
		
		queryBuilder.append(valueCols);
		queryBuilder.append(")");
		
		// ;
		queryBuilder.append(statementTerminator);
		
		// Append the args
		for (int i = 0; i < rows; ++i) {
			if (uniqueValuesList != null) {
				queryArgs.addAll(java.util.Arrays.asList(uniqueValuesList.get(i)));
			}
			
			if (insertValuesList != null) {
				queryArgs.addAll(java.util.Arrays.asList(insertValuesList.get(i)));
			}
			
			if (defaultValuesList != null) {
				queryArgs.addAll(java.util.Arrays.asList(defaultValuesList.get(i)));
			}
		}
		
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
		/// Checks that unique column and values length are not null
		if (uniqueColumns == null || uniqueValuesList == null) {
			throw new JSqlException("Upsert query requires unique columns and values");
		}
		
		// Build the statement, and execute it
		try {
			JSqlPreparedStatement statement = multiUpsert_statement(tableName, uniqueColumns,
				uniqueValuesList, insertColumns, insertValuesList, defaultColumns, defaultValuesList,
				miscColumns);
			return statement.update() >= 1;
		} catch (Exception ex) {
			throw new JSqlException(ex);
		}
	}
}
