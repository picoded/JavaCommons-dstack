package picoded.dstack.connector.jsql;

import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.sql.Connection;

import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.GenericConvertHashMap;
import picoded.core.struct.GenericConvertList;

import picoded.dstack.connector.jsql.JSql;
import picoded.dstack.connector.jsql.JSqlException;
// import picoded.JSql.JSqlQuerySet;
import picoded.dstack.connector.jsql.JSqlResult;
import picoded.dstack.connector.jsql.JSqlType;
import picoded.core.struct.MutablePair;

import oracle.jdbc.pool.OracleDataSource;

/// Pure ORACLE-SQL implentation of JSql
public class JSql_Oracle extends JSql_Base {
	
	/// Internal self used logger
	private static Logger logger = Logger.getLogger(JSql_Oracle.class.getName());
	
	///
	private String oracleTablespace = null;
	
	/// Runs JSql with the JDBC ORACLE SQL engine
	///
	/// **Note:** urlString, is just IP:PORT. For example, "127.0.0.1:3306"
	public JSql_Oracle(String oraclePath, String dbUser, String dbPass) {
		// store database connection properties
		// setConnectionProperties(oraclePath, null, dbUser, dbPass, null);
		
		// // call internal method to create the connection
		// setupConnection();
		
		GenericConvertMap<String, Object> config = new GenericConvertHashMap<>();
		
		// Basic path, dbname, user, pass configuration
		config.put("host", oraclePath);
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
	public JSql_Oracle(GenericConvertMap<String, Object> config) {
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
		sqlType = JSqlType.ORACLE;

		// !!! Unfortunately due to known stability issues with oracleDB and hikariCP
		//     the connection pool is now drop in favour of a more stable direct connection
		//     even if its at the cost of overall performance

		datasource = HikaricpUtil.oracle(config);

		// datasource = new OracleDataSource();
	}
	
	// public JSql_Oracle(java.sql.Connection inSqlConn) {
	// 	if (inSqlConn != null) {
	// 		sqlConn = inSqlConn;
	// 	}
	// }
	
	/// Collumn type correction from mysql to oracle sql
	private static String _simpleMysqlToOracle_collumnSubstitude(String qString) {
		return qString
			.replaceAll("(?i)BIT", "RAW")
			.replaceAll("(?i)TINYBLOB", "RAW")
			//, RAW
			//.replaceAll("(?i)CHAR","CHAR")
			.replaceAll("(?i)DECIMAL", "NUMBER")
			.replaceAll("(?i)DOUBLE", "FLOAT(24)")
			.replaceAll("(?i)DOUBLE PRECISION", "FLOAT(24)")
			.replaceAll("(?i)REAL", "FLOAT (24)")
			//.replaceAll("(?i)FLOAT","FLOAT")
			.replaceAll("(?i)INTEGER", "INT")
			//.replaceAll("(?i)INT","NUMBER(10,0)")
			.replaceAll("(?i)BIGINT", "NUMBER(19, 0)").replaceAll("(?i)MEDIUMINT", "NUMBER(7,0)")
			.replaceAll("(?i)SMALLINT", "NUMBER(5,0)").replaceAll("(?i)TINYINT", "NUMBER(3,0)")
			.replaceAll("(?i)YEAR", "NUMBER").replaceAll("(?i)NUMERIC", "NUMBER")
			.replaceAll("(?i)BLOB", "BLOB").replaceAll("(?i)LONGBLOB", "BLOB")
			.replaceAll("(?i)MEDIUMBLOB", "BLOB").replaceAll("(?i)LONGTEXT", "CLOB")
			.replaceAll("(?i)MEDIUMTEXT", "CLOB")
			.replaceAll("(?i)TEXT", "VARCHAR")
			//.replaceAll("(?i)DATE","DATE")
			.replaceAll("(?i)TIME", "DATE").replaceAll("(?i)TIMESTAMP", "DATE")
			.replaceAll("(?i)DATETIME", "DATE")
			/*
			.replaceAll("(?i)ENUM(?=\\()","VARCHAR2")
			.replaceAll("(?i)ENUM(?!\\()","VARCHAR2(n)")
			.replaceAll("(?i)SET(?=\\()","VARCHAR2")
			.replaceAll("(?i)SET(?!\\()","VARCHAR2(n)")
			.replaceAll("(?i)TINYTEXT(?=\\()","VARCHAR2")
			.replaceAll("(?i)TINYTEXT(?!\\()","VARCHAR2(n)")
			 */
			.replaceAll("(?i)VARCHAR(?!\\()", "VARCHAR2(4000)")
			.replaceAll("(?i)VARCHAR\\(", "VARCHAR2(").replaceAll("MAX", "4000");
	}
	
	/// Fixes the table name, and removes any trailing ";" if needed
	private static String _fixTableNameInOracleSubQuery(String qString) {
		qString = qString.trim();
		int indxPt = ((indxPt = qString.indexOf(' ')) <= -1) ? qString.length() : indxPt;
		String tableStr = qString.substring(0, indxPt).toUpperCase();
		
		/*
		if( !tableStr.substring(0,1).equals("\"") ) {
			tableStr = "\"" + tableStr;
			if( !tableStr.substring(tableStr.length()-1).equals("\"") ) {
				tableStr = tableStr+"\"";
			}
		}
		 */
		qString = tableStr + qString.substring(indxPt);
		
		while (qString.endsWith(";")) { //Remove uneeded trailing ";" semi collons
			qString = qString.substring(0, qString.length() - 1);
		}
		return qString;
	}
	
	final String ifExists = "IF EXISTS";
	final String ifNotExists = "IF NOT EXISTS";
	
	final String create = "CREATE";
	final String drop = "DROP";
	final String view = "VIEW";
	final String table = "TABLE";
	final String select = "SELECT";
	final String update = "UPDATE";
	
	final String insertInto = "INSERT INTO";
	final String deleteFrom = "DELETE FROM";
	
	final String[] indexTypeArr = { "UNIQUE", "FULLTEXT", "SPATIAL" };
	final String index = "INDEX";
	
	/// Internal parser that converts some of the common sql statements to sqlite
	public String genericSqlParser(String inString) throws JSqlException {
		//Unique to oracle prefix, automatically terminates all additional conversion attempts
		final String oracleImmediateExecute = "BEGIN EXECUTE IMMEDIATE";
		if (inString.startsWith(oracleImmediateExecute)) {
			return inString;
		}
		
		String fixedQuotes = inString.trim().replaceAll("(\\s){1}", " ").replaceAll("\\s+", " ")
			.replaceAll("'", "\"").replaceAll("`", "\""); //.replaceAll("\"", "'");
		
		String upperCaseStr = fixedQuotes.toUpperCase();
		String qString = fixedQuotes;
		String qStringPrefix = "";
		String qStringSuffix = "";
		
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
					qStringPrefix = "BEGIN EXECUTE IMMEDIATE 'DROP TABLE ";
					qStringSuffix = "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
				} else {
					qStringPrefix = "DROP TABLE ";
				}
				qString = _fixTableNameInOracleSubQuery(fixedQuotes.substring(prefixOffset));
			} else if (upperCaseStr.startsWith(view, prefixOffset)) { //VIEW
				prefixOffset += view.length() + 1;
				if (upperCaseStr.startsWith(ifExists, prefixOffset)) { //IF EXISTS
					prefixOffset += ifExists.length() + 1;
					qStringPrefix = "BEGIN EXECUTE IMMEDIATE 'DROP VIEW ";
					qStringSuffix = "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
				} else {
					qStringPrefix = "DROP VIEW ";
				}
				qString = _fixTableNameInOracleSubQuery(fixedQuotes.substring(prefixOffset));
			}
			
		} else if (upperCaseStr.startsWith(create)) { //CREATE
			prefixOffset = create.length() + 1;
			
			if (upperCaseStr.startsWith(table, prefixOffset)) { //TABLE
				prefixOffset += table.length() + 1;
				if (upperCaseStr.startsWith(ifNotExists, prefixOffset)) { //IF NOT EXISTS
					prefixOffset += ifNotExists.length() + 1;
					qStringPrefix = "BEGIN EXECUTE IMMEDIATE 'CREATE TABLE ";
					qStringSuffix = "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;";
				} else {
					qStringPrefix = "CREATE TABLE ";
				}
				qString = _fixTableNameInOracleSubQuery(fixedQuotes.substring(prefixOffset));
				qString = _simpleMysqlToOracle_collumnSubstitude(qString);
			} else if (upperCaseStr.startsWith(view, prefixOffset)) { //VIEW
				prefixOffset += view.length() + 1;
				if (upperCaseStr.startsWith(ifNotExists, prefixOffset)) { //IF NOT EXISTS
					prefixOffset += ifNotExists.length() + 1;
					qStringPrefix = "BEGIN EXECUTE IMMEDIATE 'CREATE VIEW ";
					qStringSuffix = "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;";
				} else {
					qStringPrefix = "CREATE VIEW ";
				}
				qString = _fixTableNameInOracleSubQuery(fixedQuotes.substring(prefixOffset));
				qString = _simpleMysqlToOracle_collumnSubstitude(qString);
				
				int fromKeywordIndex = qString.indexOf("FROM") + "FROM".length();
				String qStringBeforeFromKeyword = qString.substring(0, fromKeywordIndex);
				// remove 'AS' keywords after table name
				String qStringAfterFromKeyword = qString.substring(fromKeywordIndex, qString.length())
					.replaceAll("AS", "");
				// replace double quotes (") with single quotes
				qStringAfterFromKeyword = qStringAfterFromKeyword.replace("\"", "'");
				
				qString = qStringBeforeFromKeyword + qStringAfterFromKeyword;
				
			} else {
				logger.finer("Trying to matched INDEX : " + upperCaseStr.substring(prefixOffset));
				if (createIndexType.matcher(upperCaseStr.substring(prefixOffset)).matches()) { //UNIQUE|FULLTEXT|SPATIAL|_ INDEX
					logger.finer("Matched INDEX : " + inString);
					
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
							qStringPrefix = "BEGIN EXECUTE IMMEDIATE '";
							qStringSuffix = "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;";
						}
						
						tmpStr = _fixTableNameInOracleSubQuery(fixedQuotes.substring(prefixOffset));
						tmpIndx = tmpStr.indexOf(" ON ");
						
						String tableAndColumns = tmpStr.substring(tmpIndx + " ON ".length());
						// check column's type
						String metaDataQuery = "SELECT "
							+ tableAndColumns.substring(tableAndColumns.indexOf("(") + 1,
								tableAndColumns.indexOf(")")) + " FROM "
							+ tableAndColumns.substring(0, tableAndColumns.indexOf("("));
						Map<String, String> metadata = null;
						// try {
						// 	metadata = getMetaData(metaDataQuery);
						// } catch (JSqlException e) {
						// 	//throw e;
						// }
						
						// if (metadata != null && !metadata.isEmpty()) {
						// 	for (Map.Entry<String, String> entry : metadata.entrySet()) {
						// 		//System.out.println(entry.getKey() + "/" + entry.getValue());
						// 	}
						// 	for (Map.Entry<String, String> entry : metadata.entrySet()) {
						// 		if (entry.getValue() != null
						// 			&& entry.getValue().trim().toUpperCase().contains("LOB")) {
						// 			throw new JSqlException(
						// 				"Cannot create index on expression with datatype LOB for field '"
						// 					+ entry.getKey() + "'.");
						// 		}
						// 	}
						// }
						
						if (tmpIndx > 0) {
							qString = "CREATE " + ((indexType != null) ? indexType + " " : "") + "INDEX "
								+ tmpStr.substring(0, tmpIndx) + " ON "
								+ _fixTableNameInOracleSubQuery(tmpStr.substring(tmpIndx + 4));
						}
						// check if column type is blob
						
					}
				}
			}
		} else if (upperCaseStr.startsWith(insertInto)) { //INSERT INTO
			prefixOffset = insertInto.length() + 1;
			
			tmpStr = _fixTableNameInOracleSubQuery(fixedQuotes.substring(prefixOffset));
			
			//-- Not fully supported
			//tmpIndx = tmpStr.indexOf(" ON DUPLICATE KEY UPDATE ");
			//if(tmpIndx > 0) {
			//	qStringPrefix = "BEGIN EXECUTE IMMEDIATE '";
			//	qString = tmpStr.substring(0, tmpIndx);
			//	qStringSuffix = "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;";
			//}
			
			qString = "INSERT INTO " + tmpStr;
		} else if (upperCaseStr.startsWith(select)) { //SELECT
			prefixOffset = select.length() + 1;
			
			tmpStr = qString.substring(prefixOffset);
			tmpIndx = qString.toUpperCase().indexOf(" FROM ");
			
			if (tmpIndx > 0) {
				qString = "SELECT " + tmpStr.substring(0, tmpIndx - 7)
				//.replaceAll("\"", "'")
					.replaceAll("`", "\"") + " FROM "
					+ _fixTableNameInOracleSubQuery(tmpStr.substring(tmpIndx - 1));
			} else {
				qString = _fixTableNameInOracleSubQuery(fixedQuotes);
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
			// Remove 'AS' from table alias
			// qString = removeAsAfterTablename(qString);
			qString = removeASbeforeTableAlias(qString);
			qString = removeAsAfterOpeningBracket(qString);
			
			// Fix the pagination query as per the Oracle 12C
			// The Oracle 12C supports the pagination query with the OFFSET/FETCH keywords
			
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
			
			// Fix the pagination using the ROWNUM which is supported by versions below than Oracle 12C
			// String prefixQuery = null;
			// int startRowNum = -1;
			// int endRowNum = -1;
			
			// int limitIndex = qString.indexOf("LIMIT");
			// if (limitIndex != -1) {
			// 	prefixQuery = qString.substring(0, limitIndex);
			// }
			
			// if (prefixQuery != null) {
			// 	limitIndex += "LIMIT".length();
			// 	int offsetIndex = qString.indexOf("OFFSET");
			// 	if (offsetIndex != -1) {
			// 		startRowNum = Integer.parseInt(qString.substring(offsetIndex + "OFFSET".length())
			// 			.trim());
			// 		endRowNum = Integer.parseInt(qString.substring(limitIndex, offsetIndex).trim())
			// 			+ startRowNum;
			// 	}
			// }
			// if (startRowNum != -1 && endRowNum != -1) {
			// 	qString = "SELECT * FROM (SELECT a.*, rownum AS rnum FROM (" + prefixQuery
			// 		+ ") a WHERE rownum <= " + endRowNum + ") WHERE rnum > " + startRowNum;
			// }
			
		} else if (upperCaseStr.startsWith(deleteFrom)) {
			prefixOffset = deleteFrom.length() + 1;
			
			tmpStr = _fixTableNameInOracleSubQuery(qString.substring(prefixOffset));
			qString = deleteFrom + " " + tmpStr;
			
		} else if (upperCaseStr.startsWith(update)) { //UPDATE
			prefixOffset = update.length() + 1;
			
			tmpStr = _fixTableNameInOracleSubQuery(qString.substring(prefixOffset));
			qString = update + " " + tmpStr;
		}
		
		qString = qStringPrefix + qString + qStringSuffix;
		
		//logger.finer("Converting MySQL query to oracleSQL query");
		//logger.finer("MySql -> "+inString);
		//logger.finer("OracleSql -> "+qString);
		
		//logger.warning("MySql -> "+inString);
		//logger.warning("OracleSql -> "+qString);
		
		return qString; //no change of data
	}
	
	/// Executes the table meta data query, and returns the result object
	public Map<String, String> getMetaData(String sql) throws JSqlException {
		Map<String, String> metaData = null;
		ResultSet rs = null;
		//Try and finally : prevent memory leaks
		try {
			
			Connection sqlConn = datasource.getConnection();
			Statement st = sqlConn.createStatement();
			st = sqlConn.createStatement();
			rs = st.executeQuery(sql);
			ResultSetMetaData rsMetaData = rs.getMetaData();
			int numberOfColumns = rsMetaData.getColumnCount();
			for (int i = 1; i <= numberOfColumns; i++) {
				if (metaData == null) {
					metaData = new HashMap<String, String>();
				}
				metaData.put(rsMetaData.getColumnName(i), rsMetaData.getColumnTypeName(i));
			}
		} catch (Exception e) {
			throw new JSqlException("executeQuery_metadata exception", e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					//donothing
				}
				rs = null;
			}
		}
		return metaData;
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
	 * @return  Pair containing < collumn_name, collumn_type >
	 **/
	protected MutablePair<GenericConvertList<Object>, GenericConvertList<Object>> getTableColumnTypeMap_core(
		String tablename) {
		// Get the column information
		JSqlResult tableInfo = query_raw(
			"SELECT column_name, data_type FROM USER_TAB_COLUMNS WHERE table_name=?",
			new Object[] { tablename });
		
		// And return it as a list pair
		return new MutablePair<>(tableInfo.get("column_name"), tableInfo.get("data_type"));
	}
	
	private static String removeASbeforeTableAlias(String qString) {
		return qString.replace("AS ", "");
	}
	
	private static String removeAsAfterTablename(String qString) {
		int prefixOffset = 0;
		String tmpStr = null;
		
		// parse table name
		String searchString = "FROM";
		String tablename = "";
		while ((prefixOffset = qString.indexOf(searchString, prefixOffset)) > 0) {
			prefixOffset += searchString.length();
			tmpStr = qString.substring(prefixOffset, qString.length()).trim();
			if (!tmpStr.startsWith("(")) {
				// parse table name
				for (int i = 0; i < tmpStr.length(); i++) {
					if (tmpStr.charAt(i) == ' ' || tmpStr.charAt(i) == ')') {
						break;
					}
					tablename += tmpStr.charAt(i);
				}
				tablename = tablename.replaceAll("\"", "").trim();
				
			}
		}
		
		// Fix the "AS" quotation
		searchString = " " + tablename + " ";
		while ((prefixOffset = qString.indexOf(searchString, prefixOffset)) > 0) {
			prefixOffset += searchString.length();
			tmpStr = qString.substring(prefixOffset, qString.length()).trim();
			if (tmpStr.startsWith("AS")) {
				qString = qString.substring(0, prefixOffset)
					+ qString.substring(prefixOffset + "AS".length() + 1, qString.length()).trim();
			}
		}
		
		return qString;
	}
	
	private static String removeAsAfterOpeningBracket(String qString) {
		String searchString = " FROM (";
		int prefixOffset = qString.indexOf(searchString);
		if (prefixOffset != -1) {
			prefixOffset += searchString.length();
			int offsetIndex = prefixOffset;
			int obc = 1;
			int cbc = 0;
			// find the closing index
			for (; offsetIndex < qString.length(); offsetIndex++) {
				if (qString.charAt(offsetIndex) == ')') {
					cbc++;
					if (obc == cbc) {
						break;
					}
				} else if (qString.charAt(offsetIndex) == '(') {
					obc++;
				}
			}
			String strBetweenBracket = qString.substring(prefixOffset, offsetIndex).trim();
			// Remove AS
			// increment for space before bracket
			offsetIndex++;
			String tmpStr = qString.substring(offsetIndex, qString.length()).trim();
			if (tmpStr.startsWith("AS")) {
				// increment for space before bracket
				offsetIndex++;
				qString = qString.substring(0, offsetIndex)
					+ qString.substring(offsetIndex + "AS ".length(), qString.length()).trim();
				offsetIndex = offsetIndex - "AS".length();
			}
			// make recursive call
			if (strBetweenBracket.indexOf(searchString) != -1) {
				qString = qString.substring(0, prefixOffset)
					+ removeAsAfterOpeningBracket(strBetweenBracket)
					+ qString.substring(offsetIndex, qString.length());
			}
		}
		return qString;
	}
	
	/// Executes the argumented query, and returns the result object *without*
	/// fetching the result data from the database. (not fetching may not apply to all implementations)
	///
	/// **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	public JSqlResult executeQuery(String qString, Object... values) throws JSqlException {
		//try {
		return query_raw(genericSqlParser(qString), values);
		//} catch(JSqlException e) {
		//	logger.log( Level.SEVERE, "ExecuteQuery Exception" ); //, e
		//	logger.log( Level.SEVERE, "-> Original query : " + qString );
		//	logger.log( Level.SEVERE, "-> Parsed query   : " + genericSqlParser(qString) );
		//	throw e;
		//}
	}
	
	/// Executes the argumented query, and immediately fetches the result from
	/// the database into the result set.
	///
	/// **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	public JSqlResult query(String qString, Object... values) throws JSqlException {
		//try {
		return query_raw(genericSqlParser(qString), values);
		//} catch(JSqlException e) {
		//	logger.log( Level.SEVERE, "Query Exception" ); //, e
		//	logger.log( Level.SEVERE, "-> Original query : " + qString );
		//	logger.log( Level.SEVERE, "-> Parsed query   : " + genericSqlParser(qString) );
		//	throw e;
		//}
	}
	
	/// Executes and dispose the sqliteResult object.
	///
	/// Returns false if no result is given by the execution call, else true on success
	public JSqlResult execute(String qString, Object... values) throws JSqlException {
		qString = genericSqlParser(qString);
		
		//
		//  IMPORTANT NOTE
		//
		//  As of oracle 12c, AUTOINCREMENT support is implemented with "IDENTITY" syntax
		//  http://www.oracletutorial.com/oracle-basics/oracle-identity-column/
		//
		//  As such the sequence / trigger clause is commented out / deprecated
		//
		
		// String sequenceQuery = null;
		// String triggerQuery = null;
		
		// check if there is any AUTO INCREMENT field
		if (qString.indexOf("AUTOINCREMENT") != -1 || qString.indexOf("AUTO_INCREMENT") != -1) {
			
			// Create sequence and trigger if it is CREATE TABLE query and has the AUTO INCREMENT column
			int prefixOffset = qString.indexOf(create);
			// check if create statement
			if (prefixOffset != -1) { //CREATE
			
				prefixOffset += create.length() + 1;
				
				// check if create table statement
				if (qString.startsWith(table, prefixOffset)) { //TABLE
				
					prefixOffset += table.length() + 1;
					
					// check if 'IF NOT EXISTS' exists in query
					if (qString.startsWith(ifNotExists, prefixOffset)) {
						prefixOffset += ifNotExists.length() + 1;
					}
					
					// parse table name
					String tableName = qString.substring(prefixOffset,
						qString.indexOf("(", prefixOffset));
					tableName = tableName.replaceAll("\"", "").trim();
					
					prefixOffset += tableName.length();
					//prefixOffset = qString.indexOf("(", prefixOffset) + 1;
					
					// parse primary key column
					String primaryKeyColumn = "";
					
					String tmpStr = qString.substring(prefixOffset, qString.indexOf("PRIMARY KEY"))
						.trim();
					
					if (tmpStr.charAt(tmpStr.length() - 1) == ')') {
						tmpStr = tmpStr.substring(0, tmpStr.lastIndexOf("(")).trim();
					}
					// find last space
					tmpStr = tmpStr.substring(0, tmpStr.lastIndexOf(" ")).trim();
					
					for (int i = tmpStr.length() - 1; i >= 0; i--) {
						// find space, comma or opening bracket
						if (tmpStr.charAt(i) == ' ' || tmpStr.charAt(i) == ',' || tmpStr.charAt(i) == '(') {
							break;
						}
						primaryKeyColumn = tmpStr.charAt(i) + primaryKeyColumn;
					}
					
					// // create sequence sql query
					// sequenceQuery = "CREATE SEQUENCE \"" + tableName
					// 	+ "_SEQ\" START WITH 1001 INCREMENT BY 1 CACHE 10";
					
					// // create trigger sql query
					// triggerQuery = "CREATE OR REPLACE TRIGGER \"" + tableName + "_TRIGGER\" "
					// 	+ " BEFORE INSERT ON \"" + tableName + "\" FOR EACH ROW " + " BEGIN SELECT \""
					// 	+ tableName + "_SEQ\".nextval INTO :NEW." + primaryKeyColumn + " FROM dual; END;";
				}
			}
		}
		
		// BIGINT PRIMARY KEY AUTOINCREMENT
		qString = qString.replaceAll("BIGINT PRIMARY KEY AUTOINCREMENT",
			"GENERATED BY DEFAULT ON NULL AS IDENTITY");
		qString = qString.replaceAll("PRIMARY KEY AUTOINCREMENT", "GENERATED BY DEFAULT ON NULL");
		qString = qString.replaceAll("PRIMARY KEY AUTO_INCREMENT", "GENERATED BY DEFAULT ON NULL");
		qString = qString.replaceAll("AUTOINCREMENT", "GENERATED BY DEFAULT ON NULL");
		qString = qString.replaceAll("AUTO_INCREMENT", "GENERATED BY DEFAULT ON NULL");
		
		// // Replace the AUTO INCREMENT with blank
		// qString = qString.replaceAll("AUTOINCREMENT", "");
		// qString = qString.replaceAll("AUTO_INCREMENT", "");
		
		//try {
		JSqlResult retvalue = query_raw(qString, values);
		
		// // Create Sequence
		// if (sequenceQuery != null) {
		// 	query_raw(genericSqlParser(sequenceQuery));
		// }
		// //Create trigger
		// if (triggerQuery != null) {
		// 	query_raw(genericSqlParser(triggerQuery));
		// }
		
		return retvalue;
		//} catch(JSqlException e) {
		//	logger.log( Level.SEVERE, "Execute Exception" ); //, e
		//	logger.log( Level.SEVERE, "-> Original query : " + qString );
		//	logger.log( Level.SEVERE, "-> Parsed query   : " + genericSqlParser(qString) );
		//	throw e;
		//}
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
		// Perform the original update call
		//System.out.println("-----------------------------------");
		//System.out.println(qString);
		if (values.length == 0)
			//System.out.println("Query args is empty");
			
			for (Object val : values) {
				if (val != null) {
					//System.out.println(" | " + val.toString() + " | ");
				} else {
					//System.out.println(" | val is null | ");
				}
				
			}
		
		int res = super.update(qString, values);
		//System.out.println("RES : " + res);
		
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
	
	///
	/// NOTE: This assumes Oracle 11g onwards
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
	/// INTO destTable
	/// USING (SELECT
	///		? id,
	///		? name,
	///		from dual
	/// ) sourceTable
	/// ON (destTable.id = sourceTable.id)
	/// WHEN NOT MATCHED THEN
	/// INSERT (id, name, role, note) VALUES (
	///		sourceTable.id,   // Unique value
	/// 	sourceTable.name, // Insert value
	///		sourceTable.role, // Values with default
	///		sourceTable.note  // Misc values to preserve
	/// )
	/// WHEN MATCHED THEN
	/// UPDATE
	///		destTable.role = ?, 
	///		// destTable.note = sourceTable.note // Default value
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
		//
		String[] miscColumns //
	) throws JSqlException {
		
		if (tableName.length() > 30) {
			logger.warning(JSqlException.oracleNameSpaceWarning + tableName);
		}
		
		/// Checks that unique collumn and values length to be aligned
		if (uniqueColumns == null || uniqueValues == null
			|| uniqueColumns.length != uniqueValues.length) {
			throw new JSqlException(
				"Upsert query requires unique column and values to be equal length");
		}
		
		/// Table aliasing names
		String targetTableAlias = "destTable";
		String sourceTableAlias = "srcTable";
		
		/// Final actual query set
		StringBuilder queryBuilder = new StringBuilder();
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		
		tableName = tableName.toUpperCase();
		
		/// The actual query building
		queryBuilder.append("MERGE INTO " + tableName + " " + targetTableAlias + " USING ( SELECT ");
		
		/// The fields to select to search for unique
		for (int a = 0; a < uniqueColumns.length; ++a) {
			if (a > 0) {
				queryBuilder.append(", ");
			}
			queryBuilder.append("? ");
			queryBuilder.append(uniqueColumns[a]);
			queryArgs.add(uniqueValues[a]);
			
			if (uniqueValues[a] != null) {
				//System.out.println("1. UNIQUEVALUES[" + a + "] : " + uniqueValues[a].toString());
			} else {
				//System.out.println("1. UNIQUEVALUES[" + a + "] is null");
			}
			
		}
		
		/// From dual
		queryBuilder.append(" FROM DUAL ) " + sourceTableAlias);
		
		/// On unique keys
		queryBuilder.append(" ON ( ");
		for (int a = 0; a < uniqueColumns.length; ++a) {
			if (a > 0) {
				queryBuilder.append(" and ");
			}
			queryBuilder.append(targetTableAlias + "." + uniqueColumns[a]);
			queryBuilder.append(" = ");
			queryBuilder.append(sourceTableAlias + "." + uniqueColumns[a]);
		}
		queryBuilder.append(" ) ");
		
		// Has insert collumns and values
		if (insertColumns != null && insertColumns.length > 0) {
			
			if (insertColumns.length != insertValues.length) {
				throw new JSqlException(
					"Upsert query requires insert column and values to be equal length");
			}
			
			// Found it, do an insert
			queryBuilder.append(" WHEN MATCHED THEN UPDATE SET ");
			
			// For insert keys
			for (int a = 0; a < insertColumns.length; ++a) {
				if (a > 0) {
					queryBuilder.append(", ");
				}
				queryBuilder.append(targetTableAlias);
				queryBuilder.append(".");
				queryBuilder.append(insertColumns[a]);
				queryBuilder.append(" = ? ");
				
				queryArgs.add(insertValues[a]);
				
				if (insertValues[a] != null) {
					//System.out.println("2. INSERTVALUES[" + a + "] : " + insertValues[a].toString());
				} else {
					//System.out.println("2. INSERTVALUES[" + a + "] is null ");
				}
				
			}
		}
		
		// Found it, do an insert
		queryBuilder.append(" WHEN NOT MATCHED THEN INSERT ( ");
		
		// Insert query building
		StringBuilder insertNameString = new StringBuilder();
		StringBuilder insertValuesString = new StringBuilder();
		
		// Insert UNIQUE collumns
		for (int a = 0; a < uniqueColumns.length; ++a) {
			if (a > 0) {
				insertNameString.append(", ");
				insertValuesString.append(", ");
			}
			insertNameString.append(uniqueColumns[a]);
			insertValuesString.append("?");
			queryArgs.add(uniqueValues[a]);
			
			if (uniqueValues[a] != null) {
				//System.out.println("3. UNIQUEVALUES[" + a + "] : " + uniqueValues[a].toString());
			} else {
				//System.out.println("3. UNIQUEVALUES[" + a + "] : is null");
			}
			
		}
		
		// Insert INSERT collumns
		if (insertColumns != null && insertColumns.length > 0) {
			for (int a = 0; a < insertColumns.length; ++a) {
				insertNameString.append(", ");
				insertValuesString.append(", ");
				
				insertNameString.append(insertColumns[a]);
				insertValuesString.append("?");
				queryArgs.add(insertValues[a]);
			}
		}
		
		// Insert DEFAULT collumns
		if (defaultColumns != null && defaultColumns.length > 0) {
			for (int a = 0; a < defaultColumns.length; ++a) {
				insertNameString.append(", ");
				insertValuesString.append(", ");
				
				insertNameString.append(defaultColumns[a]);
				insertValuesString.append("?");
				queryArgs.add(defaultValues[a]);
			}
		}
		
		// Build the actual insert
		queryBuilder.append(insertNameString);
		queryBuilder.append(" ) VALUES ( ");
		queryBuilder.append(insertValuesString);
		queryBuilder.append(" )");
		
		// The actual query
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
	
}
