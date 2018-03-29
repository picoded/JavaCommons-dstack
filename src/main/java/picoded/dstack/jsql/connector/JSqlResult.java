package picoded.dstack.jsql.connector;

// Java depends
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.IOException;
import java.io.BufferedReader;

// OracleSQL types
import oracle.sql.CLOB;

// Lib depends
import picoded.core.struct.CaseInsensitiveHashMap;
import picoded.core.conv.ArrayConv;

/**
 * JSql result set, data is either prefetched, or fetch on row request. For example usage, refer to JSql
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.java}
 * // Below is the inner data structure format of JSqlResult,
 * // Where its respective fieldname/row can be accessed natively.
 * HashMap<String, Object[]> JSqlResultFormat;
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * *******************************************************************************
 *
 * [LOW PRIROTY TODO LIST]
 * * readRow() : Without the unchecked suppression flag
 * * rowAffected() : used to get the amount of rows affected for an update statment
 * * Exception for readRow / readRowCol on out-of-bound row number
 * * Non silent (exception), and silent varient (returns null) for readRow / readRowCol
 * * isClosed: boolean function for the fetch checking.
 *
 * IGNORED feature list (not fully supported, and dropped to ensure consistancy in sqlite/sql mode)
 * - SingleRow fetch and related features was seen to be buggy in sqlite.
 *   - fetchRowData varient of fetchAllRows : Fetching 1 row from the database at a timeon both ends
 *   - readRow / readRowCol : To support unfetched rows -> ie, automatically fetchs to the desired row count
 *   - isFetchedAll / isFetchedRow
 **/
public class JSqlResult extends CaseInsensitiveHashMap<String, Object[]> {
	
	//-------------------------------------------------------------------------
	//
	// Internal variables and constructor
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Serializable version number (to remove some java nagging warnings)
	 **/
	protected static final long serialVersionUID = 1L;
	
	/**
	 * Internal self used logger
	 **/
	private static final Logger LOGGER = Logger.getLogger(JSqlResult.class.getName());
	
	/**
	 * Total row count for query
	 **/
	private int rowCount = -1;
	
	/**
	 * Total rows affected, this applies for update statements
	 **/
	private int affectedRows = 0;
	
	/**
	 * The prepared statment used for this result
	 **/
	private PreparedStatement sqlStmt = null;
	
	/**
	 * The actual SQL result before parsing
	 **/
	private ResultSet sqlRes = null;
	
	/**
	 * Empty constructor, used as place holder
	 * (Used internally by JSql : not to be used directly)
	 **/
	public JSqlResult() {
		// empty
	}
	
	/**
	 * Constructor with SQL resultSet
	 * (Used internally by JSql : not to be used directly)
	 **/
	public JSqlResult(PreparedStatement ps, ResultSet rs) {
		sqlStmt = ps;
		sqlRes = rs;
	}
	
	/**
	 * Constructor with SQL resultSet
	 * (Used internally by JSql : not to be used directly)
	 **/
	public JSqlResult(PreparedStatement ps, ResultSet rs, int inAffectedRows) {
		sqlStmt = ps;
		sqlRes = rs;
		affectedRows = inAffectedRows;
	}
	
	//-------------------------------------------------------------------------
	//
	// Helper utility functions
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Extracts out the string data from orcale CLOB.
	 * Without any exception, returning null instead when it occurs
	 *
	 * @return  String representing the CLOB
	 **/
	static protected String CLOBtoString(CLOB inData) {
		try {
			return CLOBtoStringNoisy(inData);
		} catch (SQLException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
	}
	
	/**
	 * Extracts out the string data from oracale CLOB.
	 *
	 * @return  String representing the CLOB
	 **/
	static protected String CLOBtoStringNoisy(CLOB inData) throws SQLException, IOException {
		final StringBuilder sb = new StringBuilder();
		final BufferedReader br = new BufferedReader(inData.getCharacterStream());
		String aux = br.readLine();
		if (aux != null) { //in case there is no data
			sb.append(aux);
			while ((aux = br.readLine()) != null) {
				sb.append("\n"); //append new line too
				sb.append(aux);
			}
		}
		br.close();
		return sb.toString();
	}
	
	/**
	 * Extract out the collumn names from an SQL result set
	 *
	 * @param  SQL result set
	 *
	 * @return  String array of filtered out column names
	 **/
	static protected String[] extractColumnNames(ResultSet sqlResultSet) throws SQLException {
		// The SQL result meta data, column count, and return result
		ResultSetMetaData rsmd = sqlResultSet.getMetaData();
		int colCount = rsmd.getColumnCount();
		String[] res = new String[colCount];
		
		// Getting the column names
		for (int i = 0; i < colCount; ++i) {
			// the rsmd, column name function is 1 index-ed
			String colName = rsmd.getColumnName(i + 1);
			
			// remove quotes if it is first and last character.
			if (colName != null && colName.trim().length() > 0) {
				if (colName.charAt(0) == '\'' || colName.charAt(0) == '"') {
					colName = colName.substring(1);
				}
				if (colName.charAt(colName.length() - 1) == '\''
					|| colName.charAt(colName.length() - 1) == '"') {
					colName = colName.substring(0, colName.length() - 1);
				}
			}
			
			// Save the result
			res[i] = colName;
		}
		
		// Return the result
		return res;
	}
	
	/**
	 * Filters the result object, normalizing to their respective java format.
	 *
	 * @param  The data result
	 *
	 * @return The normalized data result
	 **/
	static protected Object filterDataObject(Object tmpObj) throws JSqlException {
		// Normalize BigDecimal as Double
		if (BigDecimal.class.isInstance(tmpObj)) {
			return ((BigDecimal) tmpObj).doubleValue();
		}
		
		// Normalize CLOB as string
		if (CLOB.class.isInstance(tmpObj)) {
			try {
				return CLOBtoStringNoisy((CLOB) tmpObj);
			} catch (SQLException e) {
				throw new JSqlException("CLOB Processing Error", e);
			} catch (IOException e) {
				throw new JSqlException("CLOB Processing Error", e);
			}
		}
		
		// Normalize BLOB as byte array
		if (Blob.class.isInstance(tmpObj)) {
			try {
				Blob bob = (Blob) tmpObj;
				tmpObj = bob.getBytes(1, (int) bob.length());
				bob.free();
				return tmpObj;
			} catch (SQLException e) {
				throw new JSqlException("BLOB Processing Error", e);
			}
		}
		
		// All normalization steps skipped, nothing to do
		return tmpObj;
	}
	
	//-------------------------------------------------------------------------
	//
	// Row fetching
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Return the row count, previously set by `fetchAllRows`
	 **/
	public int rowCount() {
		return rowCount;
	}
	
	/**
	 * Returns the amount of affected rows, applies only for non-select statements
	 **/
	public int affectedRows() {
		return affectedRows;
	}
	
	/**
	 * Fetches all the row data, and store it into the local hashmap & array
	 *
	 * @return rowCount on success, -1 indicate there was no SQLResult to process
	 **/
	public int fetchAllRows() throws JSqlException {
		// If SQL Result is null, it means data been already "fetched"
		// or this class was not initialized properly
		if (sqlRes == null) {
			return rowCount;
		}
		
		try {
			// The column names in the result
			String[] colNames = extractColumnNames(sqlRes);
			int colCount = colNames.length;
			
			// Due to the limitation of some SQL implmentation (like sqlite)
			// being only to process data in a forward fashion. And not be able to
			// get the row count. The system currently iterate across all rows first
			// and stores it into an array list implmentation.
			List<List<Object>> rowResults = new ArrayList<List<Object>>();
			
			// Lets initialize the rowResults with the column arrays
			for (int i = 0; i < colCount; ++i) {
				rowResults.add(new ArrayList<Object>());
			}
			
			// Time to iterate the actual SQL result, and count the rows
			rowCount = 0;
			while (sqlRes.next()) {
				// Iterate the result in a row, column by column
				for (int i = 0; i < colCount; i++) {
					// Get array list to populate, then add in value
					rowResults.get(i).add(
					// Gets the row data (1-indexed) and filters it
						filterDataObject(sqlRes.getObject(i + 1))
					//
						);
				}
				++rowCount;
			}
			
			// Dispose the original sql result set
			close();
			
			// Change format from List to Object array
			for (int i = 0; i < colCount; ++i) {
				// Populate the actual final result
				this.put(
				// Column name
					colNames[i],
					// Actual row result as an object array
					rowResults.get(i).toArray(new Object[rowCount]));
			}
			
			// throw new JSqlException("SQL format is not yet implemented");
			return rowCount;
		} catch (Exception e) {
			throw new JSqlException(e);
		}
	}
	
	/**
	 * Read a fetched row in a single hashmap
	 *
	 * @param  row number to fetch
	 *
	 * @return Map of row data
	 **/
	public CaseInsensitiveHashMap<String, Object> readRow(int pt) {
		// Return null for segments above row count
		if (pt >= rowCount) {
			return null;
		}
		
		// Return result, to populate
		CaseInsensitiveHashMap<String, Object> ret = new CaseInsensitiveHashMap<String, Object>();
		
		// Iterating the local data set, process the result, and return
		for (Map.Entry<String, Object[]> entry : this.entrySet()) {
			ret.put(entry.getKey(), entry.getValue()[pt]);
		}
		return ret;
	}
	
	/**
	 * [Internal use, avoid direct use]
	 *
	 * Returns SQL Table Meta Data info
	 *
	 * this is specifically for an isolated use case of querying SQL information
	 * as a single map result. This can only be called once.
	 *
	 * @return  Map of table meta information
	 **/
	public Map<String, String> fetchMetaData() throws JSqlException {
		Map<String, String> ret = null;
		if (sqlRes != null) {
			ret = new HashMap<String, String>();
			try {
				while (sqlRes.next()) {
					ret.put(sqlRes.getString("COLUMN_NAME").toUpperCase(Locale.ENGLISH), sqlRes
						.getString("TYPE_NAME").toUpperCase(Locale.ENGLISH));
				}
			} catch (Exception e) {
				throw new JSqlException("Error fetching sql meta data", e);
			}
		}
		return ret;
	}
	
	//-------------------------------------------------------------------------
	//
	// Cleanup handling
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Dispose and closes the result connection
	 *
	 * Note if you call this prior to fetchAllRows, data loss may occur.
	 **/
	public void close() {
		try {
			if (sqlRes != null) {
				sqlRes.close();
				sqlRes = null;
			}
		} catch (Exception e) {
			// Log the exception as warning
			LOGGER.log(Level.WARNING, "JSqlResult.close result exception", e);
		}
		
		try {
			if (sqlStmt != null) {
				sqlStmt.close();
				sqlStmt = null;
			}
		} catch (Exception e) {
			// Log the exception as warning
			LOGGER.log(Level.WARNING, "JSqlResult.close statement exception", e);
		}
	}
	
	//-------------------------------------------------------------------------
	//
	// Fine tuning equality checks
	//
	//-------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object o) {
		return this == o;
	}
	
	@Override
	public int hashCode() {
		return rowCount;
	}
	
}
