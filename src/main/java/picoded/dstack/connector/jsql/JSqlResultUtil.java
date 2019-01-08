package picoded.dstack.connector.jsql;

// Java depends
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.GenericConvertArrayList;

/**
 * Various internal utility functions used by JSqlResult processing
 * 
 * This is intentionally **not** a public class
 */
class JSqlResultUtil {
	
	/**
	 * Extracts out the string data from orcale CLOB.
	 * Without any exception, returning null instead when it occurs
	 *
	 * @param CLOB object to convert
	 * 
	 * @return  String representing the CLOB
	 **/
	static String CLOBtoString(CLOB inData) {
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
	 * @param CLOB object to convert
	 * 
	 * @return  String representing the CLOB
	 **/
	static String CLOBtoStringNoisy(CLOB inData) throws SQLException, IOException {
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
	 * @param  sqlResultSet to iterate
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
	 * @param  dataObject to process
	 *
	 * @return The normalized data result
	 **/
	static protected Object normalizeDataObject(Object dataObj) throws JSqlException {
		// Normalize BigDecimal as Double
		if (BigDecimal.class.isInstance(dataObj)) {
			return ((BigDecimal) dataObj).doubleValue();
		}
		
		// Normalize CLOB as string
		if (CLOB.class.isInstance(dataObj)) {
			try {
				return CLOBtoStringNoisy((CLOB) dataObj);
			} catch (SQLException e) {
				throw new JSqlException("CLOB Processing Error", e);
			} catch (IOException e) {
				throw new JSqlException("CLOB Processing Error", e);
			}
		}
		
		// Normalize BLOB as byte array
		if (Blob.class.isInstance(dataObj)) {
			try {
				Blob bob = (Blob) dataObj;
				dataObj = bob.getBytes(1, (int) bob.length());
				bob.free();
				return dataObj;
			} catch (SQLException e) {
				throw new JSqlException("BLOB Processing Error", e);
			}
		}
		
		// All normalization steps skipped, nothing to do
		return dataObj;
	}
	
	/**
	 * Merge the 2 arrays together
	 * Used to join arguments together
	 *
	 * @param  Array of arguments 1
	 * @param  Array of arguments 2
	 *
	 * @return  Resulting array of arguments 1 & 2
	 **/
	static protected Object[] joinArguments(Object[] arr1, Object[] arr2) {
		return org.apache.commons.lang3.ArrayUtils.addAll(arr1, arr2);
	}
}