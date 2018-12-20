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
 * JSql result set, where data is fetched from an SQL result set, into a map<list> data structure.
 *
 * *******************************************************************************
 *
 * [LOW PRIROTY TODO LIST]
 * * Exception varient for readRow / readRowCol on out-of-bound row number
 * * isClosed: boolean function for the fetch checking.
 *
 * IGNORED feature list (not fully supported, and dropped to ensure consistancy in sqlite/sql mode)
 * - SingleRow fetch and related features was seen to be buggy in sqlite.
 *   - fetchRowData varient of fetchAllRows : Fetching 1 row from the database at a timeon both ends
 *   - readRow / readRowCol : To support unfetched rows -> ie, automatically fetchs to the desired row count
 *   - isFetchedAll / isFetchedRow
 **/
public class JSqlResult extends CaseInsensitiveHashMap<String, GenericConvertList<Object>> {
	
	//-------------------------------------------------------------------------
	//
	// Logger and class serial version
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
	
	//-------------------------------------------------------------------------
	//
	// Constructor
	//
	//-------------------------------------------------------------------------
	
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
	public JSqlResult(ResultSet rs) {
		fetchAllRows(rs);
	}
	
	/**
	 * Constructor with SQL resultSet
	 * (Used internally by JSql : not to be used directly)
	 *
	 * @param rs result set to extract data from
	 * @
	 **/
	public JSqlResult(ResultSet rs, int inRowAffected) {
		rowAffected = inRowAffected;
		fetchAllRows(rs);
	}
	
	//-------------------------------------------------------------------------
	//
	// Internal variables
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Total rows affected, this applies for update statements
	 **/
	private int rowAffected = 0;
	
	/**
	 * Total row count for query
	 **/
	private int rowCount = -1;
	
	//-------------------------------------------------------------------------
	//
	// Row fetching
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Fetches all the row data from the result set, 
	 * store it into the local data structure,
	 * and close the ResultSet connection
	 *
	 * @return rowCount on success, -1 indicate there was no SQLResult to process
	 **/
	protected int fetchAllRows(ResultSet sqlRes) throws JSqlException {
		// If SQL Result is null, it means data been already "fetched"
		// or this class was not initialized properly
		if (sqlRes == null) {
			return -1;
		}
		
		try {
			// The column names in the result
			String[] colNames = JSqlResultUtil.extractColumnNames(sqlRes);
			int colCount = colNames.length;
			
			//
			// Lets initialize the collumn array lists
			//
			// Minor note:
			// Due to the limitation of some SQL implmentation (like sqlite)
			// being only to process data in a strict forward fashion. And not be able to
			// get the row count at the start.
			//
			// if this is fixed, we can one day initialize a fixed sized arrayList
			//
			for (int i = 0; i < colCount; ++i) {
				this.put(colNames[i], new GenericConvertArrayList<>());
			}
			
			// Time to iterate the actual SQL result, and count the rows
			// While transfering the result set over
			rowCount = 0;
			while (sqlRes.next()) {
				// Iterate the result in a row, column by column
				for (int i = 0; i < colCount; i++) {
					// Gets the row data (1-indexed) and filters it
					Object dataVal = JSqlResultUtil.normalizeDataObject(sqlRes.getObject(i + 1));
					// Get array list to populate, then add in value
					this.get(colNames[i]).add(dataVal);
				}
				++rowCount;
			}
			
			// Return the final row count
			return rowCount;
		} catch (Exception e) {
			throw new JSqlException(e);
		} finally {
			// Dispose the original sql result set
			// For resource cleanup
			try {
				sqlRes.close();
			} catch (Exception e) {
				throw new JSqlException(e);
			}
		}
	}
	
	/**
	 * Return the row count, that is returned by the database
	 **/
	public int rowCount() {
		return rowCount;
	}
	
	/**
	 * Returns the amount of affected rows, applies only for non-select statements
	 **/
	public int rowAffected() {
		return rowAffected;
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
		for (Map.Entry<String, GenericConvertList<Object>> entry : this.entrySet()) {
			ret.put(entry.getKey(), entry.getValue().get(pt));
		}
		return ret;
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
	
	// @Override
	// public int hashCode() {
	// 	return rowCount;
	// }
	
}
