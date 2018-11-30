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
 * SELECT random row statement builder
 **/
public interface StatementBuilderRandomSelect extends StatementBuilderBaseInterface {
	
	//-------------------------------------------------------------------------
	//
	// Randomly SELECT statement builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helps generate an random SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors.
	 *
	 * See : randomSelectStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 *
	 * @return  The JSqlResult
	 **/
	default JSqlResult randomSelect( //
		String tableName, // Table name to select from
		String selectStatement // The Columns to select, null means all
	) {
		return randomSelect(tableName, selectStatement, null, null, 0);
	}
	
	/**
	 * Helps generate an random SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors.
	 *
	 * See : randomSelectStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 *
	 * @return  The JSqlResult
	 **/
	default JSqlResult randomSelect( //
		String tableName, // Table name to select from
		//
		String selectStatement, // The Columns to select, null means all
		//
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues // Values that corresponds to the where statement
	) {
		return randomSelect(tableName, selectStatement, whereStatement, whereValues, 0);
	}
	
	/**
	 * Helps generate an random SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors.
	 *
	 * See : randomSelectStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 * @param  Row count, 0 for all       (eg: 3)
	 *
	 * @return  The JSqlResult
	 **/
	default JSqlResult randomSelect( //
		String tableName, // Table name to select from
		//
		String selectStatement, // The Columns to select, null means all
		//
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues, // Values that corresponds to the where statement
		//
		long rowLimit // Number of rows
	) {
		return randomSelectStatement(tableName, selectStatement, whereStatement, whereValues,
			rowLimit).query();
	}
	
	/**
	 * Helps generate an random SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors.
	 *
	 * See : randomSelectStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 *
	 * @return  A prepared SELECT statement
	 **/
	default JSqlPreparedStatement randomSelectStatement( //
		String tableName, // Table name to select from
		String selectStatement // The Columns to select, null means all
	) {
		return randomSelectStatement(tableName, selectStatement, null, null, 0);
	}
	
	/**
	 * Helps generate an random SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors.
	 *
	 * See : randomSelectStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 *
	 * @return  A prepared SELECT statement
	 **/
	default JSqlPreparedStatement randomSelectStatement( //
		String tableName, // Table name to select from
		//
		String selectStatement, // The Columns to select, null means all
		//
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues // Values that corresponds to the where statement
	) {
		return randomSelectStatement(tableName, selectStatement, whereStatement, whereValues, 0);
	}
	
	/**
	 * Helps generate an random SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors.
	 *
	 * SEE: https://stackoverflow.com/questions/580639/how-to-randomly-select-rows-in-sql
	 *
	 * The syntax below, is an example of such a random SELECT statement for SQLITE.
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * SELECT
	 *	col1, col2      // select collumn
	 * FROM tableName    // table name to select from
	 * WHERE
	 *	col1=?          // where clause
	 * ORDER BY RANDOM() // Random sorting
	 * LIMIT 1           // number of rows to fetch
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 * @param  Row count, 0 for all       (eg: 3)
	 *
	 * @return  A prepared SELECT statement
	 **/
	default JSqlPreparedStatement randomSelectStatement( //
		String tableName, // Table name to select from
		//
		String selectStatement, // The Columns to select, null means all
		//
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues, // Values that corresponds to the where statement
		//
		long rowLimit // Number of rows
	) {
		throw new UnsupportedOperationException(JSqlException.invalidDatabaseImplementationException);
	}
	
}