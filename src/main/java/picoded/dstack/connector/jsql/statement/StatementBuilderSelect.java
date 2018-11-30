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
 * SELECT statement builder
 **/
public interface StatementBuilderSelect extends StatementBuilderBaseInterface {
	
	//-------------------------------------------------------------------------
	//
	// SELECT statement builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helps generate an SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors (if any).
	 *
	 * See : selectStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 *
	 * @return  The JSqlResult
	 **/
	default public JSqlResult select( //
		String tableName // Table name to select from
	) {
		return selectStatement(tableName, "*", null, null, null, 0, 0).query();
	}
	
	/**
	 * Helps generate an SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors (if any).
	 *
	 * See : selectStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 *
	 * @return  The JSqlResult
	 **/
	default public JSqlResult select( //
		String tableName, // Table name to select from
		String selectStatement // The Columns to select, null means all
	) {
		return selectStatement(tableName, selectStatement, null, null, null, 0, 0).query();
	}
	
	/**
	 * Helps generate an SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors (if any).
	 *
	 * See : selectStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 *
	 * @return  The JSqlResult
	 **/
	default public JSqlResult select( //
		String tableName, // Table name to select from
		String selectStatement, // The Columns to select, null means all
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues // Values that corresponds to the where statement
	) {
		return selectStatement(tableName, selectStatement, whereStatement, whereValues, null, 0, 0)
			.query();
	}
	
	/**
	 * Helps generate an SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors (if any).
	 *
	 * See : selectStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 * @param  Order by statement         (eg: col2 DESC)
	 * @param  Row count limit            (eg: 2)
	 * @param  Row offset                 (eg: 3)
	 *
	 * @return  The JSqlResult
	 **/
	default public JSqlResult select( //
		String tableName, // Table name to select from
		//
		String selectStatement, // The Columns to select, null means all
		//
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues, // Values that corresponds to the where statement
		//
		String orderStatement, // Order by statements, must be either ASC / DESC
		//
		long limit, // Limit row count to, use 0 to ignore / disable
		long offset // Offset limit by?
	) {
		return selectStatement(tableName, selectStatement, whereStatement, whereValues,
			orderStatement, limit, offset).query();
	}
	
	/**
	 * Helps generate an SQL SELECT request. This function was created to acommedate the various
	 * syntax differances of SELECT across the various SQL vendors (if any).
	 *
	 * The syntax below, is an example of such an SELECT statement for SQLITE.
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * SELECT
	 *	col1, col2   //select collumn
	 * FROM tableName //table name to select from
	 * WHERE
	 *	col1=?       //where clause
	 * ORDER BY
	 *	col2 DESC    //order by clause
	 * LIMIT 2        //limit clause
	 * OFFSET 3       //offset clause
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Columns to select          (eg: col1, col2)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 * @param  Order by statement         (eg: col2 DESC)
	 * @param  Row count limit            (eg: 2)
	 * @param  Row offset                 (eg: 3)
	 *
	 * @return  A prepared SELECT statement
	 **/
	default public JSqlPreparedStatement selectStatement( //
		String tableName, // Table name to select from
		//
		String selectStatement, // The Columns to select, null means all
		//
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues, // Values that corresponds to the where statement
		//
		String orderStatement, // Order by statements, must be either ASC / DESC
		//
		long limit, // Limit row count to, use 0 to ignore / disable
		long offset // Offset limit by?
	) {
		throw new UnsupportedOperationException(JSqlException.invalidDatabaseImplementationException);
	}
	
}