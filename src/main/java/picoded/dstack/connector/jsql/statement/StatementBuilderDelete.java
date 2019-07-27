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
 * DELETE statement builder
 **/
public interface StatementBuilderDelete extends StatementBuilderBaseInterface {
	
	/**
	 * Internal self used logger
	 **/
	public static final Logger LOGGER = Logger.getLogger(JSql.class.getName());
	
	//-------------------------------------------------------------------------
	//
	// DELETE statement builder
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Helps generate an SQL DELETE request. This function was created to acommedate the various
	 * syntax differances of DELETE across the various SQL vendors (if any).
	 *
	 * See : deleteStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 *
	 * @return  the number of rows affected
	 **/
	default int delete( //
		String tableName // Table name to select from
	) {
		return deleteStatement(tableName, null, null).update();
	}
	
	/**
	 * Helps generate an SQL DELETE request. This function was created to acommedate the various
	 * syntax differances of DELETE across the various SQL vendors (if any).
	 *
	 * See : deleteStatement for full docs
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 *
	 * @return  the number of rows affected
	 **/
	default int delete( //
		String tableName, // Table name to select from
		//
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues // Values that corresponds to the where statement
	) {
		return deleteStatement(tableName, whereStatement, whereValues).update();
	}
	
	/**
	 * Helps generate an SQL DELETE request. This function was created to acommedate the various
	 * syntax differances of DELETE across the various SQL vendors (if any).
	 *
	 * The syntax below, is an example of such an DELETE statement for SQLITE.
	 *
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{.SQL}
	 * DELETE
	 * FROM tableName //table name to select from
	 * WHERE
	 *	col1=?       //where clause
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 *
	 * @param  Table name to query        (eg: tableName)
	 * @param  Where statement to filter  (eg: col1=?)
	 * @param  Where arguments value      (eg: [value/s])
	 *
	 * @return  A prepared DELETE statement
	 **/
	default JSqlPreparedStatement deleteStatement( //
		String tableName, // Table name to select from
		//
		String whereStatement, // The Columns to apply where clause, this must be sql neutral
		Object[] whereValues // Values that corresponds to the where statement
	) {
		
		if (tableName.length() > 30) {
			LOGGER.warning(JSqlException.oracleNameSpaceWarning + tableName);
		}
		
		ArrayList<Object> queryArgs = new ArrayList<Object>();
		StringBuilder queryBuilder = new StringBuilder("DELETE ");
		
		// From table names
		queryBuilder.append(" FROM `" + tableName + "`");
		
		// Where clauses
		if (whereStatement != null && (whereStatement = whereStatement.trim()).length() >= 3) {
			queryBuilder.append(" WHERE ");
			queryBuilder.append(whereStatement);
			
			if (whereValues != null) {
				for (int b = 0; b < whereValues.length; ++b) {
					queryArgs.add(whereValues[b]);
				}
			}
		}
		
		// Create the query set
		return new JSqlPreparedStatement(queryBuilder.toString(), queryArgs.toArray(), (JSql) this);
	}
	
}