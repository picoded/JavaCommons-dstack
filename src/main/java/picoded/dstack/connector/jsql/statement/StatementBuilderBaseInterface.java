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
 * The minimum query / update execution interface support.
 * Used to build up the much more complex query builders.
 **/
public interface StatementBuilderBaseInterface {
	
	//-------------------------------------------------------------------------
	//
	// Standard raw query/execute command sets
	//
	//-------------------------------------------------------------------------
	
	/**
	 * Executes the argumented SQL query, and immediately fetches the result from
	 * the database into the result set.
	 *
	 * This is a raw execution. As such no special parsing occurs to the request
	 *
	 * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  JSQL result set
	 **/
	JSqlResult query_raw(String qString, Object... values);
	
	/**
	 * Executes the argumented SQL update.
	 *
	 * Returns -1 if no result object is given by the execution call.
	 * Else returns the number of rows affected
	 *
	 * This is a raw execution. As such no special parsing occurs to the request
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  -1 if failed, 0 and above for affected rows
	 **/
	int update_raw(String qString, Object... values);
	
	//-------------------------------------------------------------------------------------
	//
	// Custom SQL specific parsing occurs here. Built ontop of query_raw / update_raw,
	// where generic SQL syntaxes are parsed over to the database specific implementation
	//
	//-------------------------------------------------------------------------------------
	
	/**
	 * Executes the argumented SQL query, and immediately fetches the result from
	 * the database into the result set.
	 *
	 * Custom SQL specific parsing occurs here
	 *
	 * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  JSQL result set
	 **/
	JSqlResult query(String qString, Object... values);
	
	/**
	 * Executes the argumented SQL update.
	 *
	 * Returns -1 if no result object is given by the execution call.
	 * Else returns the number of rows affected
	 *
	 * Custom SQL specific parsing occurs here
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  -1 if failed, 0 and above for affected rows
	 **/
	int update(String qString, Object... values);
	
	//-------------------------------------------------------------------------------------
	//
	// PreparedStatement builder
	//
	//-------------------------------------------------------------------------------------
	
	/**
	 * Prepare an SQL statement, for execution subsequently later
	 *
	 * Custom SQL specific parsing occurs here
	 *
	 * @param  Query strings including substituable variable "?"
	 * @param  Array of arguments to do the variable subtitution
	 *
	 * @return  Prepared statement
	 **/
	JSqlPreparedStatement prepareStatement(String qString, Object... values);
	
}