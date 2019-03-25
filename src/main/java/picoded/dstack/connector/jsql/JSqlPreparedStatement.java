package picoded.dstack.connector.jsql;

/**
 * A prepared JSql query
 * used to prepare a query and execute it subsequently later
 **/
public class JSqlPreparedStatement {
	
	/**
	 * SQL query set
	 **/
	protected String sqlQuery = null;
	
	/**
	 * SQL arguments set
	 **/
	protected Object[] sqlArgs = null;
	
	/**
	 * The JSql object to query against
	 **/
	protected JSql jsqlObj = null;
	
	/**
	 * Initialize the query set with the following options
	 *
	 * @param  The sql query to use
	 * @param  The sql arguments for the query
	 * @param  JSql connection
	 **/
	public JSqlPreparedStatement(String query, Object[] args, JSql dbObj) {
		sqlQuery = query;
		sqlArgs = args;
		jsqlObj = dbObj;
	}
	
	/**
	 * Gets the stored sql query string
	 **/
	public String getQuery() {
		return sqlQuery;
	}
	
	/**
	 * Gets the stored arguments list
	 **/
	public Object[] getArguments() {
		return sqlArgs;
	}
	
	/**
	 * Gets the stored JSql connection
	 **/
	public JSql getJSql() {
		return jsqlObj;
	}
	
	/**
	 * Executes the argumented SQL query, and immediately fetches the result from
	 * the database into the result set.
	 *
	 * Custom SQL specific parsing occurs here
	 *
	 * **Note:** Only queries starting with 'SELECT' will produce a JSqlResult object that has fetchable results
	 **/
	public JSqlResult query() {
		return jsqlObj.query(sqlQuery, sqlArgs);
	}
	
	/**
	 * Executes the argumented SQL update.
	 *
	 * Returns false if no result object is given by the execution call.
	 *
	 * Custom SQL specific parsing occurs here
	 **/
	public int update() {
		return jsqlObj.update(sqlQuery, sqlArgs);
	}
}
