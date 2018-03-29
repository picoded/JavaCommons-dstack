package picoded.dstack.jsql;

import picoded.dstack.jsql.connector.*;

/// Utility test class to get the respective JSQL connection used in TESTING
public class JSqlTestConnection {
	
	/// SQLite connection
	public static JSql sqlite() {
		return JSql.sqlite();
	}
	
	/// MYSQL connection
	public static JSql mysql() {
		return JSql.mysql(JSqlTestConfig.MYSQL_CONN(), JSqlTestConfig.MYSQL_DATA(), JSqlTestConfig.MYSQL_USER(),
		JSqlTestConfig.MYSQL_PASS());
	}
	
	/// MSSQL connection
	public static JSql mssql() {
		return JSql.mssql(JSqlTestConfig.MSSQL_CONN(), JSqlTestConfig.MSSQL_NAME(), JSqlTestConfig.MSSQL_USER(),
		JSqlTestConfig.MSSQL_PASS());
	}
}