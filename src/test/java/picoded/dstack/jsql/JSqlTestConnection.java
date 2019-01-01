package picoded.dstack.jsql;

import picoded.dstack.connector.jsql.*;

/// Utility test class to get the respective JSQL connection used in TESTING
public class JSqlTestConnection {
	
	/// SQLite connection
	public static JSql sqlite() {
		return new JSql_Sqlite();
	}
	
	/// MYSQL connection
	public static JSql mysql() {
		return new JSql_Mysql(JSqlTestConfig.MYSQL_HOST(), JSqlTestConfig.MYSQL_PORT(),
			JSqlTestConfig.MYSQL_DATA(), JSqlTestConfig.MYSQL_USER(), JSqlTestConfig.MYSQL_PASS());
	}
	
	/// MSSQL connection
	public static JSql mssql() {
		return new JSql_Mssql(JSqlTestConfig.MSSQL_HOST(), JSqlTestConfig.MSSQL_PORT(),
			JSqlTestConfig.MSSQL_NAME(), JSqlTestConfig.MSSQL_USER(), JSqlTestConfig.MSSQL_PASS());
	}
}