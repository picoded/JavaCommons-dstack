package picoded.dstack.connector.jsql;

import static org.junit.Assert.*;
import org.junit.*;
import picoded.dstack.jsql.JSqlTestConfig;

///
/// JSql Test case which is specific for SQLite
///
public class JSql_Mysql_test extends JSql_Base_test {
	
	/**
	 * SQL implmentation to actually overwrite
	 * @return the JSql connection to test, this is called on every test
	 */
	public JSql sqlImplementation() {
		return new JSql_Mysql(JSqlTestConfig.MYSQL_CONN(), JSqlTestConfig.MYSQL_DATA(),
			JSqlTestConfig.MYSQL_USER(), JSqlTestConfig.MYSQL_PASS());
	}
	
}