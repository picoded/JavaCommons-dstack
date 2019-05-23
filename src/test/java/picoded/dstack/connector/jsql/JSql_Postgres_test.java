package picoded.dstack.connector.jsql;

import static org.junit.Assert.*;
import org.junit.*;
import picoded.dstack.jsql.JSqlTestConfig;

///
/// JSql Test case which is specific for MSSQL
///
public class JSql_Postgres_test extends JSql_Base_test {
	
	/**
	 * SQL implmentation to actually overwrite
	 * @return the JSql connection to test, this is called on every test
	 */
	public JSql sqlImplementation() {
		return new JSql_Postgres(JSqlTestConfig.POSTGRES_HOST(), JSqlTestConfig.POSTGRES_PORT(),
			JSqlTestConfig.POSTGRES_NAME(), JSqlTestConfig.POSTGRES_USER(),
			JSqlTestConfig.POSTGRES_PASS());
	}
	
}