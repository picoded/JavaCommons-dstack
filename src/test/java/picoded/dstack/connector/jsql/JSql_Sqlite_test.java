package picoded.dstack.connector.jsql;

import static org.junit.Assert.*;
import org.junit.*;

///
/// JSql Test case which is specific for SQLite
///
public class JSql_Sqlite_test extends JSql_Base_test {
	
	/**
	 * SQL implmentation to actually overwrite
	 * @return the JSql connection to test, this is called on every test
	 */
	public JSql sqlImplementation() {
		return new JSql_Sqlite();
	}
	
}