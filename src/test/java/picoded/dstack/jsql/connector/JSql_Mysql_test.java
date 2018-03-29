package picoded.dstack.jsql.connector;

import static org.junit.Assert.*;
import org.junit.*;

import java.util.Map;

import picoded.dstack.jsql.*;

///
/// JSql Test case which is specific for MYSQL
///
public class JSql_Mysql_test extends JSql_Base_test {
	
	///
	/// SQL implmentation to actually overwrite
	///
	public JSql sqlImplementation() {
		return JSqlTestConnection.mysql();
	}
	
}