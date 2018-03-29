package picoded.dstack.jsql.connector;

import static org.junit.Assert.*;
import org.junit.*;

import java.util.Map;

import picoded.dstack.jsql.*;

///
/// JSql Test case which is specific for Mssql
///
public class JSql_Mssql_test extends JSql_Base_test {
	
	///
	/// SQL implmentation to actually overwrite
	///
	public JSql sqlImplementation() {
		return JSqlTestConnection.mssql();
	}
	
}