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
	
	@Test
	public void commitTest() {
		throw new RuntimeException("Currently hangs and does not work Q.Q");
		// jsqlObj.update("DROP TABLE IF EXISTS " + testTableName + ""); //cleanup (just incase)
		
		// jsqlObj.update("CREATE TABLE IF NOT EXISTS " + testTableName
		// 	+ " ( `col[1].pk` INT PRIMARY KEY, col2 TEXT )");
		// jsqlObj.setAutoCommit(false);
		// assertFalse(jsqlObj.getAutoCommit());
		// jsqlObj.update("INSERT INTO " + testTableName + " ( `col[1].pk`, col2 ) VALUES (?,?)", 404,
		// 	"has nothing");
		// jsqlObj.commit();
		// JSqlResult r = jsqlObj.query("SELECT * FROM " + testTableName + "");
		// assertNotNull("SQL result returns as expected", r);
		// r.fetchAllRows();
		// assertEquals("via readRow", 404, ((Number) r.readRow(0).getInt("col[1].pk")).intValue());
	}
}