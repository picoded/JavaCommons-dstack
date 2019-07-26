package picoded.dstack.connector.jsql;

import static org.junit.Assert.*;
import org.junit.*;
import picoded.dstack.jsql.JSqlTestConfig;

///
/// JSql Test case which is specific for MSSQL
///
public class JSql_Mssql_test extends JSql_Base_test {
	
	/**
	 * SQL implmentation to actually overwrite
	 * @return the JSql connection to test, this is called on every test
	 */
	public JSql sqlImplementation() {
		return new JSql_Mssql(JSqlTestConfig.MSSQL_HOST(), JSqlTestConfig.MSSQL_PORT(),
			JSqlTestConfig.MSSQL_NAME(), JSqlTestConfig.MSSQL_USER(), JSqlTestConfig.MSSQL_PASS());
	}
	
	// // THIS HAS BEEN DEPRECATED - commit toggling support was dropped in hikariCP migration
	// @Test
	// public void commitTest() {
	// 	throw new RuntimeException("Currently hangs and does not work Q.Q");
	// 	// jsqlObj.update("DROP TABLE IF EXISTS " + testTableName + ""); //cleanup (just incase)
	// 	// jsqlObj.update("CREATE TABLE IF NOT EXISTS " + testTableName
	// 	// 	+ " ( `col[1].pk` INT PRIMARY KEY, col2 TEXT )");
	// 	// jsqlObj.setAutoCommit(false);
	// 	// assertFalse(jsqlObj.getAutoCommit());
	// 	// jsqlObj.update("INSERT INTO " + testTableName + " ( `col[1].pk`, col2 ) VALUES (?,?)", 404,
	// 	// 	"has nothing");
	// 	// jsqlObj.commit();
	// 	// JSqlResult r = jsqlObj.query("SELECT * FROM " + testTableName + "");
	// 	// assertNotNull("SQL result returns as expected", r);
	// 	// r.fetchAllRows();
	// 	// assertEquals("via readRow", 404, ((Number) r.readRow(0).getInt("col[1].pk")).intValue());
	// }
	
	// Currently fails Q_Q
	@Test
	public void upsertStatementWithDefault() {
		row1to7setup();
		JSqlResult r = null;
		
		jsqlObj.update("DROP TABLE IF EXISTS `" + testTableName + "_1`"); //cleanup (just incase)
		
		jsqlObj
			.update("CREATE TABLE IF NOT EXISTS "
				+ testTableName
				+ "_1 ( col1 INT PRIMARY KEY, col2 TEXT, col3 VARCHAR(50), col4 VARCHAR(100) NOT NULL DEFAULT 'ABC' )");
		//valid table creation : no exception
		//jsqlObj.update("ALTER TABLE " + testTableName + "_1 ADD CONSTRAINT c_col4 DEFAULT (ABC) FOR col4;");
		
		assertNotNull("query should return a JSql result",
			r = jsqlObj.query("SELECT * FROM " + testTableName + " ORDER BY col1 ASC"));
		assertEquals("Initial value check failed", 404, ((Number) r.readRow(0).getInt("col1")));
		assertEquals("Initial value check failed", "has nothing", r.readRow(0).getString("col2"));
		
		//Upsert query
		assertTrue(jsqlObj.upsert( //
			testTableName + "_1", //
			new String[] { "col1" }, new Object[] { 404 }, //
			//new String[] { "col2", "col3" }, new Object[] { "not found", "not found" },  //
			new String[] { "col2" }, new Object[] { "not found" }, //
			//new String[] { "col4", "col5" }, new Object[] { "not found", "not found" },
			new String[] { "col3" }, new Object[] { "3 not found" }, new String[] { "col4" } //
			));
		
		assertNotNull("query should return a JSql result",
			r = jsqlObj.query("SELECT * FROM " + testTableName + "_1 ORDER BY col1 ASC"));
		assertEquals("Upsert value check failed", 404, ((Number) r.readRow(0).getInt("col1")));
		assertEquals("Upsert value check failed", "not found", r.readRow(0).getString("col2"));
		assertEquals("Upsert value check failed", "ABC", r.readRow(0).getString("col4"));
	}
}