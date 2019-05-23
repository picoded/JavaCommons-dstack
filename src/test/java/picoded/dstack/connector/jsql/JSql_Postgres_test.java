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
	
	//////////////////////////////////////////////////////////////////////////
	//
	// Change test table name to lowercase (POSTGRES preference)
	//
	//////////////////////////////////////////////////////////////////////////
	
	protected static String testTableName = "JSqlTest_"
		+ JSqlTestConfig.randomTablePrefix().toLowerCase();
	
	@BeforeClass
	public static void oneTimeSetUp() {
		// one-time initialization code
		testTableName = testTableName.toLowerCase();
	}
	
	//////////////////////////////////////////////////////////////////////////
	//
	// Overwritable drop table command
	//
	//////////////////////////////////////////////////////////////////////////
	
	/**
	 * Common reusable testing DROP TABLE command,
	 * this is needed as the keyword "CASCADE" is needed for postgres
	 * 
	 * It is currently of mixed oppinion if JSQL should auto fill this parameter or not 
	 * as its main use case should use the official jsqlObj.dropTable command
	 * which would add it in
	 * 
	 * @param tablename
	 */
	public void dropTableIfExist(String tablename) {
		jsqlObj.update("DROP TABLE IF EXISTS " + tablename + " CASCADE");
		jsqlObj.update("DROP TABLE IF EXISTS \"" + tablename + "\" CASCADE");
	}
	
	/**
	 * Some basic test for genericSqlParser conversions
	 */
	@Test
	public void genericSqlParserTest() {
		String s = ((JSql_Base) jsqlObj).genericSqlParser("SELECT * FROM " + testTableName
			+ " WHERE COL1 = ?");
		assertEquals(("SELECT * FROM " + testTableName + " WHERE COL1=?").toLowerCase(), s
			.toLowerCase().replaceAll("col1 = \\?", "col1=?"));
	}
	
}