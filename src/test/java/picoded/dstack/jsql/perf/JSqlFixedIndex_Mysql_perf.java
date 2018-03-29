package picoded.dstack.jsql.perf;

// Test depends
import picoded.dstack.jsql.connector.*;
import picoded.dstack.jsql.*;
import picoded.dstack.jsql.JSqlTestConfig;

/// Testing of DataTable full indexless fixed table performance
public class JSqlFixedIndex_Mysql_perf extends JSqlFixedIndexless_perf {
	
	/// Note that this SQL connector constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.mysql();
	}
	
	public void tableSetup() {
		// Does the table setup
		super.tableSetup();
		
		// Alter table format to be "dynamic", required to keep the large number of rows
		jsqlObj.update_raw("ALTER TABLE `" + tablename + "` ROW_FORMAT=DYNAMIC;");
	}
}