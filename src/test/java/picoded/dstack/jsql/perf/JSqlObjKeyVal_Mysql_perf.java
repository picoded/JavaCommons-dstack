package picoded.dstack.jsql.perf;

import picoded.dstack.jsql.connector.*;
import picoded.dstack.jsql.*;
import picoded.dstack.jsql.JSqlTestConfig;

/// [Mysql varient]
/// Testing of DataTable full obj-key-val structure performance
public class JSqlObjKeyVal_Mysql_perf extends JSqlObjKeyVal_perf {
	
	/// Note that this SQL connector constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.mysql();
	}
	
}