package picoded.dstack.jsql;

import picoded.dstack.DataObjectMap;
import picoded.dstack.FileWorkspaceMap;
import picoded.dstack.jsql.connector.JSql;
import picoded.dstack.struct.simple.StructSimple_FileWorkspaceMap_test;

public class JSql_FileWorkspaceMap_test extends StructSimple_FileWorkspaceMap_test {

	// To override for implementation
	//-----------------------------------------------------

	/// Note that this SQL connector constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.sqlite();
	}

	/// Impomentation constructor for SQL
	public FileWorkspaceMap implementationConstructor() {
		return new JSql_FileWorkspaceMap(jsqlConnection(), JSqlTestConfig.randomTablePrefix());
	}
}
