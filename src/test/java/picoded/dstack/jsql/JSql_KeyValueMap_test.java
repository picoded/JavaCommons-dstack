package picoded.dstack.jsql;

import picoded.dstack.KeyValueMap;
import picoded.dstack.jsql.connector.JSql;
import picoded.dstack.struct.simple.StructSimple_KeyValueMap_test;

public class JSql_KeyValueMap_test extends StructSimple_KeyValueMap_test {
	// To override for implementation
	//-----------------------------------------------------

	/// Note that this SQL connector constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.sqlite();
	}

	/// Impomentation constructor for SQL
	public KeyValueMap implementationConstructor() {
		return new JSql_KeyValueMap(jsqlConnection(), JSqlTestConfig.randomTablePrefix());
	}
}
