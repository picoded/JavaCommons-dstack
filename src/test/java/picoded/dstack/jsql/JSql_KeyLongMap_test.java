package picoded.dstack.jsql;

import picoded.dstack.KeyLongMap;
import picoded.dstack.jsql.connector.JSql;
import picoded.dstack.struct.simple.StructSimple_KeyLongMap_test;

public class JSql_KeyLongMap_test extends StructSimple_KeyLongMap_test{
	// To override for implementation
	//-----------------------------------------------------

	/// Note that this SQL connector constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.sqlite();
	}

	/// Impomentation constructor for SQL
	public KeyLongMap implementationConstructor() {
		return new JSql_KeyLongMap(jsqlConnection(), JSqlTestConfig.randomTablePrefix());
	}
}
