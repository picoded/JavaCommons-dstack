package picoded.dstack.jsql_json.postgres;

// Test depends
import picoded.dstack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.jsql_json.*;
import picoded.dstack.connector.jsql.*;
import picoded.dstack.struct.simple.*;

public class JSql_Json_DataObjectMap_Postgres_test extends JSql_DataObjectMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this SQL connector constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.postgres();
	}
	
	/// Impomentation constructor for SQL
	public DataObjectMap implementationConstructor() {
		return new PostgresJsonb_DataObjectMap(jsqlConnection(), JSqlTestConfig.randomTablePrefix());
	}
	
}
