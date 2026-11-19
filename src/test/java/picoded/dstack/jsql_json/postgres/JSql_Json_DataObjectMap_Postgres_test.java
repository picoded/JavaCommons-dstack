package picoded.dstack.jsql_json.postgres;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

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

	@Test
	public void testPostgresJsonbRemoveBulk() throws Exception {
		DataObject obj1 = mtObj.newEntry();
		obj1.put("jsonField", "jsonb-val-1");
		obj1.saveDelta();

		DataObject obj2 = mtObj.newEntry();
		obj2.put("jsonField", "jsonb-val-2");
		obj2.saveDelta();

		long removed = mtObj.removeBulk(Arrays.asList(obj1._oid(), obj2._oid()));
		assertEquals(2, removed);

		assertNull(mtObj.get(obj1._oid()));
		assertNull(mtObj.get(obj2._oid()));
	}
	
}
