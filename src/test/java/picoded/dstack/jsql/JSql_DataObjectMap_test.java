package picoded.dstack.jsql;

// Target test class
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

// Test Case include
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// Test depends
import picoded.dstack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.connector.jsql.*;
import picoded.dstack.struct.simple.*;

public class JSql_DataObjectMap_test extends StructSimple_DataObjectMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this SQL connector constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.sqlite();
	}
	
	/// Impomentation constructor for SQL
	public DataObjectMap implementationConstructor() {
		return new JSql_DataObjectMap(jsqlConnection(), JSqlTestConfig.randomTablePrefix());
	}

	@Test
	public void testRemoveBulkCleansUpUnderlyingSqlTables() throws Exception {
		JSql_DataObjectMap sqlMap = (JSql_DataObjectMap) mtObj;
		DataObject obj1 = sqlMap.newEntry();
		obj1.put("propA", "val1");
		obj1.put("propB", "val2");
		obj1.saveDelta();

		DataObject obj2 = sqlMap.newEntry();
		obj2.put("propA", "val3");
		obj2.put("propB", "val4");
		obj2.saveDelta();

		// Ensure records exist in underlying tables
		JSql sql = sqlMap.sqlObj;
		if (sqlMap.primaryKeyTable != null) {
			JSqlResult pkRowsBefore = sql.query(
				"SELECT * FROM " + sqlMap.primaryKeyTable + " WHERE oID IN (?, ?)",
				new Object[] { obj1._oid(), obj2._oid() }
			);
			assertEquals(2, pkRowsBefore.rowCount());
		}

		JSqlResult storageRowsBefore = sql.query(
			"SELECT * FROM " + sqlMap.dataStorageTable + " WHERE oID IN (?, ?)",
			new Object[] { obj1._oid(), obj2._oid() }
		);
		assertTrue(storageRowsBefore.rowCount() > 0);

		// Perform bulk remove
		long removed = sqlMap.removeBulk(Arrays.asList(obj1._oid(), obj2._oid()));
		assertEquals(2, removed);

		// Verify both primary key table and EAV data storage table are cleared
		if (sqlMap.primaryKeyTable != null) {
			JSqlResult pkRowsAfter = sql.query(
				"SELECT * FROM " + sqlMap.primaryKeyTable + " WHERE oID IN (?, ?)",
				new Object[] { obj1._oid(), obj2._oid() }
			);
			assertEquals(0, pkRowsAfter.rowCount());
		}

		JSqlResult storageRowsAfter = sql.query(
			"SELECT * FROM " + sqlMap.dataStorageTable + " WHERE oID IN (?, ?)",
			new Object[] { obj1._oid(), obj2._oid() }
		);
		assertEquals(0, storageRowsAfter.rowCount());
	}
}
