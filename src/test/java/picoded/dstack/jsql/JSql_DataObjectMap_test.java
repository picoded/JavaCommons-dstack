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
		if (!(mtObj instanceof JSql_DataObjectMap)) {
			return;
		}
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

	/**
	 * Tests JSql backend queryIn and queryIn_id execution across multiple values.
	 * Verifies that parameterized OR conditions correctly match rows in the JSql EAV tables.
	 */
	@Test
	public void testJsQlQueryInWithMultipleValues() throws Exception {
		DataObjectMap map = mtObj;

		DataObject doc1 = map.newEntry();
		doc1.put("category", "hardware");
		doc1.put("vendor", "intel");
		doc1.saveDelta();

		DataObject doc2 = map.newEntry();
		doc2.put("category", "software");
		doc2.put("vendor", "google");
		doc2.saveDelta();

		DataObject doc3 = map.newEntry();
		doc3.put("category", "services");
		doc3.put("vendor", "aws");
		doc3.saveDelta();

		// Query for "hardware" and "services"
		List<String> categories = Arrays.asList("hardware", "services");
		DataObject[] matchedDocs = map.queryIn("category", categories);
		assertEquals(2, matchedDocs.length);

		// Query IDs for "software"
		String[] matchedIds = map.queryIn_id("category", Arrays.asList("software"));
		assertEquals(1, matchedIds.length);
		assertEquals(doc2._oid(), matchedIds[0]);

		// Query by oID
		String[] oIdMatches = map.queryIn_id("_oid", Arrays.asList(doc1._oid(), doc3._oid()));
		assertEquals(2, oIdMatches.length);
	}

	/**
	 * Tests JSql backend queryIn with additionalWhereQuery and additionalWhereArgs.
	 * Verifies compound WHERE filtering (IN values AND additional condition) in SQL.
	 */
	@Test
	public void testJsQlQueryInWithAdditionalWhereQuery() throws Exception {
		DataObjectMap map = mtObj;

		DataObject doc1 = map.newEntry();
		doc1.put("team", "infra");
		doc1.put("active", "true");
		doc1.saveDelta();

		DataObject doc2 = map.newEntry();
		doc2.put("team", "infra");
		doc2.put("active", "false");
		doc2.saveDelta();

		DataObject doc3 = map.newEntry();
		doc3.put("team", "qa");
		doc3.put("active", "true");
		doc3.saveDelta();

		// Query for team IN ("infra", "qa") AND active = "true"
		DataObject[] activeTeamDocs = map.queryIn(
			"team",
			Arrays.asList("infra", "qa"),
			"active = ?",
			new Object[] { "true" }
		);
		assertEquals(2, activeTeamDocs.length);

		// Query IDs for team IN ("infra", "qa") AND active = "false"
		String[] inactiveTeamIds = map.queryIn_id(
			"team",
			Arrays.asList("infra", "qa"),
			"active = ?",
			new Object[] { "false" }
		);
		assertEquals(1, inactiveTeamIds.length);
		assertEquals(doc2._oid(), inactiveTeamIds[0]);
	}
}
