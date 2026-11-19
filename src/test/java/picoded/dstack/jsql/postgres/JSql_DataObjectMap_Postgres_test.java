package picoded.dstack.jsql.postgres;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

// Test depends
import picoded.dstack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.connector.jsql.*;
import picoded.dstack.struct.simple.*;

public class JSql_DataObjectMap_Postgres_test extends JSql_DataObjectMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this SQL connector constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.postgres();
	}

	@Test
	public void testPostgresRemoveBulk() throws Exception {
		DataObject obj1 = mtObj.newEntry();
		obj1.put("col1", "pg-val-1");
		obj1.saveDelta();

		DataObject obj2 = mtObj.newEntry();
		obj2.put("col1", "pg-val-2");
		obj2.saveDelta();

		long removed = mtObj.removeBulk(Arrays.asList(obj1._oid(), obj2._oid()));
		assertEquals(2, removed);

		assertNull(mtObj.get(obj1._oid()));
		assertNull(mtObj.get(obj2._oid()));
	}

	/**
	 * Tests JSql on Postgres queryIn and queryIn_id execution across multiple values.
	 * Verifies parameterized SQL OR conditions on PostgreSQL backend tables.
	 */
	@Test
	public void testPostgresJsQlQueryInMultipleValues() throws Exception {
		DataObject doc1 = mtObj.newEntry();
		doc1.put("plan", "free");
		doc1.put("seats", "1");
		doc1.saveDelta();

		DataObject doc2 = mtObj.newEntry();
		doc2.put("plan", "pro");
		doc2.put("seats", "5");
		doc2.saveDelta();

		DataObject doc3 = mtObj.newEntry();
		doc3.put("plan", "enterprise");
		doc3.put("seats", "50");
		doc3.saveDelta();

		// Query for "free" and "enterprise"
		DataObject[] planDocs = mtObj.queryIn("plan", Arrays.asList("free", "enterprise"));
		assertEquals(2, planDocs.length);

		// Query IDs for "pro"
		String[] proIds = mtObj.queryIn_id("plan", Arrays.asList("pro"));
		assertEquals(1, proIds.length);
		assertEquals(doc2._oid(), proIds[0]);

		// Query by oID
		String[] oIdMatches = mtObj.queryIn_id("_oid", Arrays.asList(doc1._oid(), doc2._oid()));
		assertEquals(2, oIdMatches.length);
	}

	/**
	 * Tests JSql on Postgres queryIn with additionalWhereQuery and additionalWhereArgs.
	 * Verifies compound WHERE filtering (IN values AND additional WHERE clause) on PostgreSQL.
	 */
	@Test
	public void testPostgresJsQlQueryInWithAdditionalWhere() throws Exception {
		DataObject doc1 = mtObj.newEntry();
		doc1.put("tier", "silver");
		doc1.put("enabled", "true");
		doc1.saveDelta();

		DataObject doc2 = mtObj.newEntry();
		doc2.put("tier", "silver");
		doc2.put("enabled", "false");
		doc2.saveDelta();

		DataObject doc3 = mtObj.newEntry();
		doc3.put("tier", "gold");
		doc3.put("enabled", "true");
		doc3.saveDelta();

		// Query for tier IN ("silver", "gold") AND enabled = "true"
		DataObject[] enabledDocs = mtObj.queryIn(
			"tier",
			Arrays.asList("silver", "gold"),
			"enabled = ?",
			new Object[] { "true" }
		);
		assertEquals(2, enabledDocs.length);

		// Query IDs for tier IN ("silver", "gold") AND enabled = "false"
		String[] disabledIds = mtObj.queryIn_id(
			"tier",
			Arrays.asList("silver", "gold"),
			"enabled = ?",
			new Object[] { "false" }
		);
		assertEquals(1, disabledIds.length);
		assertEquals(doc2._oid(), disabledIds[0]);
	}
	
}
