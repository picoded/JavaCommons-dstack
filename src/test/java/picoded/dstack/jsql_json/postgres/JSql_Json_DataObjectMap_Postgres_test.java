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

	/**
	 * Tests Postgres JSONB queryIn and queryIn_id execution across multiple values.
	 * Verifies native JSONB column containment and oID matching in PostgreSQL.
	 */
	@Test
	public void testPostgresJsonbQueryInBasicAndByOid() throws Exception {
		DataObject doc1 = mtObj.newEntry();
		doc1.put("env", "staging");
		doc1.put("cluster", "primary");
		doc1.saveDelta();

		DataObject doc2 = mtObj.newEntry();
		doc2.put("env", "production");
		doc2.put("cluster", "primary");
		doc2.saveDelta();

		DataObject doc3 = mtObj.newEntry();
		doc3.put("env", "sandbox");
		doc3.put("cluster", "secondary");
		doc3.saveDelta();

		// Query for "staging" and "production"
		DataObject[] envDocs = mtObj.queryIn("env", Arrays.asList("staging", "production"));
		assertEquals(2, envDocs.length);

		// Query IDs for "production"
		String[] prodIds = mtObj.queryIn_id("env", Arrays.asList("production"));
		assertEquals(1, prodIds.length);
		assertEquals(doc2._oid(), prodIds[0]);

		// Query by oID
		String[] oIdMatches = mtObj.queryIn_id("_oid", Arrays.asList(doc1._oid(), doc3._oid()));
		assertEquals(2, oIdMatches.length);
	}

	/**
	 * Tests Postgres JSONB queryIn with additionalWhereQuery and additionalWhereArgs.
	 * Verifies compound WHERE filtering (IN values AND additional WHERE clause) over JSONB attributes.
	 */
	@Test
	public void testPostgresJsonbQueryInWithAdditionalWhere() throws Exception {
		DataObject doc1 = mtObj.newEntry();
		doc1.put("region", "us-east");
		doc1.put("status", "healthy");
		doc1.saveDelta();

		DataObject doc2 = mtObj.newEntry();
		doc2.put("region", "us-east");
		doc2.put("status", "degraded");
		doc2.saveDelta();

		DataObject doc3 = mtObj.newEntry();
		doc3.put("region", "eu-west");
		doc3.put("status", "healthy");
		doc3.saveDelta();

		// Query for region IN ("us-east", "eu-west") AND status = "healthy"
		DataObject[] healthyDocs = mtObj.queryIn(
			"region",
			Arrays.asList("us-east", "eu-west"),
			"status = ?",
			new Object[] { "healthy" }
		);
		assertEquals(2, healthyDocs.length);

		// Query IDs for region IN ("us-east", "eu-west") AND status = "degraded"
		String[] degradedIds = mtObj.queryIn_id(
			"region",
			Arrays.asList("us-east", "eu-west"),
			"status = ?",
			new Object[] { "degraded" }
		);
		assertEquals(1, degradedIds.length);
		assertEquals(doc2._oid(), degradedIds[0]);
	}
	
}
