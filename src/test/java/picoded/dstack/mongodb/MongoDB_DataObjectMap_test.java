package picoded.dstack.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.mongodb.client.model.Filters;

import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

/**
 * ## Purpose
 * This class is meant to test the MongoDB_DataObjectMap implementation,
 * and ensure that it passes all the test layed out in StructSImple_DataObjectMap_test
 * 
 */
public class MongoDB_DataObjectMap_test extends StructSimple_DataObjectMap_test {
	
	// Hazelcast stack instance
	protected static volatile MongoDBStack instance = null;
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Impomentation constructor
	public DataObjectMap implementationConstructor() {
		
		// Initialize server
		synchronized (MongoDB_DataObjectMap_test.class) {
			if (instance == null) {
				// The default config uses localhost, 27017
				GenericConvertMap<String, Object> mongodbConfig = new GenericConvertHashMap<>();
				mongodbConfig.put("host", DStackTestConfig.MONGODB_HOST());
				mongodbConfig.put("port", DStackTestConfig.MONGODB_PORT());
				
				// Use a random DB name
				mongodbConfig.put("name", DStackTestConfig.randomTablePrefix());
				
				GenericConvertMap<String, Object> stackConfig = new GenericConvertHashMap<>();
				stackConfig.put("name", "MongoDB_DataObjectMap_test");
				stackConfig.put("mongodb", mongodbConfig);
				
				instance = new MongoDBStack(stackConfig);
			}
		}
		
		// Load the DataObjectMap
		return instance.dataObjectMap(DStackTestConfig.randomTablePrefix());
	}

	@Test
	public void testMongoDirectCollectionBulkRemove() throws Exception {
		MongoDB_DataObjectMap mongoMap = (MongoDB_DataObjectMap) mtObj;

		DataObject obj1 = mongoMap.newEntry();
		obj1.put("title", "mongo-doc-1");
		obj1.saveDelta();

		DataObject obj2 = mongoMap.newEntry();
		obj2.put("title", "mongo-doc-2");
		obj2.saveDelta();

		DataObject obj3 = mongoMap.newEntry();
		obj3.put("title", "mongo-doc-3");
		obj3.saveDelta();

		// Verify documents exist in underlying MongoDB collection
		assertEquals(2, mongoMap.collection.countDocuments(
			Filters.in("_oid", Arrays.asList(obj1._oid(), obj2._oid()))
		));

		// Execute bulk removal
		long removed = mongoMap.removeBulk(Arrays.asList(obj1._oid(), obj2._oid()));
		assertEquals(2, removed);

		// Verify documents are removed directly from MongoDB collection
		assertEquals(0, mongoMap.collection.countDocuments(
			Filters.in("_oid", Arrays.asList(obj1._oid(), obj2._oid()))
		));

		// Verify obj3 still exists in MongoDB collection
		assertEquals(1, mongoMap.collection.countDocuments(
			Filters.eq("_oid", obj3._oid())
		));
	}

	/**
	 * Tests MongoDB backend queryIn and queryIn_id using native $in filters.
	 * Verifies matching by custom property as well as document _oid.
	 */
	@Test
	public void testMongoQueryInBasicAndByOid() throws Exception {
		MongoDB_DataObjectMap mongoMap = (MongoDB_DataObjectMap) mtObj;

		DataObject doc1 = mongoMap.newEntry();
		doc1.put("cluster", "prod-us-east");
		doc1.put("tier", "frontend");
		doc1.saveDelta();

		DataObject doc2 = mongoMap.newEntry();
		doc2.put("cluster", "prod-eu-west");
		doc2.put("tier", "backend");
		doc2.saveDelta();

		DataObject doc3 = mongoMap.newEntry();
		doc3.put("cluster", "staging-asia");
		doc3.put("tier", "database");
		doc3.saveDelta();

		// Query for "prod-us-east" and "staging-asia"
		DataObject[] matched = mongoMap.queryIn("cluster", Arrays.asList("prod-us-east", "staging-asia"));
		assertEquals(2, matched.length);

		// Query IDs for "prod-eu-west"
		String[] matchedIds = mongoMap.queryIn_id("cluster", Arrays.asList("prod-eu-west"));
		assertEquals(1, matchedIds.length);
		assertEquals(doc2._oid(), matchedIds[0]);

		// Query by _oid collection
		String[] oidMatches = mongoMap.queryIn_id("_oid", Arrays.asList(doc1._oid(), doc3._oid()));
		assertEquals(2, oidMatches.length);
	}

	/**
	 * Tests MongoDB backend queryIn and queryIn_id with compound additionalWhereQuery.
	 * Verifies that Filters.in is combined with additional BSON filter via Filters.and.
	 */
	@Test
	public void testMongoQueryInWithAdditionalWhere() throws Exception {
		MongoDB_DataObjectMap mongoMap = (MongoDB_DataObjectMap) mtObj;

		DataObject doc1 = mongoMap.newEntry();
		doc1.put("region", "apac");
		doc1.put("status", "healthy");
		doc1.saveDelta();

		DataObject doc2 = mongoMap.newEntry();
		doc2.put("region", "apac");
		doc2.put("status", "degraded");
		doc2.saveDelta();

		DataObject doc3 = mongoMap.newEntry();
		doc3.put("region", "emea");
		doc3.put("status", "healthy");
		doc3.saveDelta();

		// Query region IN ("apac", "emea") AND status = "healthy"
		DataObject[] healthyDocs = mongoMap.queryIn(
			"region",
			Arrays.asList("apac", "emea"),
			"status = ?",
			new Object[] { "healthy" }
		);
		assertEquals(2, healthyDocs.length);

		// Query IDs for region IN ("apac", "emea") AND status = "degraded"
		String[] degradedIds = mongoMap.queryIn_id(
			"region",
			Arrays.asList("apac", "emea"),
			"status = ?",
			new Object[] { "degraded" }
		);
		assertEquals(1, degradedIds.length);
		assertEquals(doc2._oid(), degradedIds[0]);
	}

	/**
	 * Tests Index management operations on MongoDB_DataObjectMap:
	 * - supportsIndexManagement()
	 * - listIndexes()
	 * - hasIndex()
	 * - hasIndexWithKeys()
	 * - createIndex()
	 */
	@Test
	public void testIndexManagementOperations() throws Exception {
		MongoDB_DataObjectMap mongoMap = (MongoDB_DataObjectMap) mtObj;

		// 1. Backend must declare index management support
		assertTrue("MongoDB backend must support index management", mongoMap.supportsIndexManagement());

		// 2. Default indexes: MongoDB always creates "_id_"
		assertTrue("Collection must have _id_ index", mongoMap.hasIndex("_id_"));

		// 3. Create a compound index
		IndexDefinition customIndex = IndexDefinition.create("test_user_status_idx")
			.ascending("userId")
			.descending("status")
			.build();

		// Initially neither name nor keys should exist
		assertFalse(mongoMap.hasIndex("test_user_status_idx"));
		assertFalse(mongoMap.hasIndexWithKeys(customIndex));

		// Create the index
		mongoMap.createIndex(customIndex);

		// Now both should exist
		assertTrue(mongoMap.hasIndex("test_user_status_idx"));
		assertTrue(mongoMap.hasIndexWithKeys(customIndex));

		// 4. Duplicate call to createIndex should be idempotent (skips creation safely)
		mongoMap.createIndex(customIndex);
		assertTrue(mongoMap.hasIndex("test_user_status_idx"));

		// 5. Another index definition with same keys but different name should also be skipped by hasIndexWithKeys
		IndexDefinition duplicateKeysDiffName = IndexDefinition.create("another_name_same_keys")
			.ascending("userId")
			.descending("status")
			.build();
		mongoMap.createIndex(duplicateKeysDiffName);
		// The new name should not be created because an index with those keys already exists
		assertFalse(mongoMap.hasIndex("another_name_same_keys"));

		// 6. TTL index creation test
		IndexDefinition ttlIndex = IndexDefinition.create("test_session_ttl_idx")
			.ascending("expireAt")
			.expireAfterSeconds(3600L)
			.build();
		mongoMap.createIndex(ttlIndex);
		assertTrue(mongoMap.hasIndex("test_session_ttl_idx"));
		assertTrue(mongoMap.hasIndexWithKeys(ttlIndex));
	}
}
