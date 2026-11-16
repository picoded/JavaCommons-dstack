package picoded.dstack.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
}
