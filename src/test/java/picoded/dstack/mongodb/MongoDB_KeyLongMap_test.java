package picoded.dstack.mongodb;

import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

/**
 * ## Purpose
 * This class is meant to test the MongoDB_KeyLongMap implementation,
 * and ensure that it passes all the test layed out in StructSImple_KeyLongMap_test
 * 
 */
public class MongoDB_KeyLongMap_test extends StructSimple_KeyLongMap_test {
	
	// Hazelcast stack instance
	protected static volatile MongoDBStack instance = null;
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Impomentation constructor
	public KeyLongMap implementationConstructor() {
		
		// Initialize server
		synchronized (MongoDB_KeyLongMap_test.class) {
			if (instance == null) {
				// The default config uses localhost, 27017
				GenericConvertMap<String, Object> mongodbConfig = new GenericConvertHashMap<>();
				mongodbConfig.put("host", DStackTestConfig.MONGODB_HOST());
				mongodbConfig.put("port", DStackTestConfig.MONGODB_PORT());
				
				// Use a random DB name
				mongodbConfig.put("name", DStackTestConfig.randomTablePrefix());
				
				GenericConvertMap<String, Object> stackConfig = new GenericConvertHashMap<>();
				stackConfig.put("name", "MongoDB_KeyLongMap_test");
				stackConfig.put("mongodb", mongodbConfig);
				
				instance = new MongoDBStack(stackConfig);
			}
		}
		
		// Load the KeyLongMap
		return instance.keyLongMap(DStackTestConfig.randomTablePrefix());
	}
	
}
