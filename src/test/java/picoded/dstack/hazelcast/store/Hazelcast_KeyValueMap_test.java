package picoded.dstack.hazelcast.store;

import java.util.HashMap;

import com.hazelcast.core.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.*;

import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.jsql.JSqlTestConfig;
import picoded.dstack.struct.simple.*;
import picoded.dstack.connector.hazelcast.*;
import picoded.dstack.hazelcast.core.*;

public class Hazelcast_KeyValueMap_test extends StructSimple_KeyValueMap_test {
	
	// Hazelcast stack instance
	protected static volatile HazelcastStack instance = null;
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Impomentation constructor
	public KeyValueMap implementationConstructor() {
		
		// Initialize hazelcast server
		synchronized (Hazelcast_KeyValueMap_test.class) {
			if (instance == null) {
				GenericConvertMap<String, Object> hazelcastConfigMap = new GenericConvertHashMap<>();
				hazelcastConfigMap.put("groupName", "Hazelcast_KeyValueMap_test");
				hazelcastConfigMap.put("instanceCache", true);
				
				GenericConvertMap<String, Object> stackConfig = new GenericConvertHashMap<>();
				stackConfig.put("name", "Hazelcast_KeyValueMap_test");
				stackConfig.put("hazelcast", hazelcastConfigMap);
				
				instance = new HazelcastStoreStack(stackConfig);
			}
		}
		
		// Load the KeyValueMap
		return instance.keyValueMap(JSqlTestConfig.randomTablePrefix());
	}
}
