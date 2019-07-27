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

public class HazelcastStore_KeyLongMap_test extends StructSimple_KeyLongMap_test {
	
	// Hazelcast stack instance
	protected static volatile HazelcastStack instance = null;
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Impomentation constructor
	public KeyLongMap implementationConstructor() {
		
		// Initialize hazelcast server
		synchronized (HazelcastStore_KeyLongMap_test.class) {
			if (instance == null) {
				GenericConvertMap<String, Object> hazelcastConfigMap = new GenericConvertHashMap<>();
				hazelcastConfigMap.put("groupName", "HazelcastStore_KeyLongMap_test");
				hazelcastConfigMap.put("instanceCache", true);
				
				GenericConvertMap<String, Object> stackConfig = new GenericConvertHashMap<>();
				stackConfig.put("name", "HazelcastStore_KeyLongMap_test");
				stackConfig.put("hazelcast", hazelcastConfigMap);
				
				instance = new HazelcastStoreStack(stackConfig);
			}
		}
		
		// Load the KeyLongMap
		return instance.keyLongMap(JSqlTestConfig.randomTablePrefix());
	}
}
