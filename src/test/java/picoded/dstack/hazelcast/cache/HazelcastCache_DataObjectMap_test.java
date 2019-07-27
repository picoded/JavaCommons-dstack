package picoded.dstack.hazelcast.cache;

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

public class HazelcastCache_DataObjectMap_test extends StructSimple_DataObjectMap_test {
	
	// Hazelcast stack instance
	protected static volatile HazelcastStack instance = null;
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Impomentation constructor
	public DataObjectMap implementationConstructor() {
		
		// Initialize hazelcast server
		synchronized (HazelcastCache_DataObjectMap_test.class) {
			if (instance == null) {
				GenericConvertMap<String, Object> hazelcastConfigMap = new GenericConvertHashMap<>();
				hazelcastConfigMap.put("groupName", "HazelcastCache_DataObjectMap_test");
				hazelcastConfigMap.put("instanceCache", true);
				
				GenericConvertMap<String, Object> stackConfig = new GenericConvertHashMap<>();
				stackConfig.put("name", "HazelcastCache_DataObjectMap_test");
				stackConfig.put("hazelcast", hazelcastConfigMap);
				
				instance = new HazelcastCacheStack(stackConfig);
			}
		}
		
		// Load the DataObjectMap
		return instance.dataObjectMap(JSqlTestConfig.randomTablePrefix());
	}
	
}
