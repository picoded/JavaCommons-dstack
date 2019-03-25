package picoded.dstack.hazelcast;

import java.util.HashMap;

import com.hazelcast.core.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.*;

import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.jsql.JSqlTestConfig;
import picoded.dstack.struct.simple.*;
import picoded.dstack.connector.hazelcast.*;

public class Hazelcast_KeyValueMap_test extends StructSimple_KeyValueMap_test {
	
	// Hazelcast instance
	protected static volatile HazelcastInstance instance = null;
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Impomentation constructor
	public KeyValueMap implementationConstructor() {
		
		// Initialize hazelcast server
		synchronized (Hazelcast_KeyValueMap_test.class) {
			if (instance == null) {
				GenericConvertMap<String, Object> configMap = new GenericConvertHashMap<>();
				configMap.put("groupName", "Hazelcast_KeyValueMap_test");
				instance = HazelcastConnector.getConnection(configMap);
			}
		}
		
		// Load the KeyValueMap
		Hazelcast_KeyValueMap map = new Hazelcast_KeyValueMap( //
			instance, //
			JSqlTestConfig.randomTablePrefix() //
		); //
		return map;
	}
}
