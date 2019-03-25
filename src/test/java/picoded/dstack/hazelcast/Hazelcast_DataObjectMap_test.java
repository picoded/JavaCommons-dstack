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

public class Hazelcast_DataObjectMap_test extends StructSimple_DataObjectMap_test {
	
	// Hazelcast instance
	protected static volatile HazelcastInstance instance = null;
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Impomentation constructor
	public DataObjectMap implementationConstructor() {
		
		// Initialize hazelcast server
		synchronized (Hazelcast_DataObjectMap_test.class) {
			if (instance == null) {
				GenericConvertMap<String, Object> configMap = new GenericConvertHashMap<>();
				configMap.put("groupName", "Hazelcast_DataObjectMap_test");
				instance = HazelcastConnector.getConnection(configMap);
			}
		}
		
		// Load the DataObjectMap
		Hazelcast_DataObjectMap map = new Hazelcast_DataObjectMap( //
			instance, //
			JSqlTestConfig.randomTablePrefix() //
		); //
		return map;
	}
	
}
