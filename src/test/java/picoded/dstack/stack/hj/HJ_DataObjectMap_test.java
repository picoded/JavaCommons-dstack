package picoded.dstack.stack.hj;

// Hazelcast dependencies
import com.hazelcast.core.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.*;

// Test depends
import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.core.*;
import picoded.dstack.stack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.hazelcast.*;
import picoded.dstack.connector.jsql.*;
import picoded.dstack.connector.hazelcast.*;

/**
 * Test varient of 2 layer system of hazelcast and jsql
 */
public class HJ_DataObjectMap_test extends Stack_DataObjectMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	// Note that this SQL connector constructor
	// is to be overriden for the various backend
	// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.sqlite();
	}
	
	// Hazelcast instance
	protected static volatile HazelcastInstance instance = null;
	
	// Get the HazelcastInstance
	protected HazelcastInstance hazelcastInstance() {
		// Initialize hazelcast server
		synchronized (HJ_DataObjectMap_test.class) {
			if (instance == null) {
				GenericConvertMap<String, Object> configMap = new GenericConvertHashMap<>();
				configMap.put("groupName", "HJ_DataObjectMap_test");
				instance = HazelcastConnector.getConnection(configMap);
			}
			return instance;
		}
	}
	
	/// Impimentation constructor for stack setup
	public DataObjectMap implementationConstructor() {
		String tablePrefix = JSqlTestConfig.randomTablePrefix();
		layer1 = new Hazelcast_DataObjectMap(hazelcastInstance(), tablePrefix);
		layer2 = new JSql_DataObjectMap(jsqlConnection(), tablePrefix);
		return new Stack_DataObjectMap(new Core_DataObjectMap[] { layer1, layer2 });
	}
}