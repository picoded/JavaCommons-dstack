package picoded.dstack.hazelcast;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.*;
import picoded.dstack.*;
import picoded.dstack.connector.hazelcast.*;

import java.util.HashMap;
import com.hazelcast.core.*;

/**
 * [Internal use only]
 * 
 * Hazelcast configuration based stack provider
 **/
public class HazelcastStack extends CoreStack {
	
	/**
	 * The internal JSql connection
	 */
	protected HazelcastInstance conn = null;
	
	/**
	 * Constructor with configuration map
	 */
	public HazelcastStack(GenericConvertMap<String, Object> inConfig) {
		super(inConfig);
		
		// If name config is missing, throw
		String name = inConfig.getString("name");
		if (name == null) {
			throw new IllegalArgumentException(
				"Missing 'name' config object for Hazelcast stack provider");
		}
		
		// Extract the hazelcast connection config object
		GenericConvertMap<String, Object> hazelcastConfig = inConfig.fetchGenericConvertStringMap(
			"hazelcast", new HashMap<String, Object>());
		
		// If groupName is not set, configure using the stack name
		if (hazelcastConfig.getString("groupName") == null) {
			hazelcastConfig.put("groupName", name);
		}
		
		// Get the Hazelcast connection
		conn = HazelcastConnector.getConnection(hazelcastConfig);
	}
	
	/**
	 * Initilize and return the requested data structure with the given name or type if its supported
	 * 
	 * @param  name  name of the datastructure to initialize
	 * @param  type  implmentation type (KeyValueMap / KeyLongMap / DataObjectMap / FileWorkspaceMap)
	 * 
	 * @return initialized data structure if type is supported
	 */
	protected Core_DataStructure initDataStructure(String name, String type) {
		// Initialize for the respective type
		if (type.equalsIgnoreCase("DataObjectMap")) {
			return new Hazelcast_DataObjectMap(conn, name);
		}
		if (type.equalsIgnoreCase("KeyValueMap")) {
			return new Hazelcast_KeyValueMap(conn, name);
		}
		// No valid type, return null
		return null;
	}
}
