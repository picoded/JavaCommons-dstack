package picoded.dstack.jsql;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.*;
import picoded.dstack.*;
import picoded.dstack.connector.jsql.JSql;

/**
 * [Internal use only]
 * 
 * JSQL configuration based stack provider
 **/
public class JSqlStack extends CoreStack {
	
	/**
	 * The internal JSql connection
	 */
	protected JSql conn = null;
	
	/**
	 * Constructor with configuration map
	 */
	public JSqlStack(GenericConvertMap<String, Object> inConfig) {
		super(inConfig);
		
		// Extract the connection config object
		GenericConvertMap<String, Object> dbConfig = inConfig.fetchGenericConvertStringMap("db");
		
		// If DB config is missing, throw
		if (dbConfig == null) {
			throw new IllegalArgumentException("Missing 'db' config object for JSql stack provider");
		}
		
		// Get the JSql connection
		conn = JSql.setupFromConfig(dbConfig);
	}
	
	/**
	 * Constructor with jsql object and configuration map
	 */
	public JSqlStack(JSql inConnection, GenericConvertMap<String, Object> inConfig) {
		// Setup config object - without throwing for missing DB object
		super(inConfig);
		
		// Get the JSql connection
		conn = inConnection;
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
			// Get the respective config
			GenericConvertMap<String, Object> dataObjectMapConfig = config
				.fetchGenericConvertStringMap("dataObjectMap", "{}");
			GenericConvertMap<String, Object> tableConfig = dataObjectMapConfig
				.fetchGenericConvertStringMap(name);
			
			// Initialize with (or without) config
			return new JSql_DataObjectMap(conn, name, tableConfig);
		}
		if (type.equalsIgnoreCase("KeyValueMap")) {
			return new JSql_KeyValueMap(conn, name);
		}
		if (type.equalsIgnoreCase("KeyLongMap")) {
			return new JSql_KeyLongMap(conn, name);
		}
		if (type.equalsIgnoreCase("FileWorkspaceMap")) {
			return new JSql_FileWorkspaceMap(conn, name);
		}
		// No valid type, return null
		return null;
	}
}
