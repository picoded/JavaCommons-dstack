package picoded.dstack.jsql_json;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.*;
import picoded.dstack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.connector.jsql.JSql;
import picoded.dstack.connector.jsql.JSqlType;

/**
 * [Internal use only]
 * 
 * JSQL-JSON configuration based stack provider
 **/
public class JSqlJsonStack extends JSqlStack {
	
	// /**
	//  * The internal JSql connection
	//  */
	// protected JSql conn = null;
	
	/**
	 * Constructor with configuration map
	 */
	public JSqlJsonStack(GenericConvertMap<String, Object> inConfig) {
		// Setup config object
		super(inConfig);
		
		// Validate it
		validateJSqlConnection();
	}
	
	/**
	 * Constructor with jsql object and configuration map
	 */
	public JSqlJsonStack(JSql inConnection, GenericConvertMap<String, Object> inConfig) {
		// Setup connection and config object
		super(inConnection, inConfig);
		
		// Validate it
		validateJSqlConnection();
	}
	
	/**
	 * Validate the JSQL connection, for JSON support
	 */
	private void validateJSqlConnection() {
		if (conn.sqlType() == JSqlType.POSTGRESQL) {
			return;
		}
		
		// Throw if validation faile
		throw new IllegalArgumentException(
			"Invalid JSQL config, JSQL-json currently only support postgress databases");
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
		
		// Fall back to the original support
		return super.initDataStructure(name, type);
	}
}
