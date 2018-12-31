package picoded.dstack.connector;

import picoded.core.exception.ExceptionMessage;
import picoded.core.struct.*;
import picoded.dstack.connector.jsql.JSql;

/**
 * JSQL connector loaded
 */
public class JSqlConnector {
	
	//----------------------------------------------------
	//
	//  getConnection / closeConnection implementation
	//
	//----------------------------------------------------
	
	/**
	 * Load from caches a hazelcast provider connnection.
	 * And initializes one if needed
	 * 
	 * @param  configMap config used to initialize the client / server respectively
	 */
	public static JSql getConnection(GenericConvertMap<String, Object> configMap) {
		// Mandatory null argument check
		if (configMap == null) {
			throw new IllegalArgumentException(ExceptionMessage.unexpectedNullArgument);
		}
		
		// Setup JSQL connection  with config
		return JSql.setupFromConfig(configMap);
	}
	
	/**
	 * Close an existing connection (if it exists)
	 * 
	 * @param  groupName used for the cluster
	 */
	public static void closeConnection(JSql connection) {
		
		// Mandatory null argument check
		if (connection == null) {
			throw new IllegalArgumentException(ExceptionMessage.unexpectedNullArgument);
		}
		
		// Close the connection
		connection.close();
	}
	
}