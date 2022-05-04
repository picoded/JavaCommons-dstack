package picoded.dstack;

import picoded.dstack.jsql.JSqlTestConfig;

/**
 * DStackTest configurations
 * 
 * This extends JSqlTestConfig, for backwards compatibility for all JSQL tests.
 * 
 * This class is designed to allow the config to be easily "extendable" in the future
 * with potential env variable overwrites (if its ever needed)
 * 
 * All non "SQL" backend should be using this for their configurations.
 */
public class DStackTestConfig extends JSqlTestConfig {
	
	//----------------------------------//
	// Default Credentials for MONGODB  //
	//----------------------------------//
	static private String MONGODB_HOST = "127.0.0.1";
	static private int MONGODB_PORT = 27017;
	
	static public String MONGODB_HOST() {
		return MONGODB_HOST;
	}
	
	static public int MONGODB_PORT() {
		return MONGODB_PORT;
	}
	
}