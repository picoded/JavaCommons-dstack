package picoded.dstack.redis;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.*;

import org.redisson.Redisson;
import org.redisson.config.Config;
import org.redisson.api.RedissonClient;

/**
 * [Internal use only]
 * 
 * StructCache configuration based stack provider
 **/
public class RedisStack extends CoreStack {
	
	/**
	 * The internal Redis connection
	 */
	
	protected RedissonClient conn = null;
	
	//-------------------------------------------------------------------------
	// Database connection constructor
	//-------------------------------------------------------------------------
	
	/**
	 * Given the redis config object, get the full_url
	 * https://github.com/lettuce-io/lettuce-core/wiki/Redis-URI-and-connection-details
	 */
	public static String getFullConnectionURL(GenericConvertMap<String, Object> config) {
		// Get the DB name (required)
		
		// Redis db are labelled from 0 to 15
		String dbname = config.getString("name", null);
		if (dbname == null || dbname.isEmpty()) {
			throw new IllegalArgumentException("Missing database 'name' for redis config");
		}
		
		// Get the full connection url, and use it if present
		String full_url = config.getString("full_url", null);
		if (full_url != null) {
			return full_url;
		}
		
		// Lets get the config respectively
		String protocol = config.getString("protocol", "redis");
		String user = config.getString("user", null);
		String pass = config.getString("pass", null);
		String host = config.getString("host", "172.17.0.1"); //default docker ip 
		int port = config.getInt("port", 6379); //default redis port
		String opts = config.getString("opt_str",
			"r=majority&w=majority&retryWrites=true&maxPoolSize=50");
		
		// Lets build the auth str
		String authStr = "";
		if (user != null && pass != null) {
			authStr = user + ":" + pass + "@";
		}
		
		return protocol + "://" + authStr + host + ":" + port + "/" + dbname + "?" + opts;
	}
	
	/**
	 * Given the redis config object, get the Redisson client connection
	 */
	public static RedissonClient setupFromConfig(GenericConvertMap<String, Object> inConfig) {
		// Get the full_url
		String full_url = getFullConnectionURL(inConfig);
		System.out.println(full_url);
		
		// Apply config
		Config config = new Config();
		config.useSingleServer().setAddress(full_url);
		
		// Create the client, and return it
		return Redisson.create(config);
	}
	
	/**
	 * Constructor with configuration map
	 */
	public RedisStack(GenericConvertMap<String, Object> inConfig) {
		//https://github.com/redisson/redisson/wiki/10.-additional-features#107-low-level-redis-client
		super(inConfig);
		
		// Extract the connection config object
		GenericConvertMap<String, Object> dbConfig = inConfig.fetchGenericConvertStringMap("redis");
		
		// If DB config is missing, throw an error
		if (dbConfig == null) {
			throw new IllegalArgumentException(
				"Missing 'RedisStack' config object for Redis stack provider");
		}
		
		// Get the connection & database
		conn = setupFromConfig(dbConfig);
	}
	
	//--------------------------------------------------------------------------
	//
	// Internal package methods
	//
	//--------------------------------------------------------------------------
	
	/**
	 * @return pong
	 */
	protected String ping() {
		return "pong";
	}
	
	/**
	 * @return the internal hazelcastInstance connection
	 */
	protected RedissonClient getConnection() {
		return conn;
	}
	
	/**
	 * Initialize and return the requested data structure with the given name or type if its supported
	 * 
	 * @param  name  name of the datastructure to initialize
	 * @param  type  implmentation type (KeyValueMap / KeyLongMap / DataObjectMap / FileWorkspaceMap)
	 * 
	 * @return initialized data structure if type is supported
	 */
	protected Core_DataStructure initDataStructure(String name, String type) {
		// Initialize for the respective type
		Core_DataStructure ret = null;
		if (type.equalsIgnoreCase("DataObjectMap")) {
			ret = new Redis_DataObjectMap(this, name);
		}
		// If datastrucutre initialized, setup name
		if (ret != null) {
			ret.configMap().put("name", name);
		}
		// Return the initialized object (or null)
		return ret;
	}
}
