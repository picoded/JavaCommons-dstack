package picoded.dstack.mongodb;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.*;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.MongoClientSettings;
import com.mongodb.ConnectionString;

/**
 * [Internal use only]
 * 
 * StructCache configuration based stack provider
 **/
public class MongoDBStack extends CoreStack {
	
	/**
	 * The internal MongoClient connection
	 */
	protected MongoClient client_conn = null;
	protected MongoDatabase db_conn = null;
	
	//-------------------------------------------------------------------------
	// Database connection constructor
	//-------------------------------------------------------------------------
	
	/**
	 * Given the mongodb config object, get the full_url
	 */
	public static String getFullConnectionURL(GenericConvertMap<String, Object> config) {
		// Get the DB name (required)
		String dbname = config.getString("name", null);
		if (dbname == null || dbname.isEmpty()) {
			throw new IllegalArgumentException("Missing database 'name' for mongodb config");
		}
		
		// Get the full connection url, and use it if present
		String full_url = config.getString("full_url", null);
		if (full_url != null) {
			return full_url;
		}
		
		// Lets get the config respectively
		String protocol = config.getString("protocol", "mongodb+srv");
		String user = config.getString("user", null);
		String pass = config.getString("pass", null);
		String host = config.getString("host", "localhost");
		int port = config.getInt("port", 27017);
		String opts = config.getString("opt_str",
			"r=majority&w=majority&retryWrites=true&maxPoolSize=50");
		
		// In the future we may want to support opt_map
		// this should still support the default read/write "majority" concern.
		// GenericConvertMap<String,Object> optMap = config.getGenericConvertStringMap("opt_map", "{}");
		
		// Lets build the auth str
		String authStr = "";
		if (user != null && pass != null) {
			authStr = user + ":" + pass + "@";
		}
		
		// Return the full URL without user & pass
		return protocol + "://" + authStr + host + ":" + port + "/" + dbname + "?" + opts;
	}
	
	/**
	 * Given the mongodb config object, get the MongoClient connection
	 */
	public static MongoClient setupFromConfig(GenericConvertMap<String, Object> inConfig) {
		// Get the full_url
		String full_url = getFullConnectionURL(inConfig);
		
		// Lets build using the stable API settings
		ServerApi serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();
		MongoClientSettings settings = MongoClientSettings.builder()
			.applyConnectionString(new ConnectionString(full_url)).serverApi(serverApi).build();
		
		// Create the client, and return it
		return MongoClients.create(settings);
	}
	
	/**
	 * Constructor with configuration map
	 */
	public MongoDBStack(GenericConvertMap<String, Object> inConfig) {
		super(inConfig);
		
		// Extract the connection config object
		GenericConvertMap<String, Object> dbConfig = inConfig.fetchGenericConvertStringMap("mongodb");
		
		// If DB config is missing, throw an error
		if (dbConfig == null) {
			throw new IllegalArgumentException(
				"Missing 'mongodb' config object for MongoDB stack provider");
		}
		
		// Get the connection & database
		client_conn = setupFromConfig(dbConfig);
		
		// Get the DB conneciton
		db_conn = client_conn.getDatabase(inConfig.getString("name"));
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
		// // Initialize for the respective type
		// Core_DataStructure ret = null;
		// if (type.equalsIgnoreCase("DataObjectMap")) {
		// 	ret = new StructCache_DataObjectMap();
		// }
		
		// // If datastrucutre initialized, setup name
		// if (ret != null) {
		// 	ret.configMap().put("name", name);
		// 	return ret;
		// }
		
		// No valid type, return null
		return null;
	}
}
