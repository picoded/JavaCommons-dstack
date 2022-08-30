package picoded.dstack.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import picoded.core.conv.GenericConvert;
import picoded.core.struct.GenericConvertHashMap;
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
	
	/// Internal self used logger
	private static Logger LOGGER = Logger.getLogger(MongoDBStack.class.getName());
	
	/**
	 * The internal MongoClient connection
	 */
	protected MongoClient client_conn = null;
	protected MongoDatabase db_conn = null;
	
	/**
	 * The secondary connetion settings
	 */
	protected MongoClient sec_client_conn = null;
	protected MongoDatabase sec_db_conn = null;
	protected String sec_mode = null;
	
	//-------------------------------------------------------------------------
	// Connector utilities
	//-------------------------------------------------------------------------
	
	/// Default settings option JSON
	protected static String defaultOptJson = "{" + //
		"	\"w\":\"majority\"," + //
		"	\"retryWrites\":\"true\"," + //
		"	\"retryReads\":\"true\"," + //
		"	\"maxPoolSize\":20," + //
		"	\"compressors\":\"zstd\"" + //
		"}";
	
	// "&readPreference=master&readConcernLevel=majority"
	// "&readPreference=nearest&readConcernLevel=linearizable"
	
	/**
	 * Map the option object to a URI parameter string
	 */
	protected static String mapToOptStr(Map<String, Object> map) {
		// Get the sorted list of keys
		List<String> keys = (new ArrayList<>(map.keySet()));
		Collections.sort(keys);
		
		// The ret stringbuilder
		StringBuilder ret = new StringBuilder();
		
		// Lets loop the keys
		for (String key : keys) {
			if (ret.length() > 0) {
				ret.append("&");
			}
			ret.append(key + "=" + GenericConvert.toString(map.get(key)));
		}
		
		// And return the built str
		return ret.toString();
	}
	
	//-------------------------------------------------------------------------
	// Database connection URL constructor
	//-------------------------------------------------------------------------
	
	/**
	 * Given the mongodb config object, get the full_url
	 */
	public static String getFullConnectionURL_primary(GenericConvertMap<String, Object> config) {
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
		String protocol = config.getString("protocol", "mongodb");
		String user = config.getString("user", null);
		String pass = config.getString("pass", null);
		String host = config.getString("host", "localhost");
		int port = config.getInt("port", 27017);
		
		// Hanlding of option string
		GenericConvertMap<String, Object> optMap = config.getGenericConvertStringMap("opt",
			defaultOptJson);
		String optStr = config.getString("opt_str", mapToOptStr(optMap));
		
		// Lets do a logging, for missing read concern if its not configured
		if (optStr.indexOf("readConcernLevel") < 0) {
			//
			// readConcernLevel is a complicated topic, do consider reading up
			// https://jepsen.io/analyses/mongodb-4.2.6
			// https://www.mongodb.com/blog/post/performance-best-practices-transactions-and-read--write-concerns
			// https://www.mongodb.com/docs/manual/reference/read-concern-linearizable/#mongodb-readconcern-readconcern.-linearizable-
			//
			// This option was removed by default, as an error will be thrown when its applied to single node clusters
			//
			// Unless you know what your doing from a performance standpoint, it is strongly recommended to use 
			// `readConcernLevel=linearizable`
			//
			LOGGER
				.warning("MongoDB is configured without readConcernLevel for the primary connection, "
					+ "this is alright for a single node, but `readConcernLevel=linearizable`"
					+ "or `readPreference=master&readConcernLevel=majority`"
					+ "is highly recommended for replica clusters to ensure read after write consistency.");
		}
		
		// Lets build the auth str
		String authStr = "";
		if (user != null && pass != null) {
			authStr = user + ":" + pass + "@";
		}
		
		// Return the full URL depending on the settings
		if (protocol.equals("mongodb+srv")) {
			// mongodb+srv does not support the port protocol
			return protocol + "://" + authStr + host + "/" + dbname + "?" + optStr;
		}
		return protocol + "://" + authStr + host + ":" + port + "/" + dbname + "?" + optStr;
	}
	
	/**
	 * Given the mongodb config object, get the full_url
	 */
	public static String getFullConnectionURL_secondary(GenericConvertMap<String, Object> config) {
		// Null check for secondary connection
		String sec_mode = config.getString("sec_mode", null);
		if (sec_mode == null) {
			return null;
		}
		
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
		String protocol = config.getString("protocol", "mongodb");
		String user = config.getString("user", null);
		String pass = config.getString("pass", null);
		String host = config.getString("host", "localhost");
		int port = config.getInt("port", 27017);
		
		// Hanlding of option string, default sec_opt uses `secondaryPreferred`
		GenericConvertMap<String, Object> optMap = new GenericConvertHashMap<>();
		optMap.putAll(config.getGenericConvertStringMap("opt", defaultOptJson));
		optMap.putAll(config.getGenericConvertStringMap("sec_opt", "{ \"readPreference\":\"secondaryPreferred\" }"));
		
		// The opt string overwrite
		String optStr = config.getString("sec_opt_str", mapToOptStr(optMap));
		
		// Lets do a logging, for missing read concern if its not configured
		if (optStr.indexOf("readConcernLevel") < 0) {
			//
			// readConcernLevel is a complicated topic, do consider reading up
			// https://jepsen.io/analyses/mongodb-4.2.6
			// https://www.mongodb.com/blog/post/performance-best-practices-transactions-and-read--write-concerns
			// https://www.mongodb.com/docs/manual/reference/read-concern-linearizable/#mongodb-readconcern-readconcern.-linearizable-
			//
			// This option was removed by default, as an error will be thrown when its applied to single node clusters
			//
			// Unless you know what your doing from a performance standpoint, it is strongly recommended to use 
			// `readConcernLevel=linearizable`
			//
			LOGGER
				.warning("MongoDB is configured without readConcernLevel for the secondary connection, "
					+ "this is alright for a single node, but `readConcernLevel=linearizable`"
					+ "or `readPreference=secondaryPreferred&readConcernLevel=majority`"
					+ "is highly recommended for replica clusters to ensure read after write consistency.");
		}
		
		// Lets build the auth str
		String authStr = "";
		if (user != null && pass != null) {
			authStr = user + ":" + pass + "@";
		}
		
		// Return the full URL depending on the settings
		if (protocol.equals("mongodb+srv")) {
			// mongodb+srv does not support the port protocol
			return protocol + "://" + authStr + host + "/" + dbname + "?" + optStr;
		}
		return protocol + "://" + authStr + host + ":" + port + "/" + dbname + "?" + optStr;
	}
	
	//-------------------------------------------------------------------------
	// Database connection setup
	//-------------------------------------------------------------------------
	
	/**
	 * Utility library, used to check that the connection "works"
	 * @param db_conn
	 */
	protected void checkMongoDatabaseConnection(MongoDatabase db_conn) {
		
		// Safety check, get the list of connection names
		// this should throw an error if its not connected
		Iterable<String> list = db_conn.listCollectionNames();
		for (String n : list) {
			// does nothing
		}
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
		
		// Primary connection
		// ------
		
		// Get the full_url
		String full_url = getFullConnectionURL_primary(inConfig);
		
		// Lets build using the stable API settings
		ServerApi serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();
		MongoClientSettings settings = MongoClientSettings.builder()
			.applyConnectionString(new ConnectionString(full_url)).serverApi(serverApi).build();
		
		// Get the connection & database
		client_conn = MongoClients.create(settings);
		
		// Get the DB connection, and validate it
		db_conn = client_conn.getDatabase(dbConfig.fetchString("name"));
		checkMongoDatabaseConnection(db_conn);
		
		// Secondary connection
		// ------
		
		// Null check for secondary connection
		String config_sec_mode = config.getString("sec_mode", null);
		if (config_sec_mode == null) {
			return;
		}
		sec_mode = config_sec_mode.trim().toUpperCase();
		
		// lets get the secondary connection
		full_url = getFullConnectionURL_secondary(inConfig);
		serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();
		settings = MongoClientSettings.builder()
			.applyConnectionString(new ConnectionString(full_url)).serverApi(serverApi).build();
		
		// Get the connection & database
		client_conn = MongoClients.create(settings);
		
		// Get the DB connection, and validate it
		sec_db_conn = client_conn.getDatabase(dbConfig.fetchString("name"));
		checkMongoDatabaseConnection(sec_db_conn);
		
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
		Core_DataStructure ret = null;
		if (type.equalsIgnoreCase("DataObjectMap")) {
			ret = new MongoDB_DataObjectMap(this, name);
		} else if (type.equalsIgnoreCase("KeyValueMap")) {
			ret = new MongoDB_KeyValueMap(this, name);
		} else if (type.equalsIgnoreCase("KeyLongMap")) {
			ret = new MongoDB_KeyLongMap(this, name);
		} else if (type.equalsIgnoreCase("FileWorkspaceMap")) {
			ret = new MongoDB_FileWorkspaceMap(this, name);
		}
		
		// If datastrucutre initialized, setup name
		if (ret != null) {
			ret.configMap().put("name", name);
		}
		
		// Return the initialized object (or null)
		return ret;
	}
}
