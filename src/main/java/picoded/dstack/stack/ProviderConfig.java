package picoded.dstack.stack;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import picoded.core.conv.ConvertJSON;
import picoded.core.struct.GenericConvertHashMap;
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.CoreStack;

/**
 * [Internal use only]
 *
 * Utility class used to read the provider configruation list,
 * and generate the provider specific data stacks in the process.
 *
 * This takes in a list of provider config, for example (in json form)
 *
 * @TODO
 * + Read-Only support
 * + Caching / Query priority support
 *
 * ```
 * [
 * 	{
 * 		"type" : "StructSimple",
 * 		"name" : "requestCache"
 * 	},
 * 	{
 * 		"type" : "jsql",
 * 		"name" : "db_a",
 * 		"db" : {
 * 			"type" : "sqlite",
 * 			"path" : "./shared.db"
 * 		}
 * 	}
 * ]
 * ```
 */
public class ProviderConfig {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	// Logger to use, for config file warnings
	private static final Logger LOGGER = Logger.getLogger(ProviderConfig.class.getName());
	
	/**
	 * Load the provider config with provider list
	 **/
	public ProviderConfig(List<Object> inConfigList) {
		providerConfigMap = new HashMap<>();
		providerStackMap = new ConcurrentHashMap<>();
		loadConfigArray(inConfigList);
	}
	
	//--------------------------------------------------------------------------
	//
	// Config loading
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Stores the internal config mapping by its name
	 **/
	protected final Map<String, GenericConvertMap<String, Object>> providerConfigMap;
	
	/**
	 * Loads a configuration array of backend providers for dstack, into the provider map
	 *
	 * @param  inConfigList of providers
	 **/
	protected void loadConfigArray(List<Object> inConfigList) {
		// Map it as a generic list
		GenericConvertList<Object> configList = GenericConvertList.build(inConfigList);
		
		// Iterate each config item
		int size = configList.size();
		for (int i = 0; i < size; ++i) {
			// Get the provider config object
			GenericConvertMap<String, Object> config = configList.getGenericConvertStringMap(i, null);
			
			// Validate the provider object
			if (config == null) {
				throw new IllegalArgumentException("Missing provider config at index : " + i);
			}
			if (config.getString("type") == null) {
				throw new IllegalArgumentException("Missing provider type at index : " + i);
			}
			if (config.getString("name") == null) {
				throw new IllegalArgumentException("Missing provider name at index : " + i);
			}
			
			// Add to provider config map
			providerConfigMap.put(config.getString("name"), config);
		}
	}
	
	/**
	 * Get and return the config selected by name
	 *
	 * @param  name of config to get
	 *
	 * @return  config map if found
	 */
	protected GenericConvertMap<String, Object> getStackConfig(String name) {
		return providerConfigMap.get(name);
	}
	
	//--------------------------------------------------------------------------
	//
	// Provider stack handling
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Stores the respective stack providers
	 */
	protected final ConcurrentHashMap<String, CoreStack> providerStackMap;
	
	/**
	 * Get the stack of the provider specified by the name,
	 * initializing the stack if necessary
	 *
	 * @param name - Name of the provider
	 *
	 * @return the stack provider if found
	 */
	public CoreStack getProviderStack(String name) {
		// Get and return from cache if found
		CoreStack cache = providerStackMap.get(name);
		if (cache != null) {
			return cache;
		}
		
		synchronized (this) {
			// Check the cache again (avoid race condition)
			cache = providerStackMap.get(name);
			if (cache != null) {
				return cache;
			}
			
			// Cache not found, get config to initialize a new stack
			GenericConvertMap<String, Object> providerConfig = getStackConfig(name);
			if (providerConfig == null) {
				LOGGER.log(Level.SEVERE, "Unknown provider name, config not found : " + name);
				throw new IllegalArgumentException("Unknown provider name, config not found : " + name);
			}
			
			// Log the setup
			String type = providerConfig.getString("type");
			LOGGER.info("Setting DStack provider backend : " + name + " (" + type + ")");
			
			// Initialization of stack and store into cache
			try {
				cache = initStack(type, providerConfig);
			} catch (Exception e) {
				// Log the error, as this is easily missed into an API error
				LOGGER.log(Level.SEVERE, "Error while setting DStack provider : " + name + " (" + type
					+ ")", e);
				throw new RuntimeException(e);
			}
			
			// Save it into cache
			providerStackMap.put(name, cache);
			
			// Return result
			return cache;
		}
	}
	
	/**
	 * Initialize a new stack object based on the given type
	 *
	 * @param  type of stack to initialize ( StructSimple / JSql / JConfig / Stack )
	 */
	protected CoreStack initStack(String type, GenericConvertMap<String, Object> config) {
		if (type.equalsIgnoreCase("StructSimple")) {
			return new picoded.dstack.struct.simple.StructSimpleStack(config);
		}
		if (type.equalsIgnoreCase("StructCache")) {
			return new picoded.dstack.struct.cache.StructCacheStack(config);
		}
		if (type.equalsIgnoreCase("JSql")) {
			return new picoded.dstack.jsql.JSqlStack(config);
		}
		if (type.equalsIgnoreCase("MongoDB")) {
			return new picoded.dstack.mongodb.MongoDBStack(config);
		}
		if (type.equalsIgnoreCase("HazelcastStore")) {
			return new picoded.dstack.hazelcast.store.HazelcastStoreStack(config);
		}
		if (type.equalsIgnoreCase("HazelcastCache")) {
			return new picoded.dstack.hazelcast.cache.HazelcastCacheStack(config);
		}
		if (type.equalsIgnoreCase("FileSimple")) {
			return new picoded.dstack.file.simple.FileSimpleStack(config);
		}
		if (type.equalsIgnoreCase("FileLayered")) {
			return new picoded.dstack.file.layered.FileLayeredStack(config);
		}
		
		// Deprecated type errors
		if (type.equalsIgnoreCase("Hazelcast")) {
			throw new IllegalArgumentException(
				"Hazlecast backend is deprecated, please use HazelcastStore or HazelcastCache instead (to better reflect their respective use case)");
		}
		
		// Unknown type error
		throw new IllegalArgumentException("Unknown stack configuration type : " + type);
	}
	
}
