package picoded.dstack.stack;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import picoded.core.struct.GenericConvertHashMap;
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.GenericConvertMap;
import picoded.dstack.struct.simple.StructSimpleStack;
import picoded.dstack.core.CoreStack;
import picoded.dstack.jsql.JSqlStack;

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
	
	/**
	 * Load the provider config with provider list
	 **/
	public ProviderConfig(List<Object> inConfigList) {
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
	protected Map<String, GenericConvertMap<String, Object>> providerConfigMap = new HashMap<>();;
	
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
	protected Map<String, CoreStack> providerStackMap = new HashMap<>();
	
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
		
		// Cache not found, get config to initialize a new stack
		GenericConvertMap<String, Object> providerConfig = getStackConfig(name);
		if (providerConfig == null) {
			throw new IllegalArgumentException("Unknown provider name, config not found : " + name);
		}
		
		// Initialization of stack and store into cache
		cache = initStack(providerConfig.getString("type"), providerConfig);
		providerStackMap.put(name, cache);
		
		// Return result
		return cache;
	}
	
	/**
	 * Initialize a new stack object based on the given type
	 * 
	 * @param  type of stack to initialize ( StructSimple / JSql / JConfig / Stack )
	 */
	protected CoreStack initStack(String type, GenericConvertMap<String, Object> config) {
		if (type.equalsIgnoreCase("StructSimple")) {
			return new StructSimpleStack(config);
		}
		if (type.equalsIgnoreCase("JSql")) {
			return new JSqlStack(config);
		}
		throw new IllegalArgumentException("Unknown stack configuration type : " + type);
	}
	
}