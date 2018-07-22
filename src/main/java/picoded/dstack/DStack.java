package picoded.dstack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.RuntimeErrorException;

import picoded.core.conv.ArrayConv;
import picoded.core.conv.GenericConvert;
import picoded.core.struct.GenericConvertHashMap;
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.CoreStack;
import picoded.dstack.core.Core_DataStructure;
import picoded.dstack.core.Core_FileWorkspaceMap;
import picoded.dstack.core.Core_KeyLong;
import picoded.dstack.core.Core_KeyLongMap;
import picoded.dstack.core.Core_KeyValueMap;
import picoded.dstack.core.Core_DataObjectMap;
import picoded.dstack.stack.ProviderConfig;
import picoded.dstack.stack.Stack_DataObjectMap;
import picoded.dstack.stack.Stack_FileWorkspaceMap;
import picoded.dstack.stack.Stack_KeyLongMap;
import picoded.dstack.stack.Stack_KeyValueMap;

public class DStack extends CoreStack {
	
	// List of provider backends - to fetch / initialize from
	protected ProviderConfig providerConfig;
	
	// Namespace listing
	protected GenericConvertList<Object> namespace;
	
	/**
	 * Constructor with configuration map
	 */
	public DStack(GenericConvertMap<String, Object> inConfig) {
		super(inConfig);
		
		GenericConvertList<Object> providerConfigList = inConfig.fetchGenericConvertList("provider");
		if (providerConfigList == null) {
			throw new IllegalArgumentException("Missing `provider` config list");
		}
		
		providerConfig = new ProviderConfig(providerConfigList);
		namespace = inConfig.fetchGenericConvertList("namespace");
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
			return new Stack_DataObjectMap(fetchCommonStructureImplementation(name, "DataObjectMap",
				new Core_DataObjectMap[] {}));
		}
		if (type.equalsIgnoreCase("KeyValueMap")) {
			return new Stack_KeyValueMap(fetchCommonStructureImplementation(name, "KeyValueMap",
				new Core_KeyValueMap[] {}));
		}
		if (type.equalsIgnoreCase("KeyLongMap")) {
			return new Stack_KeyLongMap(fetchCommonStructureImplementation(name, "KeyLongMap",
				new Core_KeyLongMap[] {}));
		}
		if (type.equalsIgnoreCase("FileWorkspaceMap")) {
			return new Stack_FileWorkspaceMap(fetchCommonStructureImplementation(name,
				"FileWorkspaceMap", new Core_FileWorkspaceMap[] {}));
		}
		
		// No valid type supported
		return null;
	}
	
	/**
	 * Given the data structure name, and string type. Get the relevent underlying data structure implmentation.
	 * 
	 * @param  name of data structure to use
	 * @param  type of data structure to implement
	 * @param  refrenceType return array to use for type reference (not actually used)
	 * 
	 * @return  array of data structures found applicable, which is the same as referenceType
	 */
	protected <V extends Core_DataStructure> V[] fetchCommonStructureImplementation(String name,
		String type, V[] refrenceType) {
		// Get the relevent namespace config
		GenericConvertMap<String, Object> namespaceConfig = resolveNamespaceConfig(name);
		if (namespaceConfig == null) {
			throw new RuntimeException("No `namespace` configuration found for " + name);
		}
		
		// Get the provider list
		List<Object> providerList = namespaceConfig.getObjectList("providers");
		if (providerList == null) {
			throw new RuntimeException("No `providers` found in namespaceConfig for " + name);
		}
		
		// return list to use, time to fill it up with objects from the providers
		List<V> retList = new ArrayList<>();
		
		// Iterate the provider
		for (Object provider : providerList) {
			// Get the relevent provider CoreStack
			// Skip if null
			CoreStack providerStack = providerConfig.getProviderStack(provider.toString());
			if (providerStack == null) {
				continue;
			}
			
			// Get the relevent data structure
			// Skip if null
			Core_DataStructure providerDataStructure = providerStack.cacheDataStructure(name, type,
				null);
			if (providerDataStructure == null) {
				continue;
			}
			
			// Add to response
			retList.add((V) providerDataStructure);
		}
		
		// Throw an exception if empty
		if (retList.isEmpty()) {
			throw new RuntimeException("No `providers` returned a valid DataStructure");
		}
		
		// returning as array
		return retList.toArray(refrenceType);
	}
	
	//
	// Helper Functions
	//
	
	/**
	 * This function will find the first namespaceConfig that matches the requested name
	 * 
	 * @param name of the object to be searched
	 * 
	 * @return the configuration or null
	 */
	protected GenericConvertMap<String, Object> resolveNamespaceConfig(String name) {
		for (Object object : namespace) {
			GenericConvertMap<String, Object> namespaceConfig = GenericConvert
				.toGenericConvertStringMap(object);
			String pattern = namespaceConfig.getString("regex", "");
			if (regexNameMatcher(name, pattern)) {
				return namespaceConfig;
			}
		}
		return null;
	}
	
	/**
	 * Attempts to match the name with the regex pattern given in the param
	 * 
	 * @param nameToMatch is the string to check if it matches with the pattern
	 * @param pattern     is the `regex` that is set in the namespace 
	 * 
	 * @return true if match is valid
	 */
	protected boolean regexNameMatcher(String nameToMatch, String pattern) {
		return nameToMatch.matches(pattern);
	}
	
}