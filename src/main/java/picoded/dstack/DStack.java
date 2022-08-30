package picoded.dstack;

import picoded.core.conv.GenericConvert;
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.*;
import picoded.dstack.stack.*;

import java.util.ArrayList;
import java.util.List;

public class DStack extends CoreStack {
	
	// List of provider backends - to fetch / initialize from
	protected final ProviderConfig providerConfig;
	
	// Namespace listing
	protected final GenericConvertList<Object> namespace;
	
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
	 * @param name name of the datastructure to initialize
	 * @param type implmentation type (KeyValueMap / KeyLongMap / DataObjectMap / FileWorkspaceMap)
	 *
	 * @return initialized data structure if type is supported
	 */
	protected Core_DataStructure initDataStructure(String name, String type) {
		// Initialize for the respective type
		if (type.equalsIgnoreCase("DataObjectMap")) {
			return new Stack_DataObjectMap( //
				fetchCommonStructureLayers(name, "DataObjectMap", new Core_DataObjectMap[] {}), //
				fetchCommonStructureQueryLayer(name, "DataObjectMap", new Core_DataObjectMap[] {}) //
			); //
		}
		if (type.equalsIgnoreCase("KeyValueMap")) {
			return new Stack_KeyValueMap( //
				fetchCommonStructureLayers(name, "KeyValueMap", new Core_KeyValueMap[] {}), //
				fetchCommonStructureQueryLayer(name, "KeyValueMap", new Core_KeyValueMap[] {}) //
			); //
		}
		if (type.equalsIgnoreCase("KeyLongMap")) {
			return new Stack_KeyLongMap( //
				fetchCommonStructureLayers(name, "KeyLongMap", new Core_KeyLongMap[] {}), //
				fetchCommonStructureQueryLayer(name, "KeyLongMap", new Core_KeyLongMap[] {}) //
			); //
		}
		if (type.equalsIgnoreCase("FileWorkspaceMap")) {
			return new Stack_FileWorkspaceMap(
				//
				fetchCommonStructureLayers(name, "FileWorkspaceMap", new Core_FileWorkspaceMap[] {}), //
				fetchCommonStructureQueryLayer(name, "FileWorkspaceMap", new Core_FileWorkspaceMap[] {}) //
			); //
		}
		
		// No valid type supported
		return null;
	}
	
	//------------------------------------------------------------------------------------
	//
	// Fetching of CommonStructure, which is the base class of all the various
	// DataStructure implementation used within dstack.
	//
	// This is then used to form a combination data stack layer interface via Stack_*
	//
	//------------------------------------------------------------------------------------
	
	/**
	 * Given the provider name, data structure name, and type, fetch the relevent CommonStructure
	 *
	 * @param providerName to fetch from
	 * @param structureName to use
	 * @param type of data structure to expect
	 *
	 * @return null if not found, else the data structure
	 */
	protected <V extends Core_DataStructure> V fetchCommonStructureFromProvider(String providerName,
		String structureName, String type) {
		// Get the relevent provider CoreStack
		CoreStack providerStack = providerConfig.getProviderStack(providerName);
		if (providerStack == null) {
			return null;
		}
		
		// Get the relevent data structure
		Core_DataStructure providerDataStructure = providerStack.cacheDataStructure(structureName,
			type, null);
		if (providerDataStructure == null) {
			return null;
		}
		
		// Return the valid data structure
		return (V) providerDataStructure;
	}
	
	/**
	 * Given the data structure name, and string type. Get the relevent underlying data structure implmentation.
	 * According to the given configuration in the layered stack
	 *
	 * @param name         of data structure to use
	 * @param type         of data structure to implement
	 * @param refrenceType return array to use for type reference (not actually used)
	 *
	 * @return array of data structures found applicable, which is the same as referenceType
	 */
	protected <V extends Core_DataStructure> V[] fetchCommonStructureLayers(String name,
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
		
		// Iterate the provider for the data structures
		for (Object provider : providerList) {
			V providerDataStructure = fetchCommonStructureFromProvider(provider.toString(), name, type);
			if (providerDataStructure != null) {
				retList.add(providerDataStructure);
			}
		}
		
		// Throw an exception if empty
		if (retList.isEmpty()) {
			throw new RuntimeException("No `providers` returned a valid DataStructure");
		}
		
		// returning as array
		return retList.toArray(refrenceType);
	}
	
	/**
	 * Given the data structure name, and string type. Get the relevent query layer (if configured)
	 * According to the given configuration in the layered stack
	 *
	 * @param name         of data structure to use
	 * @param type         of data structure to implement
	 * @param refrenceType return array to use for type reference (not actually used)
	 *
	 * @return datastructure if configured, else null
	 */
	protected <V extends Core_DataStructure> V fetchCommonStructureQueryLayer(String name,
		String type, V[] refrenceType) {
		// Get the relevent namespace config
		GenericConvertMap<String, Object> namespaceConfig = resolveNamespaceConfig(name);
		if (namespaceConfig == null) {
			throw new RuntimeException("No `namespace` configuration found for " + name);
		}
		
		// Get the query provider (if configured)
		String queryProvider = namespaceConfig.getString("queryProvider");
		if (queryProvider == null) {
			return null;
		}
		
		// Fetch and return the provider data structure
		V providerDataStructure = fetchCommonStructureFromProvider(queryProvider, name, type);
		if (providerDataStructure == null) {
			throw new RuntimeException("Missing `queryProvider` : " + queryProvider);
		}
		return providerDataStructure;
	}
	
	//------------------------------------------------------------------------------------
	//
	// Regex Helper Functions
	//
	//------------------------------------------------------------------------------------
	
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
