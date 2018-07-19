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
import picoded.dstack.core.Core_DataObjectMap;
import picoded.dstack.stack.ProviderConfig;
import picoded.dstack.stack.Stack_DataObjectMap;

public class DStack extends CoreStack {
	
	protected ProviderConfig providerConfig;

	protected GenericConvertList<Object> namespace;
	protected GenericConvertList<Object> providerConfigList;

	/**
	 * Constructor with configuration map
	 */
	public DStack(GenericConvertMap<String, Object> inConfig) {
		super(inConfig);

		providerConfigList = inConfig.fetchGenericConvertList("provider");
		if(providerConfigList == null) {
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

		// Grab the first match namepsace configuration
		GenericConvertMap<String,Object> namespaceConfig = resolveNamespaceConfig(name);
		if ( namespaceConfig == null ) {
			throw new RuntimeException("No namespace configuration found for " + name);
		}

		// Obtain the prefix and the providers from the name space to search through
		String prefix = namespaceConfig.getString("prefix");
		List<Object> providerList = namespaceConfig.getObjectList("providers");
		if ( providerList == null ) {
			throw new RuntimeException("No `providers` found in namespaceConfig of "+prefix);
		}

		// For each of the provider inside the providerList, grab the dataObjectMap from it
		List<Core_DataObjectMap> stackDataObjectMapList = new ArrayList<>();
		for ( Object object : providerList ) {
			String nameOfProvider = "";
			if ( object instanceof String ) {
				 nameOfProvider = object.toString();
			}

			Core_DataObjectMap dataObjectMap = providerConfig.getProviderStack(nameOfProvider).dataObjectMap(name);

			if ( dataObjectMap == null ) {
				throw new RuntimeException(nameOfProvider +" in `providers` of `namespace` under the `prefix`"+prefix+" cannot be found.");
			}

			stackDataObjectMapList.add(dataObjectMap);
		}

		// Add the found dataObjectMap list and initialize the Stack DataObject 
		Core_DataObjectMap[] dataObjectMapsArray = stackDataObjectMapList.toArray(new Core_DataObjectMap[stackDataObjectMapList.size()]);

		Stack_DataObjectMap ret = new Stack_DataObjectMap( dataObjectMapsArray );

		return ret;
	}

	/**
	 * This function will find the first namespaceConfig that matches the requested name
	 * 
	 * @param name of the object to be searched
	 * 
	 * @return the configuration or null
	 */
	protected GenericConvertMap<String, Object> resolveNamespaceConfig(String name) {
		for(Object object : namespace){
			if( object instanceof Map){
				GenericConvertMap<String, Object> namespaceConfig = GenericConvert.toGenericConvertStringMap(object);
				String pattern = namespaceConfig.getString("prefix", "");
				if(regexNameMatcher(name, pattern)){
					return namespaceConfig;
				}
			}
		}
		return null;
	}

	/**
	 * Attempts to match the name with the regex pattern given in the param
	 * 
	 * @param nameToMatch is the string to check if it matches with the pattern
	 * @param pattern     is the `prefix` that is set in the namespace 
	 * 
	 * @return 
	 */
	protected boolean regexNameMatcher(String nameToMatch, String pattern) {
		return nameToMatch.matches(pattern);
	}

}